/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.ro;

import java.io.ByteArrayInputStream;
import java.lang.AutoCloseable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;

import acdp.exceptions.ShutdownException;
import acdp.internal.Database_;
import acdp.internal.FileIO;
import acdp.internal.CryptoProvider.ROCrypto;
import acdp.misc.Utils;

/**
 * Unpacking packed data means decrypting the data, provided that it is
 * encrypted, <em>and</em> uncompressing the data.
 * <p>
 * The unpacker provides the {@link #unpack} method which unpacks portions of
 * the packed RO table data in a <em>streamlike</em> fashion.
 * This means that any two consecutive invocations of the unpack method
 * 
 * <pre>
 * unpack(p0, n0, data0);
 * ... (anything else but not an invocation of the unpack method)
 * unpack(p1, n1, data1);</pre>
 * 
 * must satisfy {@code p0} + {@code n0} &le; {@code p1}.
 * This guarantees that the unpacker can apply a look-ahead strategy.
 * Indeed, each invocation of the unpack method involves at most a single read
 * operation on the database file and there is a good chance that "seamless"
 * invocations of the unpack method with {@code p0} + {@code n0} == {@code p1}
 * can be accomplished with a single file read even if the constructor is
 * invoked with a value of {@code capacity} equal to zero.
 * <p>
 * Unlike other classes that implement {@link AutoCloseable}, an unpacker can
 * still be used without any restriction after the {@code close} method has
 * been invoked.
 * However, to avoid resource leaks, the {@code close} method must be the last
 * method invoked by the client before the lifetime of an instance of this
 * class ends <em>unless</em> no method was called at all.
 *
 * @author Beat Hoermann
 */
final class Unpacker implements AutoCloseable {
	/**
	 * Given the value of an array index, the block streamer returns an {@link
	 * ByteArrayInputStream} that is set on the data block identical to the data
	 * block contained in the array of the table's data blocks at that index.
	 * <p>
	 * Implementations of a block streamer rely on the <em>stream constraint</em>
	 * of a block streamer to be satisfied.
	 * This means that any two consecutive invocations of the {@link
	 * #getBlockInputStream} method
	 * 
	 * <pre>
	 * getBlockInputStream(c0, l0, h0, bis0);
	 * ... (anything else but not an invocation of the
	 *      "getBlockInputStream"-method)
	 * getBlockInputStream(c1, l1, h1, bis1);</pre>
	 * 
	 * must satisfy {@code c1 == l0 && l0 < l1}.
	 * This guarantees that a block streamer can apply a look-ahead strategy.
	 * 
	 * @author Beat Hoermann
	 */
	private interface IBlockStreamer {
		/**
		 * Returns the block input stream, positioned on the beginning of the
		 * block at index {@code l} within the array of the table's data blocks
		 * and guarantees that at least this block can be read from the stream
		 * without reaching its end.
		 * <p>
		 * This method must be invoked in accordance with the <em>stream
		 * constraint</em> of a block streamer, see the class description for
		 * details.
		 * 
		 * @param  cur The index of the current data block or zero if this method
		 *         is invoked for the first time.
		 *         If this method was invoked before then this value must be
		 *         greater than or equal to zero and less than {@code l} and
		 *         equal to the value of the parameter {@code l} from the last
		 *         invocation of this method.
		 * @param  l The index of the data block.
		 *         This value must be greater than or equal to zero, greater than
		 *         {@code cur} and less than or equal to {@code h}.
		 * @param  h The index of a data block that can be used as a hint for a
		 *         strategy that buffers a certain amount of data blocks in
		 *         memory ("up to the data block with index {@code h}").
		 *         This value must be greater than or equal to zero and greater
		 *         than or equal to {@code l}.
		 * 
		 * @return The block input stream, positioned on the beginning of the
		 *         block with index {@code l}.
		 * 
		 * @throws ShutdownException If the file channel provider is shut down
		 *         due to a closed database.
		 *         This exception never happens if the data blocks are already in
		 *         memory.
		 * @throws IOException If an I/O error occurs.
		 *         This exception never happens if the data blocks are already in
		 *         memory.
		 */
		ByteArrayInputStream getBlockInputStream(int cur, int l, int h) throws
																ShutdownException, IOException;
	}
	
	/**
	 * A {@linkplain IBlockStreamer block streamer} that reads the table's data
	 * blocks from the database file.
	 * <p>
	 * Internally, the file block streamer applies a buffering strategy to avoid
	 * frequent file reads of data with a small size.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class FileBlockStreamer implements IBlockStreamer {
		/**
		 * The capacity of the buffer.
		 * Note that the size of the buffer may actually be larger than this
		 * value, see the description of the constructor.
		 */
		private final int capacity;
		/**
		 * The database file, never {@code null}.
		 */
		private final FileIO dbFile;
		/**
		 * The starting position of the table data within the database file.
		 * The value is equal to the starting position of the table's first data
		 * block.
		 */
		private final long startData;
		/**
		 * The sizes of the packed data blocks of the table.
		 */
		private final byte[] blockSizes;
		/**
		 * The number of packed data blocks of the table.
		 */
		private final int nBlocks;
		/**
		 * The buffer containing data blocks.
		 */
		private byte[] buf;
		/**
		 * The position within the database file relative to the starting
		 * position of the table data within the database file where the block
		 * with index {@code l} (see {@code getBlockInputStream} method) starts.
		 */
		private long lPos;
		/**
		 * The data block with the highest index contained in the buffer.
		 */
		private int high;
		/**
		 * The position within the database file relative to the starting
		 * position of the table data within the database file where the data
		 * block with index {@code high + 1} starts.
		 */
		private long pos;
		/**
		 * The block input stream used to read the packed data blocks from.
		 * <p>
		 * It is worth noting that it is not required to close a {@code
		 * ByteArrayInputStream}.
		 */
		private ByteArrayInputStream bis;
		
		/**
		 * Constructs the file block streamer with the specified capacity.
		 * <p>
		 * If the {@link #getBlockInputStream} method is invoked such that the
		 * sum of the sizes of the data blocks with index {@code l} to {@code h}
		 * is larger then the specified capacity then the size of the buffer will
		 * be equal to this sum.
		 * 
		 * @param capacity The capacity of the buffer, hence, the size of the
		 *        internal buffer in bytes.
		 * @param db The database, not allowed to be {@code null}.
		 * @param dbFile The database file, not allowed to be {@code null}.
		 * @param startData The position of the first data block to unpack.
		 * @param blockSizes The sizes of the packed data blocks of the table, not
		 *        allowed to be {@code null}.
		 */
		FileBlockStreamer(int capacity, Database_ db, FileIO dbFile,
														long startData, byte[] blockSizes) {
			this.capacity = capacity;
			this.dbFile = dbFile;
			this.startData = startData;
			this.blockSizes = blockSizes;
			this.nBlocks = blockSizes.length / ROStore.nobsBlockSize;
			buf = new byte[0];
			lPos = 0;
			high = -1;
			pos = 0;
			bis = null;
		}
		
		/**
		 * Reads the specified number of bytes from the database file starting at
		 * the specified position and saves them into the buffer {@link #buf}.
		 * 
		 * @param  pos The file position, not allowed to be negative.
		 * @param  n The number of bytes to read.
		 * 
		 * @throws ShutdownException If the file channel provider is shut down
		 *         due to a closed database.
		 * @throws IOException If an I/O error occurs.
		 */
		private final void loadBuf(long pos, int n) throws ShutdownException,
																						IOException {
			final ByteBuffer byteBuf = ByteBuffer.wrap(buf);
			byteBuf.limit(n);
			dbFile.open();
			try {
				dbFile.read_(byteBuf, pos);
			} finally {
				dbFile.close();
			}
		}
		
		/**
		 * Returns the size of the packed data block with index {@code i}.
		 * 
		 * @param  i The index of the packed data block.
		 * @return The size of the packed data block with index {@code i}.
		 */
		private final int blockSize(int i) {
			return (int) Utils.unsFromBytes(blockSizes, i * ROStore.nobsBlockSize,
																			ROStore.nobsBlockSize);
		}

		@Override
		public final ByteArrayInputStream getBlockInputStream(int cur, int l,
											int h) throws ShutdownException, IOException {
			if (l <= high) {
				// The data block with index l is already loaded. This method was
				// invoked before, therefore cur < l.
				int off = 0;
				for (int i = cur; i < l; i++) {
					off += blockSize(i);
				}
				lPos += off;
				bis.reset();
				bis.skip(off);
				bis.mark(0);
				// The block input stream is positioned on the beginning of the
				// block with index l.
			}
			else {
				// l > high
			
				// Compute the new buffer size.
				// First, compute the size of the data blocks with index l to h.
				long sum = 0;
				for (int i = l; i <= h; i++) {
					sum += blockSize(i);
				}
				if (sum > Integer.MAX_VALUE) {
					throw new IOException("Blocks too large.");
				}
				int size = (int) sum;
				// Second, try to maximize the size to read even more than the
				// required blocks into memory with a single file read.
				final int bufSize;
				if (size >= capacity)
					bufSize = size;
				else {
					// There may be a potential for reading more blocks with a
					// single file read.
					int i = h + 1;
					int s = i < nBlocks ? size + blockSize(i) : 0;
					// 0 < s && s <= capacity means that the table data consists of
					// at least one more block and bufSize can be increased by the
					// size of the next block without exceeding "capacity".
					while (0 < s && s <= capacity) {
						size = s;
						i++;
						s = i < nBlocks ? size + blockSize(i) : 0;
					}
					if (i < nBlocks)
						// The table data consists of at least one more block but
						// bufSize can't be increased by the size of the next block
						// without exceeding "capacity".
						// Set bufSize to the maximum allowed capacity.
						bufSize = capacity;
					else {
						// The table data has no more blocks.
						// We do not need the maximum allowed capacity.
						bufSize = size;
					}
					h = i - 1;
				}
				// size <= bufSize
			
				if (bufSize > buf.length) {
					// We need a new buffer.
					buf = new byte[bufSize];
				}
				// Since we have used the mark for "bis" we can't reuse "bis" by
				// simply invoking "bis.reset()" if bufSize <= buf.length. There is
				// no way to set the mark back to zero once it is set to a value
				// greater than zero.
				bis = new ByteArrayInputStream(buf);
				
				// Get the position within the database file relative to the
				// starting position of the table data within the database file
				// where the block with index l starts.
				lPos = pos;
				for (int i = high + 1; i < l; i++) {
					lPos += blockSize(i);
				}
			
				// Load the buffer.
				loadBuf(startData + lPos, size);
				
				// The input stream is positioned on the beginning of the block
				// with index l.
			
				// Update global properties for a later invocation of this method.
				high = h;
				pos = lPos + size;
			}
			
			return bis;
		}
	}
	
	/**
	 * A {@linkplain IBlockStreamer block streamer} that gets the table's data
	 * blocks from the primary memory.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class MemoryBlockStreamer implements IBlockStreamer {
		/**
		 * The sizes of the packed data blocks of the table.
		 */
		private final byte[] blockSizes;
		/**
		 * The block input stream used to read the packed data blocks from.
		 * <p>
		 * It is worth noting that it is not required to close a {@code
		 * ByteArrayInputStream}.
		 */
		private final ByteArrayInputStream bis;
		
		/**
		 * Constructs the memory block streamer.
		 *
		 * @param blockSizes The sizes of the packed data blocks of the table, not
		 *        allowed to be {@code null}.
		 * @param tableData The table data, not allowed to be {@code null}.
		 */
		MemoryBlockStreamer(byte[] blockSizes, byte[] tableData) {
			this.blockSizes = blockSizes;
			bis = new ByteArrayInputStream(tableData);
		}
		
		/**
		 * Returns the size of the packed data block with index {@code i}.
		 * 
		 * @param  i The index of the packed data block.
		 * @return The size of the packed data block with index {@code i}.
		 */
		private final int blockSize(int i) {
			return (int) Utils.unsFromBytes(blockSizes, i * ROStore.nobsBlockSize,
																			ROStore.nobsBlockSize);
		}
		
		/**
		 * Returns the block input stream, positioned on the beginning of the
		 * block at index {@code l} within the array of the table's data blocks
		 * and guarantees that at least this block can be read from the stream
		 * without reaching its end.
		 * <p>
		 * This method must be invoked in accordance with the <em>stream
		 * constraint</em> of a block streamer, see the class description for
		 * details.
		 * 
		 * @param  cur The index of the current data block or zero if this method
		 *         is invoked for the first time.
		 *         If this method was invoked before then this value must be
		 *         greater than or equal to zero and less than {@code l} and
		 *         equal to the value of the parameter {@code l} from the last
		 *         invocation of this method.
		 * @param  l The index of the data block.
		 *         This value must be greater than or equal to zero and greater
		 *         than {@code cur}.
		 * @param  h The "h" index, not used by a {@code MemoryBlockStreamer}.
		 * 
		 * @return The block input stream, positioned on the beginning of the
		 *         block with index {@code l}.
		 */
		@Override
		public final ByteArrayInputStream getBlockInputStream(int cur, int l,
																								int h) {
			int off = 0;
			for (int i = cur; i < l; i++) {
				off += blockSize(i);
			}
			bis.reset();
			bis.skip(off);
			bis.mark(0);
			// The block input stream is positioned on the beginning of the block
			// with index l.
			return bis;
		}
	}
	
	/**
	 * A block unpacker is used for unpacking data within a block of packed data
	 * in a <em>streamlike</em> fashion.
	 * (See the description of the {@link Unpacker} class to learn what is
	 * meant by "streamlike fashion".)
	 * <p>
	 * The block unpacker operates on a <em>current block</em> and unpacking data
	 * starts from a <em>current position</em> within the current block.
	 * Only data within the current block can be unpacked.
	 * <p>
	 * Both, the current block as well as the current position are part of the
	 * mutual state of the block unpacker.
	 * <p>
	 * After being opened, the current position gets incremented each time a
	 * client invokes the {@link #skip} and the {@link #unpack} method.
	 * The block unpacker must be closed when the client can't guarantee that
	 * there will be a next invocation of the {@code unpack} method.
	 * If it turns out that there is a need for unpacking more data, the client
	 * just <em>reopens</em> the block unpacker.
	 * <p>
	 * It is the client who has to make sure that he does not unpack data beyond
	 * the last byte of the current block.
	 * If the unpacker is closed, the current block can be changed by reopening
	 * the block unpacker.
	 * Changing the current block while the block unpacker is open, can be
	 * achieved by invoking the {@link #nextBlock} method.
	 * (The name "nextBlock" comes from the fact that the streamlike process of
	 * unpacking requires to walk through a sequence of blocks that is in the
	 * same order as the array of the table's data blocks.
	 * However, the "next" block is not necessarily the block that immediately
	 * follows a particular block of the table's data blocks but a block with a
	 * larger index.)
	 * 
	 * @author Beat Hoermann
	 */
	private static abstract class BlockUnpacker {
		/**
		 * The database's RO crypto object or {@code null} if data is not
		 * encrypted.
		 */
		private final ROCrypto roCrypto;
		/**
		 * The cipher to decrypt the data or {@code null} if data is not
		 * encrypted.
		 */
		private Cipher cipher;
		/**
		 * The information whether the block unpacker is closed or open.
		 */
		protected boolean closed;

		/**
		 * The constructor.
		 * 
		 * @param roCrypto The database's RO crypto object or {@code null} if
		 *        data is not encrypted.
		 */
		BlockUnpacker(ROCrypto roCrypto) {
			this.roCrypto = roCrypto;
			cipher = null;
			closed = true;
		}
		
		/**
		 * Creates the unpacker stream.
		 * <p>
		 * The unpacker stream is a {@link GZIPInputStream} sitting on the
		 * specified block input stream with an intermediate {@link
		 * CipherInputStream} if data is encrypted.
		 * <p>
		 * The specified block input stream must be positioned on the beginning
		 * of a block.
		 * 
		 * @param  blockIS The block input stream.
		 *         The block input stream must be positioned on the beginning of
		 *         a block.
		 * 
		 * @return The created unpacker stream.
		 * 
		 * @throws IOException If an I/O error occurs including the case where
		 *         a GZIP format error occurs.
		 */
		protected final GZIPInputStream createUnpackerStream(
													InputStream blockIS) throws IOException {
			// Precondition: blockIS is positioned on the beginning of a block.
			
			// closed == true implies that this method is invoked from the "open"
			// method while closed == false implies that this method is invoked
			// from the "nextBlock" method.
			
			if (closed && roCrypto != null) {
				// Set or reset the cipher.
				cipher = roCrypto.request();
			}
			
			// If data is encrypted, create a cipher input stream.
			if (cipher != null) {
				// Initialize the cipher. Note that initializing the cipher
				// object is much faster than creating it.
				roCrypto.init(cipher, false);
				// Create a cipher input stream.
				blockIS = new CipherInputStream(blockIS, cipher);
			}
			
			// Create a new instance of the unpacker stream and return it.
			return new GZIPInputStream(blockIS);
		}
		
		
		/**
		 * Opens or reopens the block unpacker.
		 * <p>
		 * This method assumes that the specified block input stream is positioned
		 * on the beginning of a data block and is capable to deliver all bytes
		 * of the block without signalling "end of stream".
		 * <p>
		 * The block unpacker must be closed.
		 * 
		 * @param  blockIS The block input stream, positioned on the beginning of
		 *         a block and capable to deliver all bytes of the block without
		 *         signalling "end of stream".
		 * @param  newBlock The information whether the block unpacker is used for
		 *         the first time with the block provided by the {@code blockIS}
		 *         argument, {@code true} if yes, {@code false} if no.
		 * 
		 * @throws IOException If an I/O error occurs including the case where
		 *         a GZIP format error occurs.
		 */
		abstract void open(InputStream blockIS, boolean newBlock) throws
																						IOException;
		
		/**
		 * Prepares the block unpacker for unpacking the next block.
		 * <p>
		 * This method assumes that the specified block input stream is positioned
		 * on the beginning of a data block and is capable to deliver all bytes
		 * of the block without signalling "end of stream".
		 * <p>
		 * The block unpacker must be open.
		 * 
		 * @param  blockIS The block input stream, positioned on the beginning of
		 *         a block and capable to deliver all bytes of the block without
		 *         signalling "end of stream".
		 * 
		 * @throws IOException If an I/O error occurs including the case where
		 *         a GZIP format error occurs.
		 */
		abstract void nextBlock(InputStream blockIS) throws IOException;
		
		/**
		 * Skips the specified number of bytes within the current block.
		 * <p>
		 * The new position of the block unpacker will be the current position
		 * plus {@code n}.
		 * <p>
		 * The block unpacker must be open.
		 * 
		 * @param  n The number of bytes to skip, must be greater than or equal
		 *         to zero.
		 *         The value must be such that the new position will still be
		 *         inside the current block.
		 * 
		 * @throws IOException If an I/O error occurs.
		 */
		abstract void skip(long n) throws IOException;
		
		/**
		 * Unpacks the specified number of bytes within the current block starting
		 * at the current position and saves the unpacked data into the specified
		 * array of bytes starting at the specified offset.
		 * <p>
		 * The new position of the block unpacker will be the current position
		 * plus {@code n}.
		 * <p>
		 * The block unpacker must be open.
		 * 
		 * @param  data The byte array where to save the unpacked data, not
		 *         allowed to be {@code null}.
		 *         The length of this byte array must be greater than or equal to
		 *         {@code off + n}.
		 * @param  off The offset, must be greater than or equal to zero.
		 * @param  n The number of bytes to unpack, must be greater than or equal
		 *         to zero.
		 *         The value must be such that the new position will still be
		 *         inside the current block.
		 *         
		 * @return The new offset, equal to {@code off + n}.
		 * 
		 * @throws IOException If an I/O error occurs.
		 */
		abstract int unpack(byte[] data, int off, int n) throws IOException;
		
		/**
		 * Returns the information whether the block unpacker is open or closed.
		 * 
		 * @return The boolean value {@code true} if and only if the block
		 *         unpacker is closed.
		 */
		final boolean isClosed() {
			return closed;
		}
		
		/**
		 * Releases the cipher if the cipher is not equal to {@code null}.
		 * <p>
		 * This method must be invoked by concrete block unpacker implementations
		 * in the {@code close} method if and only if the {@code
		 * createUnpackerStream} was invoked in the {@code open} method.
		 */
		protected final void releaseCipher() {
			if (cipher != null) {
				roCrypto.release(cipher);
			}
		}
		
		/**
		 * Closes the block unpacker.
		 * 
		 * @throws IOException If an I/O error occurs.
		 */
		abstract void close() throws IOException;
	}
	
	/**
	 * A block unpacker that unpacks the entire current block and saves the
	 * unpacked data in a buffer as soon as a new block is available.
	 * <p>
	 * The buffered block unpacker is especially useful if there are multiple
	 * unpacks within the same block separated by closing and reopening the block
	 * unpacker.
	 * This is the typical situation when a table is iterated.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class BufferedBlockUnpacker extends BlockUnpacker {
		/**
		 * The buffer used to buffer the unpacked data of the current block.
		 */
		private byte[] buf;
		/**
		 * The current position.
		 */
		private int bufPos;
		/**
		 * The information whether the buffered block unpacker is open and
		 * was opened with the {@code newBlock} argument set to {@code true}.
		 */
		private boolean newBlock;
		
		/**
		 * The constructor.
		 * 
		 * @param roCrypto The database's RO crypto object or {@code null} if
		 *        data is not encrypted.
		 */
		BufferedBlockUnpacker(ROCrypto roCrypto) {
			super(roCrypto);
			buf = null;
			bufPos = 0;
			newBlock = false;
		}
		
		/**
		 * Reads the entire block pointed to by the specified block input stream
		 * into the buffer.
		 * <p>
		 * The specified block input stream must be positioned on the beginning
		 * of a block.
		 * 
		 * @param  blockIS The block input stream.
		 * 
		 * @throws IOException If an I/O error occurs including the case where
		 *         a GZIP format error occurs.
		 */
		private final void loadBuf(InputStream blockIS) throws IOException {
			if (buf == null) {
				buf = new byte[ROStore.regularBlockSize];
			}
			try (GZIPInputStream us = createUnpackerStream(blockIS)) {
				// Read the entire current block into the buffer.
				int off = 0;
				int left = ROStore.regularBlockSize;
				int m = 0;
				while (m != -1 && left > 0) {
					// Note that GZIPInputStream.read may throw an IOException via the
					// ZIPException even if the underlying input stream of the
					// GZIPInputStream instance is based on a ByteArrayInputStream.
					m = us.read(buf, off, left);
					off += m;
					left -= m;
				}
			}
		}
		
		@Override
		final void open(InputStream blockIS, boolean newBlock) throws
																						IOException {
			if (newBlock) {
				loadBuf(blockIS);
			}
			this.newBlock = newBlock;
			bufPos = 0;
			closed = false;
		}
		
		@Override
		final void nextBlock(InputStream blockIS) throws IOException {
			loadBuf(blockIS);
			bufPos = 0;
		}

		@Override
		final void skip(long n) throws IOException {
			bufPos += n;
		}
		
		@Override
		final int unpack(byte[] data, int off, int n) throws IOException {
			System.arraycopy(buf, bufPos, data, off, n);
			bufPos += n;
			return off + n;
		}
		
		@Override
		final void close() {
			if (!closed) {
				if (newBlock) {
					releaseCipher();
				}
				closed = true;
			}
		}
	}
	
	/**
	 * A block unpacker that unpacks data in the current block on demand.
	 * <p>
	 * The instant block unpacker does not use an internal buffer to buffer the
	 * unpacked data of the entire block.
	 * Therefore, multiple unpacks within the same block separated by closing
	 * and opening the block unpacker requires repeated repositioning of the
	 * unpacker stream.
	 * As a consequence, this type of block unpacker should only be used when a
	 * row of a table is requested at random.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class InstantBlockUnpacker extends BlockUnpacker {
		/**
		 * The unpacker stream used to read unpacked data from a block.
		 */
		private GZIPInputStream us = null;

		/**
		 * The constructor.
		 * 
		 * @param roCrypto The database's RO crypto object or {@code null} if
		 *        data is not encrypted.
		 */
		InstantBlockUnpacker(ROCrypto roCrypto) {
			super(roCrypto);
		}
		
		@Override
		final void open(InputStream blockIS, boolean newBlock) throws
																						IOException {
			us = createUnpackerStream(blockIS);
			closed = false;
		}
		
		@Override
		final void nextBlock(InputStream blockIS) throws IOException {
			us.close();
			us = createUnpackerStream(blockIS);
		}
		
		@Override
		final void skip(long n) throws IOException {
			us.skip(n);
		}
		
		@Override
		final int unpack(byte[] data, int off, int n) throws IOException {
			while (n > 0) {
				// Note that GZIPInputStream.read may throw an IOException via the
				// ZIPException even if the underlying input stream of the
				// GZIPInputStream instance is based on a ByteArrayInputStream.
				int m = us.read(data, off, n);
				off += m;
				n -= m;
			}
			return off;
		}
		
		@Override
		final void close() throws IOException {
			if (!closed) {
				try {
					us.close();
				} finally {
					releaseCipher();
					closed = true;
				}
			}
		}
	}
	
	/**
	 * The {@linkplain IBlockStreamer block streamer}, never {@code null}.
	 */
	private final IBlockStreamer bs;
	/**
	 * The block unpacker used for unpacking the packed data.
	 */
	private final BlockUnpacker blockUnpacker;
	/**
	 * The block input stream.
	 * <p>
	 * The block input stream is used to transfer packed data blocks from the
	 * {@linkplain #bs block streamer} to the unpacker.
	 * <p>
	 * It is worth noting that it is not required to close a {@code
	 * ByteArrayInputStream}.
	 */
	private ByteArrayInputStream bis;
	/**
	 * The index of the block within the array of the table's data blocks saved
	 * in the database file that was last processed by this unpacker.
	 * The value is -1 before the first incovation of the {@code unpack} method.
	 */
	private int cur;
	/**
	 * The position within the database file relative to the starting position
	 * of the table data within the database file where unpacking can start
	 * without a need to skip bytes on the block input stream.
	 */
	private long nextPos;
	/**
	 * The number of bytes that can be unpacked with the unpacker stream.
	 * This value is grater than or equal to zero.
	 */
	private int avail;
	
	/**
	 * Constructs the unpacker with the specified capacity.
	 * 
	 * @param  capacity The capacity of the internal buffer, hence, the size of
	 *         the internal buffer in bytes.
	 *         If {@link ROStore#dbFile} is {@code null} then the unpacker does
	 *         not make use of an internal buffer.
	 *         In such a case the unpacker assumes the packed table data to
	 *         reside in memory and ignores this value.
	 *         Otherwise, set this value equal to zero if you intend to unpack
	 *         the data of a single random row.
	 *         However, if you intend to iterate the rows of a table, a value
	 *         greater than zero should be chosen.
	 *         (The size of the internal buffer used by this unpacker may be
	 *         larger then this value).
	 * @param  store The store of the table, not allowed to be {@code null}.
	 */
	Unpacker(int capacity, ROStore store) {
		this(store.table.db(), store.dbFile == null ?
				new MemoryBlockStreamer(store.blockSizes, store.tableData) :
				new FileBlockStreamer(capacity, store.table.db(), store.dbFile,
										store.startData, store.blockSizes), capacity > 0);
	}
	
	/**
	 * Constructs an unpacker that is ready to unpack data from the specified
	 * database file starting at the specified position.
	 * 
	 * @param db The database, not allowed to be {@code null}.
	 * @param dbFile The database file, not allowed to be {@code null}.
	 * @param startData The position of the first data block to unpack.
	 * @param blockSizes The sizes of the packed data blocks of the table, not
	 *        allowed to be {@code null}.
	 */
	Unpacker(Database_ db, FileIO dbFile, long startData, byte[] blockSizes) {
		this(db, new FileBlockStreamer(0, db, dbFile, startData, blockSizes),
																								false);
	}
	
	/**
	 * This private constructor is invoked by the constructors.
	 * 
	 * @param db The database.
	 * @param bs The block streamer.
	 * @param lookAhead The information whether the unpacker can apply a
	 *        look-ahead strategy.
	 */
	private Unpacker(Database_ db, IBlockStreamer bs, boolean lookAhead) {
		this.bs = bs;
		this.blockUnpacker = lookAhead ? new BufferedBlockUnpacker(db.roCrypto()):
													new InstantBlockUnpacker(db.roCrypto());
		this.bis = null;
		this.cur = -1;
		this.nextPos = 0;
		this.avail = 0;
	}
	
	/**
	 * Returns the index of the data block that contains the byte at the
	 * specified position.
	 * 
	 * @param  pos The position within the database file relative to the starting
	 *         position of the table data within the database file.
	 *         This value must be greater than or equal to zero.
	 *         
	 * @return The block index.
	 */
	private final int posToBI(long pos) {
		return (int) (pos / ROStore.regularBlockSize);
	}
	
	/**
	 * Unpacks the specified number of bytes at the specified position within
	 * the database file and saves the unpacked data into the specified byte
	 * array.
	 * <p>
	 * The values of the arguments must satisfy the preconditions described
	 * below.
	 * If this is not the case then this method either returns a strange result
	 * or throws an exception that may not be described below.
	 * 
	 * @param  pos The position within the database file relative to the starting
	 *         position of the table data within the database file where to start 
	 *         unpacking the bytes.
	 *         This value must be greater than or equal to zero and such that
	 *         {@code pos + n} is less than or equal to the total length of the
	 *         unpacked table data.
	 *         Furthermore, if this method was invoked before then the values
	 *         {@code p0} and {@code n0} of the {@code pos} and the {@code n}
	 *         arguments of the last invocation of this method must be such that
	 *         {@code p0 + n0} is less than or equal to this value.
	 * @param  n The number of bytes to unpack, must be greater than or equal to
	 *         zero.
	 * @param  data The byte array where to save the unpacked data, not allowed
	 *         to be {@code null}.
	 *         The length of this byte array must be greater than or equal to
	 *         {@code off + n}.
	 * @param  off The index within {@code data} where to start saving the
	 *         unpacked data, must be greater than or equal to zero.
	 * 
	 * @throws ShutdownException If the file channel provider is shut down due
	 *         to a closed database.
	 *         This exception never happens if the data blocks are already in
	 *         memory.
	 * @throws ZipException If a GZIP format error occurs.
	 * @throws IOException If an I/O error occurs.
	 */
	final void unpack(final long pos, final int n, final byte[] data,
					int off) throws ShutdownException, ZipException, IOException {
		// l   : The index of the data block that contains the byte at "pos",
		//       that is, the first byte to unpack. Later "l" may be incremented
		//       and represents the current block.
		// h :   The index of the data block that contains the last byte to
		//       unpack.
		// off : The index within "data" where to start saving the unpacked data.
		// left: The number of bytes yet to be unpacked.

		// Whenever "avail" is set to a value where "ROStore.regularBlockSize"
		// is involved then note that the size of the very last unpacked data
		// block of a table may be smaller than "ROStore.regularBlockSize"
		// resulting in a value of "avail" which is actually too large. This is
		// not a problem because we assume that the client of this method never
		// unpacks beyond the table data, hence, we assume that "pos + n" is less
		// than or equal to the total length of the unpacked table data.
		
		// Due to the stream constraint of the unpacker:
		// nextPos <= pos && cur <= l.
		
		int l = posToBI(pos);			
		final int h = n == 0 ? l : posToBI(pos + n - 1);
		int left = n;
		
		if (blockUnpacker.isClosed()) {
			// Open or reopen blockUnpacker.
			if (cur < l) {
				if (cur == -1) {
					// First invocation of the unpack method.
					cur = 0;
				}
				bis = bs.getBlockInputStream(cur, l, h);
				// The block input stream bis is positioned on the beginning of the
				// block with index l.
				blockUnpacker.open(bis, true);
				cur = l;
			}
			else {
				// cur == l: data was unpacked in the same block before.
				bis.reset();
				// The block input stream bis is positioned on the beginning of the
				// block with index l.
				blockUnpacker.open(bis, false);
			}
			
			avail = ROStore.regularBlockSize;
			nextPos = cur * ROStore.regularBlockSize;
		}
		else if (cur < l) {
			bis = bs.getBlockInputStream(cur, l, h);
			// The block input stream bis is positioned on the beginning of the
			// block with index l.
			blockUnpacker.nextBlock(bis);
			cur = l;
			
			avail = ROStore.regularBlockSize;
			nextPos = cur * ROStore.regularBlockSize;
		}
		// !blockUnpacker.isClosed() && cur == l
		
		// left >= 0
		// Consider the case "left == 0"!
		
		// The value of "avail" is large enough that the unpacker stream is able
		// to process at least the byte at position "pos".
		
		// Skip some bytes if necessary.
		if (nextPos < pos) {
			final long diff = pos - nextPos;
			blockUnpacker.skip(diff);
			avail -= diff;
		}

		// Update "nextPos" for a later invocation of this method.
		nextPos = pos + n;
			
		// avail > 0
		if (left <= avail) {
			blockUnpacker.unpack(data, off, left);
			// Update "avail" for a later invocation of this method.
			avail -= left;
		}
		else {
			// left > avail
			off = blockUnpacker.unpack(data, off, avail);
			left -= avail;
			// left > 0
			// The current block is unpacked. Set the focus to the next block.
			l += 1;
			int m;
			do {
				// cur < l
				bis = bs.getBlockInputStream(cur, l, h);
				// The block input stream bis is positioned on the beginning of the
				// block with index l.
				blockUnpacker.nextBlock(bis);
				cur = l;
			
				m = Math.min(left, ROStore.regularBlockSize);
				off = blockUnpacker.unpack(data, off, m);
				left -= m;
				l++;
			} while (left > 0);
			// left == 0 && cur == h
			// Update "avail" for a later invocation of this method.
			avail = ROStore.regularBlockSize - m;
		}
	}

	@Override
	public final void close() throws IOException {
		if (blockUnpacker != null) {
			blockUnpacker.close();
		}
	}
}
