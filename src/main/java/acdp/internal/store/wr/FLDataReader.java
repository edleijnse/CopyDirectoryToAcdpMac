/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import acdp.Column;
import acdp.exceptions.ACDPException;
import acdp.exceptions.ShutdownException;
import acdp.internal.Buffer;
import acdp.internal.Column_;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.store.wr.FLDataHelper.Block;
import acdp.internal.store.wr.FLDataHelper.Blocks;
import acdp.internal.store.wr.FLDataHelper.Section;
import acdp.internal.store.wr.WRStore.WRColInfo;

/**
 * Given an array of columns and a WR store this class tries to find a good way
 * to efficiently read from the FL data file of the store's table the bitmap
 * and the FL column data matching the given columns of a particular row.
 *
 * @author Beat Hoermann
 */
final class FLDataReader {
	/**
	 * Keeps the column info object as well as the offset where the data for
	 * the column start within {@linkplain FLData#bytes the array of row
	 * bytes}.
	 * 
	 * @author Beat Hoermann
	 */
	static final class ColOffset {
		/**
		 * The offset within {@linkplain FLData#bytes the array of row bytes}
		 * where the data for the column start.
		 */
		final int offset;
		/**
		 * The info object of the column, never {@code null}.
		 */
		final WRColInfo ci;
		
		/**
		 * The constructor.
		 * 
		 * @param offset The offset within {@linkplain FLData#bytes the array
		 *        of row bytes} where the data for the column start.
		 * @param ci The info object of the column, not allowed to be {@code
		 *        null}.
		 */
		private ColOffset(int offset, WRColInfo ci) {
			this.offset = offset;
			this.ci = ci;
		}
	}
	
	/**
	 * Keeps the byte array which either houses a whole row of length equal to
	 * the value of {@link WRStore#n} or the bitmap of the row along with some
	 * FL column data of some selected columns only.
	 * 
	 * @author Beat Hoermann
	 */
	static final class FLData {
		/**
		 * The byte array containing the FL data, never {@code null}.
		 * If the FL data reader reads the whole FL data block then the byte
		 * array contains the whole FL data block.
		 * Otherwise, the byte array starts with the bitmap of the row followed
		 * by the FL column data of some selected columns.
		 */
		final byte[] bytes;
		/**
		 * The column offsets, never {@code null} and never empty.
		 * This value informs about the offsets where the FL data of the columns
		 * start within {@link #bytes}.
		 * <p>
		 * Note that if the {@code cols} argument of the constructor was neither
		 * {@code null} nor an empty array then this array is sorted according to
		 * the columns of the {@code cols} array and not according to the order
		 * defined by the table definition.
		 */
		final ColOffset[] colOffsets;
		/**
		 * The map used in the {@code FLData(byte[], FLDataHelper)} constructor to
		 * map a column to the corresponding index within the {@code colOffsets}
		 * array.
		 * <p>
		 * This value is {@code null} if this object was created with the other
		 * constructor.
		 */
		final Map<Column<?>, Integer> colMap;
		
		/**
		 * The constructor used by a reader reading the whole FL data block.
		 * 
		 * @param  bytes The byte array containing the whole FL data block, not
		 *         allowed to be {@code null}.
		 *         Must be of length equal to the value of {@link WRStore#n}.
		 * @param  cols The array of columns, not allowed to be {@code null} but
		 *         may be empty.
		 * @param  store The store, not allowed to be {@code null}.
		 *
		 * @throws IllegalArgumentException If the specified array of columns
		 *         contains at least one column that is not a column of the
		 *         store's table.
		 */
		private FLData(byte[] bytes, Column<?>[] cols, WRStore store) throws
																		IllegalArgumentException {
			this.bytes = bytes;
			
			if (cols.length == 0) {
				WRColInfo[] colInfoArr = store.colInfoArr;
				final int n = colInfoArr.length;
				colOffsets = new ColOffset[n];
				// Initialize colOffsets
				for (int i = 0; i < n; i++) {
					final WRColInfo ci = colInfoArr[i];
					colOffsets[i] = new ColOffset(ci.offset, ci);
				}
			}
			else {
				final int n = cols.length;
				colOffsets = new ColOffset[n];
				// Initialize colOffsets
				final Map<Column_<?>, WRColInfo> colInfoMap = store.colInfoMap;
				for (int i = 0; i < n; i++) {
					final WRColInfo ci = colInfoMap.get(cols[i]);
					if (ci == null) {
						throw new IllegalArgumentException(ACDPException.prefix(
											store.table) + "Column \"" + cols[i] +
											"\" is not a column of this table.");
					}
					colOffsets[i] = new ColOffset(ci.offset, ci);
				}
			}
			colMap = null;
		}
		
		/**
		 * The constructor used by a reader reading parts of an FL data block.
		 * 
		 * @param bytes  The byte array containing parts of an FL data block, not
		 *        allowed to be {@code null}.
		 *        Must be of length equal to the value of {@link
		 *        FLDataHelper#length}.
		 * @param flDataHelper The FL data helper, not allowed to be {@code null}.
		 */
		private FLData(byte[] bytes, FLDataHelper flDataHelper) {
			this.bytes = bytes;
			
			final Column<?>[] cols = flDataHelper.cols;
			// All cols are columns of the right table.
			
			final int n = cols.length;
			colOffsets = new ColOffset[n];
			colMap = new HashMap<>(n * 4 / 3 + 1);
			
			// Build colMap to map a column to its index.
			for (int i = 0; i < n; i++) {
				colMap.put(cols[i], i);
			}
			
			// Get sections iterator. Skip bitmap of row. Initialize offset.
			Iterator<Section> sectionsIt = flDataHelper.sections.iterator();
			int offset = sectionsIt.next().len;
			while (sectionsIt.hasNext()) {
				final WRColInfo ci = sectionsIt.next().ci;
				colOffsets[colMap.get(ci.col)] = new ColOffset(offset, ci);
				offset += ci.len;
			}
		}
	}
	
	/**
	 * Reads from the FL data file the bitmap and the column data of some
	 * selected columns of a particular row.
	 * Which columns are selected depends on the implementation of this
	 * interface.
	 * 
	 * @author Beat Hoermann
	 */
	static interface IFLDataReader {
		/**
		 * Reads from the FL data file the bitmap and the FL column data of some
		 * selected columns of the row with the specified index.
		 * Which columns are selected depends on the implementation of this
		 * interface.
		 * <p>
		 * Some implementations of this method rely on the {@code index} argument
		 * to be equal to the index of the <em>next</em> FL data block.
		 * <p>
		 * If this method is invoked more than once then be aware that it may
		 * return the same reference to a {@code FLData} object (if not {@code
		 * null}) and even the same reference to {@link FLData#bytes} all over
		 * again.
		 * <p>
		 * Ensure that the FL data file is open.
		 *
		 * @param  index The index of the FL data block housing the FL data of
		 *         the row, must be greater than or equal to zero and less than
		 *         the number of FL data blocks in the FL file space.
		 *         Some implementations of this method may rely on the {@code
		 *         index} argument to be equal to the index of the <em>next</em>
		 *         FL data block.
		 *
		 * @return The FL data of the row with the specified index.
		 *         The value is {@code null} if it turns out that the "row" is a
		 *         row gap.
		 *         
		 * @throws IllegalArgumentException If converting {@code index} to a file
		 *         position results in a value less than zero.
		 *         This exception never happens if {@code index} is equal to or
		 *         greater than zero.
		 * @throws ShutdownException If the file channel provider is shut down.
		 * @throws FileIOException If {@code index} is greater than or equal
		 *         to the number data blocks in the FL file space or if an I/O
		 *         error occurs while reading the FL data.
		 */
		FLData readFLData(long index) throws IllegalArgumentException,
															ShutdownException, FileIOException;
	}
	
	/**
	 * Reads some data of a row blockwise from the FL data file.
	 * Note that the {@code readFLData} method may be invoked more than once.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class BlocksFLDataReader implements IFLDataReader {
		/**
		 * The fixed length file space, never {@code null}.
		 */
		private final FLFileSpace flFileSpace;
		/**
		 * The FL data file, never {@code null}.
		 */
		private final FileIO flDataFile;
		/**
		 * The byte array wrapped by {@link #buf}, never {@code null}.
		 * Note that the byte array is reused if the {@code readFLData} method
		 * is invoked more than once.
		 */
		private final byte[] bytes;
		/**
		 * The FL data object, never {@code null}.
		 * Note that the FL data object is reused if the {@code readFLData}
		 * method is invoked more than once.
		 */
		private final FLData flData;
		/**
		 * The byte buffer wrapping the byte array, never {@code null}.
		 */
		private final ByteBuffer buf;
		/**
		 * The blocks from the row helper, never {@code null} and never empty.
		 */
		private final Blocks blocks;
		
		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param flDataHelper The FL data helper, not allowed to be {@code null}.
		 */
		BlocksFLDataReader(WRStore store, FLDataHelper flDataHelper) {
			flFileSpace = store.flFileSpace;
			flDataFile = store.flDataFile;
			bytes = new byte[flDataHelper.length];
			flData = new FLData(bytes, flDataHelper);
			buf = ByteBuffer.wrap(bytes);
			blocks = flDataHelper.blocks;
		}

		@Override
		public final FLData readFLData(long index) throws
						IllegalArgumentException, ShutdownException, FileIOException {
			final long pos = flFileSpace.indexToPos(index);
			final Iterator<Block> blocksIt = blocks.iterator();
			buf.limit(blocksIt.next().length());
			flDataFile.read_(buf, pos);
			if (bytes[0] < 0)
				// Row gap!
				return null;
			else {
				while (blocksIt.hasNext()) {
					final Block b = blocksIt.next();
					buf.limit(buf.position() + b.length());
					flDataFile.read_(buf, pos + b.offset());
				}
				return flData;
			}
		}
	}
	
	/**
	 * Reads all FL data of a row from the FL data file.
	 * Note that the {@code readFLData} method may be invoked more than once.
	 *
	 * @author Beat Hoermann
	 */
	private static final class BasicWholeFLDataReader implements IFLDataReader {
		/**
		 * The fixed length file space, never {@code null}.
		 */
		private final FLFileSpace flFileSpace;
		/**
		 * The FL data file, never {@code null}.
		 */
		private final FileIO flDataFile;
		/**
		 * The byte array wrapped by {@link #buf}, never {@code null}.
		 * Note that the byte array is reused if the {@code readFLData} method
		 * is invoked more than once.
		 */
		private final byte[] bytes;
		/**
		 * The FL data object, never {@code null}.
		 * Note that the FL data object is reused if the {@code readFLData}
		 * method is invoked more than once.
		 */
		private final FLData flData;
		/**
		 * The byte buffer wrapping the byte array, never {@code null}.
		 */
		private final ByteBuffer buf;
		
		/**
		 * The constructor.
		 * 
		 * @param  cols The array of columns, not allowed to be {@code null}.
		 *         The columns must be columns of the table associated with the
		 *         specified store.
		 *         If the array of columns is empty then the FL data reader
		 *         behaves as if the value is identical to the table definition.
		 * @param  store The store, not allowed to be {@code null}.
		 * 
		 * @throws IllegalArgumentException If the specified array of columns
		 *         contains at least one column that is not a column of the
		 *         store's table.
		 */
		BasicWholeFLDataReader(Column<?>[] cols, WRStore store) throws
																		IllegalArgumentException {
			flFileSpace = store.flFileSpace;
			flDataFile = store.flDataFile;
			bytes = new byte[store.n];
			flData = new FLData(bytes, cols, store);
			buf = ByteBuffer.wrap(bytes);
		}
		
		@Override
		public final FLData readFLData(long index) throws
						IllegalArgumentException, ShutdownException, FileIOException {
			final long pos = flFileSpace.indexToPos(index);
			buf.rewind();
			flDataFile.read_(buf, pos);
			
			return bytes[0] < 0 ? null : flData;
		}
	}
	
	/**
	 * Reads all FL data of the <em>next</em> row from the FL data file using an
	 * extra buffer.
	 * Note that the {@code readFLData} method may be invoked more than once.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class BufferedWholeFLDataReader implements
																					IFLDataReader {
		/**
		 * The fixed length file space, never {@code null}.
		 */
		private final FLFileSpace flFileSpace;
		/**
		 * The FL data file, never {@code null}.
		 */
		private final FileIO flDataFile;
		/**
		 * The size of the FL data file in bytes.
		 */
		private final long fileSize;
		/**
		 * The length of a single FL data block.
		 */
		private final int n;
		/**
		 * The initial limit of the {@linkplain #extraBuf extra byte buffer}.
		 */
		private final int limit;
		/**
		 * The internal byte buffer.
		 */
		private final ByteBuffer extraBuf;
		/**
		 * The byte array of the extra buffer.
		 */
		private final byte[] extraBufBytes;
		/**
		 * The byte array storing the FL data block, never {@code null}.
		 * Note that the byte array is reused if the {@code readFLData} method
		 * is invoked more than once.
		 */
		private final byte[] bytes;
		/**
		 * The FL data object, never {@code null}.
		 * Note that the FL data object is reused if the {@code readFLData}
		 * method is invoked more than once.
		 */
		private final FLData flData;
		
		/**
		 * Constructs the reader with an extra buffer.
		 * 
		 * @param  length The total number of bytes this reader has to return,
		 *         must be greater than zero and divisible by {@link WRStore#n}
		 *         without a remainder.
		 * @param  cols The array of columns, not allowed to be {@code null}.
		 *         The columns must be columns of the table associated with the
		 *         specified store.
		 *         If the array of columns is empty then the FL data reader
		 *         behaves as if the value is identical to the table definition.
		 * @param  store The store, not allowed to be {@code null}.
		 * @param  buffer The extra buffer, not allowed to be {@code null}.
		 *         Its maximum capacity must be greater than or equal to {@link
		 *         WRStore#n}.
		 * 
		 * @throws IllegalArgumentException If the specified array of columns
		 *         contains at least one column that is not a column of the
		 *         store's table.
		 */
		BufferedWholeFLDataReader(long length, Column<?>[] cols, WRStore store,
										Buffer buffer) throws IllegalArgumentException {
			flFileSpace = store.flFileSpace;
			flDataFile = store.flDataFile;
			fileSize = store.flFileSpace.size();
			final int maxCap = buffer.maxCap();
			n = store.n;
			limit = maxCap < length ? maxCap / n * n : (int) length;
			// limit % store.n == 0
			extraBuf = buffer.buf(limit);
			extraBufBytes = extraBuf.array();
			bytes = new byte[n];
			flData = new FLData(bytes, cols, store);
			extraBuf.position(extraBuf.limit()); // -> !extraBuf.hasRemaining()
		}
		
		@Override
		public final FLData readFLData(long index) throws
						IllegalArgumentException, ShutdownException, FileIOException {
			// Precondition: Index is the index of the next row.
			// Load buffer if necessary.
			if (!extraBuf.hasRemaining()) {
				final long pos = flFileSpace.indexToPos(index);
				final long remaining = fileSize - pos;
				if (remaining <= 0) {
					// There is no next row.
					throw new FileIOException(flDataFile.path, null);
				}
				if (extraBuf.limit() < limit) {
					// This situation arises only if the limit was reduced due to
					// setting the limit equal to the value of remaining below. But
					// this can only be the case if the buffer was filled for the
					// last time during an iteration. Now the buffer has no
					// remaining elements - the FL data of the last row have been
					// processed. Obviously, a new iteration starts with the same
					// instance of BufferedWholeFLDataReader! Use the initial limit
					// of the buffer!
					extraBuf.limit(limit);
				}
				// remaining > 0
				if (extraBuf.limit() > remaining) {
					extraBuf.limit((int) remaining);
				}
				extraBuf.rewind();
				flDataFile.read_(extraBuf, pos);
				extraBuf.rewind();
			}
			// The ByteBuffer.get(byte[], ...)-methods are not well implemented.
			final int extraBufPos = extraBuf.position();
			System.arraycopy(extraBufBytes, extraBufPos, bytes, 0, n);
			extraBuf.position(extraBufPos + n);
			return bytes[0] < 0 ? null : flData;
		}
	}
	
	/**
	 * Creates a reader for reading a random FL data block from the FL data
	 * file.
	 * The returned reader won't make use of an extra buffer.
	 * 
	 * @param  cols The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of the table associated with the
	 *         specified store.
	 *         If the array of columns is empty then the returned FL data reader
	 *         behaves as if the value is identical to the table definition.
	 * @param  store The store, not allowed to be {@code null}.
	 * 
	 * @return The FL data reader, never {@code null}.
	 * 
	 * @throws IllegalArgumentException If the specified array of columns
	 *         contains at least one column that is not a column of the
	 *         store's table.
	 */
	static final IFLDataReader createRandomFLDataReader(Column<?>[] cols,
										WRStore store) throws IllegalArgumentException {
		final FLDataHelper flDataHelper = cols.length == 0 ? null :
																new FLDataHelper(cols, store);
		if (flDataHelper != null && flDataHelper.doProcessBlocks)
			return new BlocksFLDataReader(store, flDataHelper);
		else {
			// cols.length == 0 || !flDataHelper.doProcessBlocks
			return new BasicWholeFLDataReader(cols, store);
		}
	}
	
	/**
	 * Creates a reader for reading the next FL data block from the FL data
	 * file.
	 * The returned reader may be using an extra buffer.
	 * 
	 * @param  cols The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of the table associated with the
	 *         specified store.
	 *         If the array of columns is empty then the returned FL data reader
	 *         behaves as if the value is identical to the table definition.
	 * @param  store The store, not allowed to be {@code null}.
	 * @param  start The index of the first FL data block (inclusive) to be
	 *         returned from this reader, must be greater than or equal to zero.
	 *         The value of {@code start} is allowed to be greater than or equal
	 *         to the value of {@code end}.
	 * @param  end The index of the last FL data block (exclusive) to be
	 *         returned from this reader, must be greater than or equal to zero
	 *         and less than or equal to the number of data blocks contained in
	 *         the FL file space.
	 * @param  buffer The extra buffer to reduce the frequency of file reads, not
	 *         allowed to be {@code null}.
	 *         If this buffer is actually used depends on its {@linkplain
	 *         Buffer#maxCap() maximum capacity}.
	 * 
	 * @return The FL data reader, never {@code null}.
	 * 
	 * @throws IllegalArgumentException If the specified array of columns
	 *         contains at least one column that is not a column of the
	 *         store's table.
	 */
	static final IFLDataReader createNextFLDataReader(Column<?>[] cols,
							WRStore store, long start, long end, Buffer buffer) throws
																		IllegalArgumentException {
		// Use an extra buffer with a limit of at most buffer.maxCap() bytes if
		// at least two FL data blocks have to be read and if at least two FL
		// data blocks fit into the buffer.
		// (The real limit of the buffer will never exceed the size of all FL
		// data blocks that have to be read.)
		final long nofBlocksToRead = end - start;
		if (nofBlocksToRead >= 2 && buffer.maxCap() >= 2 * store.n)
			// At least two FL data blocks must be read and an extra buffer with a
			// size equal to buffer.maxCap() is able to cache at least two FL data
			// blocks.
			return new BufferedWholeFLDataReader(nofBlocksToRead * store.n, cols,
																					store, buffer);
		else {
			// Either a single FL data block has to be read at most or a buffer
			// with a size equal to buffer.maxCap() is too small to cache more
			// than a single FL data block.
			// Forget using a buffer.
			return createRandomFLDataReader(cols, store);
		}
	}
}