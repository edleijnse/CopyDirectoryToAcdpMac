/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.lang.AutoCloseable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;

import acdp.ReadZone;
import acdp.design.SimpleType;
import acdp.exceptions.CryptoException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.ShutdownException;
import acdp.internal.Buffer;
import acdp.internal.Column_;
import acdp.internal.Database_;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.Table_;
import acdp.internal.CryptoProvider.ROCrypto;
import acdp.internal.CryptoProvider.WRCrypto;
import acdp.internal.misc.array.FLByteArray;
import acdp.internal.store.Bag;
import acdp.internal.store.ro.ROStore;
import acdp.internal.store.ro.ROStore.ROColInfo;
import acdp.internal.store.ro.ROStore.ROStoreParams;
import acdp.internal.store.wr.FLDataReader.FLData;
import acdp.internal.store.wr.FLDataReader.IFLDataReader;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.internal.types.ArrayOfRefType_;
import acdp.internal.types.ArrayType_;
import acdp.internal.types.RefType_;
import acdp.internal.types.Type_;
import acdp.misc.Layout;
import acdp.misc.Utils;
import acdp.types.Type.Scheme;

/**
 * Converts a given WR database to an RO database.
 * <p>
 * The result of the conversion process is the so called <em>RO database
 * file</em> or just <em>database file</em> because contrary to a WR database
 * which consists of more than one file an RO database consists of only one
 * file.
 *
 * @author Beat Hoermann
 */
public final class WRtoRO {
	
	/**
	 * A {@code FileIO} output stream can roughly be viewed as a combination of
	 * a {@link java.io.FileOutputStream} and a {@link
	 * java.io.BufferedOutputStream}.
	 * <p>
	 * Note that invoking the {@link #flush} and the {@link #close} methods
	 * have no effect.
	 * <p>
	 * Invoke the {@link #resetCounter} method to find out how many bytes were
	 * written to the {@code FileIO} output stream since object creation or
	 * since the {@code resetCounter} method was last being invoked.
	 *
	 * @author Beat Hoermann
	 */
	private static final class FileIOOutputStream extends OutputStream {
		
		/**
		 * The writer used by the {@code FileIO} output stream.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class Writer extends AbstractWriter {
			private final FileIO file;
			
			/**
			 * Creates a writer with an initial buffer limit of {@code min(limit,
			 * buffer.maxCap())} bytes.
			 * 
			 * @param limit The initial limit of the buffer, must be greater than
			 *        zero.
		    * @param buffer The buffer to apply, not allowed to be {@code null}.
			 * @param file The file, not allowed to be {@code null}.
			 *        Data received via the {@code write} methods is finally saved
			 *        to the file at the position given by the file channel's
			 *        current position.
			 */
			Writer(int limit, Buffer buffer, FileIO file) {
				super(limit, buffer);
				this.file = file;
			}
			
			/**
			 * Saves the data in the specified buffer to the file channel at the
			 * channel's current file position.
			 * 
			 * @param  buf The buffer, never {@code null}.
			 * 
			 * @throws FileIOException If an I/O error occurs.
			 */
			@Override
			protected final void save(ByteBuffer buf) throws FileIOException {
				file.write(buf);
			}
		}
		
		private final Writer wr;
		private int counter;
		
		/**
		 * Creates a {@code FileIO} output stream with an initial buffer limit of
		 * {@code min(limit, buffer.maxCap())} bytes.
		 * 
		 * @param limit The initial limit of the buffer, must be greater than
		 *        zero.
		 * @param buffer The buffer to apply, not allowed to be {@code null}.
		 * @param file The file, not allowed to be {@code null}.
		 *        Data received via the {@code write} methods is finally saved
		 *        to the file at the position  given by the channel's current
		 *        file position.
		 */
		FileIOOutputStream(int limit, Buffer buffer, FileIO file) {
			this.wr = new Writer(limit, buffer, file);
			this.counter = 0;
		}
		
		@Override
		public final void write(int b) throws FileIOException {
			wr.write(b);
			counter++;
		}
		
		@Override
	   public final void write(byte b[], int off, int len) throws
	   								NullPointerException, IndexOutOfBoundsException,
	   																			FileIOException {
			wr.write(b, off, len);
			counter += len;
		}
		
		/**
		 * If there are any bytes left in the internal buffer of this file
		 * channel output stream then this method writes them to the file.
		 * Invoke this method once when your are done with this {@code FileIO}
		 * output stream to ensure that all data given to it via its {@code
		 * write} methods is actually written to the file.
		 * 
		 * @throws FileIOException If an I/O error occurs.
		 */
		final void finish() throws FileIOException {
			wr.flush();
		}
		
		/**
		 * Returns the number of bytes that were received by this {@code FileIO}
		 * output stream via its {@code write} methods since this {@code FileIO}
		 * output stream was created or since this method was last being
		 * invoked.
		 * 
		 * @return The number of bytes received.
		 */
		final int resetCounter() {
			int old = counter;
			counter = 0;
			return old;
		}
	}
	
	/**
	 * Packing data means compressing <em>and</em> encrypting the data, whereby
	 * data encryption is optional.
	 * <p>
	 * The packer provides the {@link #pack} method which packs portions of the
	 * unpacked WR table data and writes the packed table data to a file.
	 * <p>
	 * Packing data is done <em>blockwise</em>, that is, the unpacked data given
	 * to a packer by subsequently invoking the {@code pack} method is divided
	 * into a series of data blocks all but the last data block having a size
	 * equal to the value of the {@link ROStore#regularBlockSize} constant.
	 * <p>
	 * The sizes of the <em>packed</em> data blocks can be retrieved by invoking
	 * the {@link #blockSizes} method once all rows of the table have been
	 * packed.
	 *
	 * @author Beat Hoermann
	 */
	private static final class Packer implements AutoCloseable {
		
		/**
		 * Provides the {@link #pack} method which packs some data and writes the
		 * packed data to a file.
		 * By invoking the {@link #reset} method you can divide the data given
		 * to the block packer via the {@code pack} method into data blocks.
		 *
		 * @author Beat Hoermann
		 */
		private static final class BlockPacker implements AutoCloseable {
			/**
			 * The database's RO crypto object, may be {@code null}.
			 */
			private final ROCrypto roCrypto;
			/**
			 * The cipher used for packing the data, may be {@code null}.
			 */
			private final Cipher cipher;
			/**
			 * The path of the underlying file.
			 */
			private final Path path;
			/**
			 * The underlying {@code FileIO} output stream of the packer output
			 * stream, never {@code null}.
			 */
			private final FileIOOutputStream os;
			/**
			 * The packer output stream used to pack a block of data.
			 */
			private GZIPOutputStream packerOS;
			
			/**
			 * The constructor.
			 * 
			 * @param roCrypto The RO crypto object of the database, may be {@code
			 *        null}.
			 * @param buffer The buffer to apply, not allowed to be {@code null}.
			 * @param file The file, not allowed to be {@code null}.
			 *        Data received via the {@code write} methods is finally saved
			 *        to the file at the position given by the file channel's
			 *        current position.
			 */
			BlockPacker(ROCrypto roCrypto, Buffer buffer, FileIO file) {
				this.roCrypto = roCrypto;
				this.cipher = roCrypto == null ? null : roCrypto.get();
				this.path = file.path;
				this.os = new FileIOOutputStream(40, buffer, file);
				this.packerOS = null;
			}
			
			/**
			 * Creates the packer output stream.
			 * 
			 * @return The packer output stream, never {@code null}.
			 * 
			 * @throws FileIOException If an I/O error occurs.
			 */
			private final GZIPOutputStream createPackerOS() throws FileIOException{
				try {
					if (cipher == null)
						return new GZIPOutputStream(os);
					else {
						roCrypto.init(cipher, true);
						return new GZIPOutputStream(new CipherOutputStream(os,
																							cipher));
					}
				} catch (IOException e) {
					throw new FileIOException(path, e);
				}
			}
			
			/**
			 * Packs {@code n} bytes of the specified byte array and writes the
			 * packed data to the file.
			 * 
			 * @param  data The data to be packed, not allowed to be {@code null}.
			 *         The length of this byte array must be greater than or equal
			 *         to {@code off + n}.
			 * @param  off The index within {@code data} where to start packing,
			 *         must be greater than or equal to zero.
			 * @param  n The number of bytes to pack, must be greater than or
			 *         equal to zero.
			 * 
			 * @throws NullPointerException If {@code data} is {@code null}.
			 * @throws IndexOutOfBoundsException If the described preconditions on
			 *         the bag do not hold.
			 * @throws FileIOException If an I/O error occurs.
			 */
			final void pack(byte[] data, int off, int n) throws
										NullPointerException, IndexOutOfBoundsException,
																				FileIOException {
				if (packerOS == null) {
					packerOS = createPackerOS();
				}
				try {
					packerOS.write(data, off, n);
				} catch (IOException e) {
					throw new FileIOException(path, e);
				}
			}
			
			/**
			 * Resets the block packer.
			 * Invoke this method when you have finished packing data to the
			 * current block.
			 * 
			 * @return The size of the packed data block.
			 * 
			 * @throws FileIOException If an I/O error occurs.
			 */
			final int reset() throws FileIOException {
				try {
					packerOS.close();
				} catch (IOException e) {
					throw new FileIOException(path, e);
				}
				packerOS = null;
				return os.resetCounter();
			}

			@Override
			public final void close() throws FileIOException {
				// Flush the file output stream. Closing this type of file output
				// stream has no effect.
				os.finish();
				// Close the packer output stream.
				if (packerOS != null) {
					try {
						packerOS.close();
					} catch (IOException e) {
						throw new FileIOException(path, e);
					}
				}
			}
		}
		
		/**
		 * The block packer, never {@code null}.
		 */
		private final BlockPacker bp;
		/**
		 * The number of bytes left in the current data block of size {@code
		 * ROStore.nobsBlockSize}.
		 * If {@code left} equals zero then the current data block is full.
		 */
		private int left;
		/**
		 * The sizes of the packed data blocks, never {@code null}.
		 */
		private FLByteArray blockSizes;
		
		/**
		 * The constructor.
		 * 
		 * @param store The WR store.
		 * @param roCrypto The RO crypto object of the database, may be {@code
		 *        null}.
		 * @param buf The buffer used to reduce the frequency of file reads, not
		 *        allowed to be {@code null}.
		 * @param file The file, not allowed to be {@code null}.
		 *        The packed data is written to the file at the position given by
		 *        the file channel's current position.
		 */
		Packer(WRStore store, ROCrypto roCrypto, Buffer buf, FileIO file) {
			this.bp = new BlockPacker(roCrypto, buf, file);
			this.left = ROStore.regularBlockSize;
			
			// Estimate the total amount of unpacked table data.
			final FLFileSpace flFileSpace = store.flFileSpace;
			long tot = (flFileSpace.nofBlocks() - flFileSpace.nofGaps()) * store.n;
			if (store.vlFileSpace != null) {
				tot += store.vlFileSpace.allocated();
			}
			
			// Estimate the total number of data blocks.
			final long estNofBlocks = tot / ROStore.regularBlockSize;
			
			this.blockSizes = new FLByteArray(ROStore.nobsBlockSize, estNofBlocks *
									ROStore.nobsBlockSize, null, ROStore.nobsBlockSize);
		}
		
		/**
		 * Packs {@code n} bytes of the specified byte array and writes the
		 * packed data to the file.
		 * 
		 * @param  data The data to be packed, not allowed to be {@code null}.
		 *         The length of this byte array must be greater than or equal
		 *         to {@code off + n}.
		 * @param  off The index within {@code data} where to start packing, must
		 *         be greater than or equal to zero.
		 * @param  n The number of bytes to pack, must be greater than or equal
		 *         to zero.
		 * 
		 * @throws NullPointerException If {@code data} is {@code null}.
		 * @throws IndexOutOfBoundsException If the described preconditions on
		 *         the bag do not hold.
		 * @throws FileIOException If an I/O error occurs.
		 */
		final void pack(byte[] data, int off, int n) throws NullPointerException,
												IndexOutOfBoundsException, FileIOException {
			if (n <= 0) {
				return;
			}
			do {
				// n > 0
				if (left == 0) {
					// The block is full. Register the size of the packed block.
					blockSizes.add(Utils.unsToBytes(bp.reset(),
																		ROStore.nobsBlockSize));
					// New block.
					left = ROStore.regularBlockSize;
				}
				final int m = n > left ? left : n;
				// m > 0
				bp.pack(data, off, m);
				off += m;
				n -= m;
				left -= m;
			} while (n > 0);
			// n == 0
		}

		@Override
		public final void close() throws FileIOException {
			// Register the size of the last packed block.
			blockSizes.add(Utils.unsToBytes(bp.reset(), ROStore.nobsBlockSize));
			bp.close();
		}
		
		/**
		 * Returns the sizes of the packed data blocks.
		 * 
		 * @return The sizes of the packed data blocks, never {@code null}.
		 */
		final FLByteArray blockSizes() {
			return blockSizes;
		}
	}
	
	/**
	 * The table converter converts a given WR table to its corresponding RO
	 * table.
	 * The result is stored in the RO database file which is given to this
	 * class via the constructor.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class TableConverter {
		/**
		 * The WR crypto object.
		 * This value is {@code null} if the WR database does not apply
		 * encryption.
		 */
		private final WRCrypto wr_crypto;
		/**
		 * The RO crypto object.
		 * This value is {@code null} if converting the WR database to an RO
		 * database does not require encrypting the data in the RO database.
		 */
		private final ROCrypto ro_crypto;
		/**
		 * The gaps map, see the {@link #computeGapsMap} method description,
		 * never {@code null}.
		 */
		private final Map<Table_, long[]> gapsMap;
		/**
		 * The RO database file, never {@code null}.
		 */
		private final FileIO ro_file;
		
		/**
		 * The constructor.
		 * 
		 * @param db The WR database, not allowed to be {@code null}.
		 * @param gapsMap The gaps map, see the {@link #computeGapsMap} method
		 *        description, not allowed to be {@code null}.
		 * @param ro_fileIO The RO database file, not allowed to be {@code null}.
		 */
		TableConverter(Database_ db, Map<Table_, long[]> gapsMap,
																				FileIO ro_fileIO) {
			this.wr_crypto = db.wrCrypto();
			this.ro_crypto = db.roCrypto();
			this.gapsMap = gapsMap;
			this.ro_file = ro_fileIO;
		}
		
		/**
		 * For each table of a WR database a bunch keeps a bunch of information
		 * heavily used in the process of converting the table's WR rows to RO
		 * rows.
		 * <p>
		 * The bunch also keeps some state information needed when converting the
		 * next row or needed at the end when all rows are converted.
		 * Keeping together this state information in a single object avoids
		 * coding methods with a very long parameter list.
		 * <p>
		 * Furthermore, the bunch provides some fields which can be reused in
		 * the conversion process from one row to the next row thus avoiding
		 * repeated memory allocation.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class Bunch {
			/**
			 * The WR store.
			 */
			final WRStore store;
			
			/**
			 * The number of bytes required for referencing any outrow data from a
			 * WR row.
			 */
			final int wr_nobsOutrowPtr;
			/**
			 * The number of bytes of the bitmap of the WR row.
			 */
			final int wr_nBM;
			/**
			 * The column information for each column of the WR row.
			 */
			final WRColInfo[] wr_ciArr;
			/**
			 * The number of header bytes of the RO row.
			 */
			final int ro_nH;
			/**
			 * The number of bytes used by the null information of the RO row.
			 */
			final int ro_nNI;
			/**
			 * The column information for each column of the RO row.
			 */
			final ROColInfo[] ro_ciArr;
			/**
			 * A reusable placeholder for storing the length information of an RO
			 * row.
			 */
			final int[] ro_liArr;
			/**
			 * A reusable placeholder for storing the header of an RO row.
			 */
			final byte[] ro_rowHeader;
			
			/**
			 * A reusable bag.
			 */
			final Bag bag;
			/**
			 * A buffer which may be reused if its size is large enough.
			 */
			final Buffer buf;

			/**
			 * The file from where to read the outrow data of a WR row, may be
			 * {@code null}.
			 */
			final FileIO vlDataFile;
			/**
			 * The packer used to pack the RO table data.
			 */
			final Packer packer;
			
			/**
			 * The current number of bytes of the unpacked RO table data.
			 */
			long dataLength;
			/**
			 * The index of the current row.
			 */
			int i;
			/**
			 * The row pointers.
			 */
			final long[] rowPtrs;
			
			/**
			 * The constructor.
			 * 
			 * @param  table The WR table to be converted.
			 * @param  store The WR store of the table.
			 * @param  buf A buffer which may be reused if its size is large
			 *         enough.
			 * @param  nofRows The number of RO rows.
			 * @param  vlDataFile The file from where to read the outrow data of a
			 *         WR row.
			 * @param  packer The packer used to pack the RO table data.
			 * 
			 * @throws ImplementationRestrictionException If the WR table has too
			 *         many columns.
			 */
			Bunch(Table_ table, WRStore store, Buffer buf, int nofRows,
													FileIO vlDataFile, Packer packer) throws
														ImplementationRestrictionException {
				this.store = store;
				this.wr_nobsOutrowPtr = store.nobsOutrowPtr;
				this.wr_nBM = store.nBM;
				this.wr_ciArr = store.colInfoArr;
				
				final ROStoreParams rosp = new ROStoreParams(table);
				
				this.ro_nH = rosp.nH;
				this.ro_nNI = rosp.nNI;
				this.ro_ciArr = rosp.colInfoArr;
				this.ro_liArr = new int[ro_ciArr.length];
				this.ro_rowHeader = new byte[ro_nH];
				
				this.bag = new Bag();
				this.buf = buf;
				
				this.vlDataFile = vlDataFile;
				this.packer = packer;
				
				this.dataLength = 0;
				this.i = -1;
				this.rowPtrs = new long[nofRows];
			}
		}
		
		/**
		 * Returns the number of rows within the specified table.
		 * 
		 * @param  table The table, not allowed to be {@code null}.
		 * 
		 * @return The number of rows within the table.
		 * 
		 * @throws ImplementationRestrictionException If the number of rows is
		 *         greater than {@code Integer.MAX_VALUE}.
		 */
		private final int getNofRows(Table_ table) throws
														ImplementationRestrictionException {
			final long nofRows = table.numberOfRows();
			if (nofRows > Integer.MAX_VALUE) {
				throw new ImplementationRestrictionException(table, "The table " +
							"has too many rows for the RO format: " + nofRows + ".");
			}
			return (int) nofRows;
		}
		
		/**
		 * Tests if the bit corresponding to the specified column must be set in
		 * the null information of the RO row.
		 * 
		 * @param  bitmap The bitmap of the WR row.
		 * @param  flData The FL data of the WR row.
		 * @param  wrCI The column info object of the WR column.
		 * @param  roCI The column info object of the RO column.
		 * 
		 * @return The boolean value {@code true} if the bit corresponding to the
		 *         specified column must be set in the null information of the RO
		 *         row, {@code false} otherwise.
		 */
		private final boolean roNull(long bitmap, byte[] flData, WRColInfo wrCI,
																					ROColInfo roCI) {
			if (roCI.nullBitMask == 0)
				// Non-nullable ST, RefType
				return false;
			else if (wrCI.nullBitMask != 0)
				// INROW nullable ST, INROW AT with INROW ST, INROW ArrayOfRefType
				return (bitmap & wrCI.nullBitMask) != 0;
			else {
				// OUTROW nullable ST, INROW AT with OUTROW ST, OUTROW AAT
				return Utils.isZero(flData, wrCI.offset, wrCI.len);
			}
		}
		
		/**
		 * Computes the length of the byte representation of a value stored in
		 * the specified column of the specified table in an RO database.
		 * 
		 * @param  table The table.
		 * @param  flData The FL data of the WR row.
		 * @param  wrCI The column info object of the WR column.
		 * @param  roCI The column info object of the RO column.
		 * 
		 * @return The length of the byte representation in an RO database,
		 *         greater than or equal to zero.
		 *         
		 * @throws ImplementationRestrictionException If the length of the byte
		 *         representation of a value of the specified column exceeds
		 *         {@code Integer.MAX_Value}.
		 */
		private final int roLength(Table_ table, byte[] flData, WRColInfo wrCI,
							ROColInfo roCI) throws ImplementationRestrictionException {
			final int length;
			
			if (roCI.len > -1)
				// INROW ST, RefType
				length = roCI.len;
			else {
				// OUTROW ST, AAT
				final Type_ type = wrCI.col.type();
				final boolean arrayOfRefType = type instanceof ArrayOfRefType_;
				final int colOff = wrCI.offset;
					
				if (type.scheme() == Scheme.INROW && (arrayOfRefType ||
											type instanceof ArrayType_ &&
										 	((ArrayType_) type).elementType().scheme() ==
										 											Scheme.INROW)) {
					// INROW AT with INROW ST, INROW ArrayOfRefType
					final int elLen;
					int niLen = 0;
					int size = (int) Utils.unsFromBytes(flData, colOff,wrCI.sizeLen);
					if (arrayOfRefType)
						// INROW ArrayOfRefType
						// Note that roCI.nobsRowRef may be smaller than
						// wrCI.refdStore.nobsRowRef.
						elLen = roCI.nobsRowRef;
					else {
						// INROW AT with INROW ST
						final SimpleType<?> st = ((ArrayType_) type).elementType();
						elLen = st.length();
						if (st.nullable()) {
							niLen = Utils.bmLength(size);
							size -= Utils.bitCount(flData, colOff + wrCI.sizeLen,
																								niLen);
						}
					}
					length = wrCI.sizeLen + niLen + size * elLen;
				}
				else {
					// OUTROW ST, INROW ArrayType with OUTROW ST, OUTROW AAT
					long lLength = Utils.unsFromBytes(flData, colOff,wrCI.lengthLen);
					if (lLength > Integer.MAX_VALUE) {
						throw new ImplementationRestrictionException(table,
											wrCI.col.name(), "The length of the byte " +
											"representation of a value of this column " +
											"exceeds Integer.MAX_VALUE");
					}
					// lLength <= Integer.MAX_VALUE
					
					if (lLength > 0 && arrayOfRefType && wrCI.refdStore.nobsRowRef >
																				roCI.nobsRowRef) {
						// OUTROW ArrayOfRefType && WR length greater than RO length.
						// Compute the size of the OUTROW ArrayOfRefType.
						final int size = (int) (lLength - wrCI.sizeLen) /
																		wrCI.refdStore.nobsRowRef;
						// Compute the RO length of the array.
						lLength = wrCI.sizeLen + size * roCI.nobsRowRef;
					}
					
					length = (int) lLength;
				}
			}
			
			return length;
		}
		
		
		/**
		 * Computes the null information and the length information of the RO row.
		 * The null information is returned whereas the length information is
		 * stored in {@code bunch.ro_liArr}.
		 * 
		 * @param  flData The FL data of the WR row.
		 * @param  bunch The bunch.
		 * 
		 * @return The null information of the RO row.
		 *         
		 * @throws ImplementationRestrictionException If the length of the byte
		 *         representation of a column value exceeds {@code
		 *         Integer.MAX_Value}.
		 */
		private final long computeNiAndLi(byte[] flData, Bunch bunch) throws
														ImplementationRestrictionException {
			// Get the bitmap of the WR row.
			final long bitmap = Utils.unsFromBytes(flData, bunch.wr_nBM);
			long ni = 0L;
			for (int i = 0; i < bunch.wr_ciArr.length; i++) {
				final WRColInfo wrCI = bunch.wr_ciArr[i];
				final ROColInfo roCI = bunch.ro_ciArr[i];
				
				if (roNull(bitmap, flData, wrCI, roCI)) {
					bunch.ro_liArr[i] = 0;
					ni |= roCI.nullBitMask;
				}
				else {
					bunch.ro_liArr[i] = roLength(bunch.store.table, flData, wrCI,
																								roCI);
				}
			}
			return ni;
		}
		
		/**
		 * Computes the header of the RO row and packs it into the RO database
		 * file.
		 * As a side effect this method stores the length information in {@code
		 * bunch.ro_liArr}.
		 * 
		 * @param  flData The FL data of the WR row.
		 * @param  bunch The bunch.
		 * 
		 * @throws ImplementationRestrictionException If the length of the byte
		 *         representation of a column value exceeds {@code
		 *         Integer.MAX_Value}.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void computeAndPackRowHeader(byte[] flData,
																				Bunch bunch) throws
									ImplementationRestrictionException, FileIOException {
			final byte[] ro_rh = bunch.ro_rowHeader;
			// Compute null information and length information and copy null
			// information to row header. Length information is stored in
			// bunch.ro_liArr.
			final long ni = computeNiAndLi(flData, bunch);
			if (bunch.ro_nNI > 0) {
				Utils.unsToBytes(ni, bunch.ro_nNI, ro_rh);
			}
			// Copy length information stored in bunch.ro_liArr to row header.
			int off = bunch.ro_nNI;
			for (int i = 0; i < bunch.ro_liArr.length; i++) {
				final ROColInfo roCI = bunch.ro_ciArr[i];
				if (roCI.len == -1) {
					Utils.unsToBytes(bunch.ro_liArr[i], roCI.lengthLen, ro_rh, off);
					off += roCI.lengthLen;
				}
			}
			// Pack row header.
			bunch.packer.pack(ro_rh, 0, ro_rh.length);
		}
		
		/**
		 * Reads the specified number of bytes from the {@code bunch.vlDataFile}
		 * at the specified position.
		 * This method uses the {@code bunch.buf} buffer.
		 *
		 * @param  pos The file position, not allowed to be negative.
		 * @param  n The number of bytes to read, not allowed to be negative. 
		 * @param  bunch The bunch.
		 * 
		 * @return The byte array.
		 *         The size of the byte array may be greater than the value
		 *         of {@code n}.
		 * 
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final byte[] readVLData(long pos, int n, Bunch bunch) throws
																					FileIOException {
			final ByteBuffer buf = bunch.buf.buf(n);
			bunch.vlDataFile.read_(buf, pos);
			return buf.array();
		}
		
		/**
		 * Adjusts the reference stored in the specified array of bytes with
		 * respect to the specified sorted list of gap indices.
		 * 
		 * @param bytes The array of bytes housing the reference.
		 * @param off The offset within {@code bytes} where the reference starts.
		 * @param len The length of the reference.
		 * @param gaps The list of gap indices.
		 *        Add one to a gap index to get the index of the corresponding
		 *        row gap.
		 */
		private final void adjustRef(byte[] bytes, int off, int len,long[] gaps) {
			final long rowIndex = Utils.unsFromBytes(bytes, off, len);
			final long newRowIndex = FLCompactor.adjustRowIndex(rowIndex, gaps);
			if (newRowIndex < rowIndex) {
				Utils.unsToBytes(newRowIndex, len, bytes, off);
			}
		}
		
		/**
		 * Adjusts the array of references stored in the specified array of bytes
		 * with respect to the specified sorted list of gap indices.
		 * 
		 * @param bytes The array of bytes housing the array of references.
		 * @param off The offset within {@code bytes} where the array of
		 *        references starts.
		 * @param wrCI The column information of the WR row.
		 * @param gaps The list of gap indices.
		 *        Add one to a gap index to get the index of the corresponding
		 *        row gap.
		 */
		private final void adjustArrayOfRef(byte[] bytes, int off, WRColInfo wrCI,
																						long[] gaps) {
			int offset = off + wrCI.sizeLen;
			final int len = wrCI.refdStore.nobsRowRef;
			final int end = offset + (int) Utils.unsFromBytes(bytes, off,
																				wrCI.sizeLen) * len;
			while (offset < end) {
				adjustRef(bytes, offset, len, gaps);
				offset += len;
			}
		}
		
		/**
		 * Converts the specified FL column data of an ST column to the
		 * corresponding RO byte representation.
		 * 
		 * @param  flData The FL data of the WR row.
		 * @param  off The offset within the FL data where the FL column data
		 *         starts.
		 *         (If the column is an outrow simple type then the byte
		 *         representation must be read from the VL data file.)
		 * @param  wrCI The column information of the WR row.
		 * @param  type The type of the column which must be an ST column.
		 * @param  length The length of the RO byte representation.
		 * @param  bunch The bunch.
		 * 
		 * @throws CryptoException If decrypting the WR byte representation fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void convertST(byte[] flData, int off, WRColInfo wrCI,
												Type_ type, int length, Bunch bunch) throws
															CryptoException, FileIOException {
			if (type.scheme() == Scheme.INROW) {
				// INROW ST
				// wrCI.len == roCI.len == st.length() == length
				if (wr_crypto != null) {
					wr_crypto.dcrpt(flData, off, length);
				}
				bunch.packer.pack(flData, off, length);
			}
			else {
				// OUTROW ST
				final long ptr = Utils.unsFromBytes(flData, off + wrCI.lengthLen,
																		bunch.wr_nobsOutrowPtr);
				final byte[] bytes = readVLData(ptr, length, bunch);
				if (wr_crypto != null) {
					wr_crypto.dcrpt(bytes, 0, length);
				}
				bunch.packer.pack(bytes, 0, length);
			}
		}
		
		/**
		 * Converts the specified FL column data of an RT column to the
		 * corresponding RO byte representation.
		 * 
		 * @param  flData The FL data of the WR row.
		 * @param  off The offset within the FL data where the FL column data
		 *         starts.
		 * @param  wrCI The column information of the WR row.
		 * @param  length The length of the RO byte representation.
		 * @param  bunch The bunch.
		 * 
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void convertRefType(byte[] flData, int off, WRColInfo wrCI,
										int length, Bunch bunch) throws FileIOException {
			// References are never encrypted.
			// roCI.nobsRowRef == roCI.len == length
			// wrCI.refdStore.nobsRowRef == wrCI.len
			long[] gaps = gapsMap.get(wrCI.refdStore.table);
			if (gaps.length > 0) {
				adjustRef(flData, off, wrCI.len, gaps);
			}
			// The length of a WR ref may be larger than the length of an
			// RO ref.
			bunch.packer.pack(flData, off + wrCI.len - length, length);
		}
		
		/**
		 * Converts the specified FL column data of an A[RT] column to the
		 * corresponding RO byte representation.
		 * 
		 * @param  flData The FL data of the WR row.
		 * @param  off The offset within the FL data where the FL column data
		 *         starts.
		 *         (If the column is an OUTROW A[RT] then the byte representation
		 *         must be read from the VL data file.)
		 * @param  wrCI The column information of the WR row.
		 * @param  roCI The column information of the RO row.
		 * @param  type The type of the column which must be an A[RT] column.
		 * @param  length The length of the RO byte representation.
		 * @param  bunch The bunch.
		 * 
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void convertArrayOfRefType(byte[] flData, int off,
										WRColInfo wrCI, ROColInfo roCI, Type_ type,
										int length, Bunch bunch) throws FileIOException {
			// References are never encrypted.
			final int offset;
			if (type.scheme() == Scheme.INROW)
				// INROW ArrayOfRefType
				offset = off;
			else {
				// OUTROW ArrayOfRefType
				final int wrLength = (int) Utils.unsFromBytes(flData, off,
																					wrCI.lengthLen);
				final long ptr = Utils.unsFromBytes(flData, off + wrCI.lengthLen,
																		bunch.wr_nobsOutrowPtr);
				flData = readVLData(ptr, wrLength, bunch);
				offset = 0;
			}

			long[] gaps = gapsMap.get(wrCI.refdStore.table);
			if (gaps.length > 0) {
				adjustArrayOfRef(flData, offset, wrCI, gaps);
			}
			
			// The length of a WR ref may be larger than the length of an
			// RO ref.
			final int ro_refLen = roCI.nobsRowRef;
			final int wr_refLen = wrCI.refdStore.nobsRowRef;
			final int diff = wr_refLen - ro_refLen;
			if (diff > 0) {
				final Packer packer = bunch.packer;
				packer.pack(flData, offset, wrCI.sizeLen);
				off = offset + wrCI.sizeLen;
				final int end = off + (int) Utils.unsFromBytes(flData, offset,
																		wrCI.sizeLen) * wr_refLen;
				while (off < end) {
					packer.pack(flData, off + diff, ro_refLen);
					off += wr_refLen;
				}
			}
			else {
				bunch.packer.pack(flData, offset, length);
			}
		}
		
		/**
		 * Converts the specified FL column data of the A[ST] column to the
		 * corresponding RO byte representation.
		 * 
		 * @param  flData The FL data of the WR row.
		 * @param  off The offset within the FL data where the FL column data
		 *         starts.
		 *         (If the column is an OUTROW A[ST] then the byte representation
		 *         must be read from the VL data file.)
		 * @param  wrCI The column information of the WR row.
		 * @param  type The type of the column which must be an A[ST] column.
		 * @param  length The length of the RO byte representation.
		 * @param  bunch The bunch.
		 * 
		 * @throws CryptoException If decrypting an element of the array fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void convertAT(byte[] flData, int off, WRColInfo wrCI,
												Type_ type, int length, Bunch bunch) throws
															CryptoException, FileIOException {
			final SimpleType<?> st = ((ArrayType_) type).elementType();
			final boolean inrow = st.scheme() == Scheme.INROW;
			
			if (type.scheme() == Scheme.INROW && inrow) {
				// INROW AT with INROW ST
				if (wr_crypto != null) {
					int offset = off + wrCI.sizeLen;
					if (st.nullable()) {
						offset += Utils.bmLength((int) Utils.unsFromBytes(flData, off,
																					wrCI.sizeLen));
					}
					final int end = off + length;
					final int stLen = st.length();
					while (offset < end) {
						wr_crypto.dcrpt(flData, offset, stLen);
						offset += stLen;
					}
				}
				bunch.packer.pack(flData, off, length);
			}
			else {
				// other AT
				final long ptr = Utils.unsFromBytes(flData, off + wrCI.lengthLen,
																		bunch.wr_nobsOutrowPtr);
				final IStreamer sr = new FileStreamer(bunch.vlDataFile, ptr, length,
																				bunch.buf, false);
				final Packer packer = bunch.packer;
				final Bag bag = bunch.bag;
				
				if (wr_crypto != null) {
					sr.pull(wrCI.sizeLen, bag);
					packer.pack(bag.bytes, bag.offset, wrCI.sizeLen);
					int size = (int) Utils.unsFromBytes(bag.bytes, bag.offset,
																						wrCI.sizeLen);
					if (st.nullable()) {
						final int niLen = Utils.bmLength(size);
						sr.pull(niLen, bag);
						packer.pack(bag.bytes, bag.offset, niLen);
						size -= Utils.bitCount(bag.bytes, bag.offset, niLen);
					}
					
					final int stLen = st.length();
					for (int i = 0; i < size; i++) {
						sr.pull(stLen, bag);
						if (inrow) {
							wr_crypto.dcrpt(bag.bytes, bag.offset, stLen);
							packer.pack(bag.bytes, bag.offset, stLen);
						}
						else {
							packer.pack(bag.bytes, bag.offset, stLen);
							final int len = (int) Utils.unsFromBytes(bag.bytes,
																				bag.offset, stLen);
							sr.pull(len, bag);
							wr_crypto.dcrpt(bag.bytes, bag.offset, len);
							packer.pack(bag.bytes, bag.offset, len);
						}
					}
				}
				else {
					final int limit = ((FileStreamer) sr).bufLimit();
					// length >= limit
					final int n = length / limit;
					for (int i = 0; i < n; i++) {
						sr.pull(limit, bag);
						packer.pack(bag.bytes, bag.offset, limit);
					}
					final int remaining = length - limit * n;
					if (remaining > 0) {
						sr.pull(remaining, bag);
						packer.pack(bag.bytes, bag.offset, remaining);
					}
				}
			}
		}
		
		/**
		 * Converts the specified FL column data to the corresponding RO byte
		 * representation.
		 * 
		 * @param  flData The FL data of the WR row.
		 * @param  i The column index.
		 * @param  bunch The bunch.
		 * 
		 * @throws CryptoException If decrypting the WR byte representation fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void convertColumnData(byte[] flData, int i,
																				Bunch bunch) throws
															CryptoException, FileIOException {
			final int length = bunch.ro_liArr[i];
			if (length > 0) {
				final WRColInfo wrCI = bunch.wr_ciArr[i];
				final int colOff = wrCI.offset;
				final Type_ type = wrCI.col.type();
				
				if (type instanceof SimpleType)
					convertST(flData, colOff, wrCI, type, length, bunch);
				else if (type instanceof RefType_)
					convertRefType(flData, colOff, wrCI, length, bunch);
				else if (type instanceof ArrayOfRefType_)
					convertArrayOfRefType(flData, colOff, wrCI, bunch.ro_ciArr[i],
																			type, length, bunch);
				else {
					// ArrayType
					convertAT(flData, colOff, wrCI, type, length, bunch);
				}
			}
		}
		
		/**
		 * Converts the WR row to the corresponding RO row.
		 * 
		 * @param  flData The FL data of the WR row.
		 * @param  bunch The bunch.
		 * 
		 * @throws ImplementationRestrictionException If the length of the byte
		 *         representation of a column value exceeds Integer.MAX_Value.
		 * @throws CryptoException If decrypting the byte representation of a
		 *         column value fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void convertRow(byte[] flData, Bunch bunch) throws
															ImplementationRestrictionException,
															CryptoException, FileIOException {
			long dl = bunch.dataLength;
			
			// Register row pointer.
			bunch.i++;
			bunch.rowPtrs[bunch.i] = dl;
			
			// Compute and pack the header of the RO row. As a side effect the
			// length information is stored in bunch.ro_liArr.
			computeAndPackRowHeader(flData, bunch);
			dl += bunch.ro_nH;

			// Convert the WR FL column data to the corresponding RO byte
			// representation for each column of the row.
			final int[] ro_liArr = bunch.ro_liArr;
			for (int i = 0; i < ro_liArr.length; i++) {
				convertColumnData(flData, i, bunch);
				dl += ro_liArr[i];
			}
			
			// Save dataLength.
			bunch.dataLength = dl;
		}
		
		/**
		 * Compresses the row pointers and the block sizes and writes them to
		 * the {@link #ro_file} file channel at the channel's current file
		 * position.
		 * 
		 * @param  rowPtrs The row pointers.
		 * @param  nobsRowPtr The number of bytes of the byte representation of a
		 *         row pointer. 
		 * @param  blockSizes The block sizes.
		 * @param  buf The buffer used to reduce the frequency of file reads, not
		 *         allowed to be {@code null}.
		 * 
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void saveRPsAndBSs(long[] rowPtrs, int nobsRowPtr,
						FLByteArray blockSizes, Buffer buf) throws FileIOException {
			final long tot = Math.min(rowPtrs.length * nobsRowPtr +
						ROStore.nobsBlockSize * blockSizes.size(), Integer.MAX_VALUE);
			final FileIOOutputStream os = new FileIOOutputStream((int) tot, buf,
																							ro_file);
			try (GZIPOutputStream osZip = new GZIPOutputStream(os)) {
				// Write row pointers.
				final byte[] bytes = new byte[nobsRowPtr];
				for (long rowPtr : rowPtrs) {
					Utils.unsToBytes(rowPtr, nobsRowPtr, bytes);
					osZip.write(bytes);
				}
				// Write block sizes.
				for (byte[] bs : blockSizes) {
					osZip.write(bs);
				}
			} catch (IOException e) {
				throw new FileIOException(ro_file.path, e);
			} // osZip.close()
			os.finish();
		}
		
		/**
		 * Converts the specified WR table to the corresponding RO table.
		 * The result is stored in the {@linkplain #ro_file RO database file}.
		 * 
		 * @param  table The WR table.
		 * @param  layout The layout of the WR table.
		 * @param  buf1 The first buffer used to reduce the frequency of file
		 *         reads, not allowed to be {@code null}.
		 * @param  buf2 The second buffer used to reduce the frequency of file
		 *         reads, not allowed to be {@code null}.
		 * @param  buf3 The third buffer used to reduce the frequency of file
		 *         reads, not allowed to be {@code null}.
		 * 
		 * @throws ImplementationRestrictionException If the WR table has more
		 *         than {@code Integer.MAX_VALUE} rows <em>or</em> if the length
		 *         of the byte representation of a stored column value exceeds
		 *         {@code Integer.MAX_VALUE} <em>or</em> if the WR table has too
		 *         many columns.
		 * @throws ShutdownException If the database's file channel provider is
		 *         shut down.
		 *         A reason for this exception to happen is that the WR database
		 *         is closed.
		 * @throws CryptoException If decrypting the stored byte representation
		 *         of a column value fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 * @throws FileIOException If an I/O error occurs.
		 */
		final void run(Table_ table, Layout layout, Buffer buf1, Buffer buf2,
																				Buffer buf3) throws
													ImplementationRestrictionException,
													ShutdownException, CryptoException,
																					FileIOException {
			final int nofRows = getNofRows(table);
			final long startData = ro_file.position();
			
			if (nofRows == 0)
				// There are no rows to convert. Create the RO store layout with
				// suitable values and put it into the table layout.
				layout.replace(Table_.lt_store, ROStore.createLayout(nofRows,
																startData, 0, startData, 0, 0));
			else {
				// There exists at least one row to convert.
				final long dataLength;
				final long[] rowPtrs;
				final FLByteArray blockSizes;
				
				final WRStore store = (WRStore) table.store();
				
				final long nofBlocks = store.flFileSpace.nofBlocks();
				// Note that an empty column array results in a "whole FL data
				// reader".
				final IFLDataReader rdr = FLDataReader.createNextFLDataReader(
											new Column_<?>[0], store, 0, nofBlocks, buf1);
				store.flDataFile.open();
				try {
					store.vlDataFile.open();
					try {
						// Wrap ro_file into a packer. Table data will be packed.
						try (Packer packer = new Packer(store, ro_crypto, buf3,
																						ro_file)) {
							final Bunch bunch = new Bunch(table, store, buf2, nofRows,
																		store.vlDataFile, packer);
							// Read the rows and convert them.
							for (long i = 0; i < nofBlocks; i++) {
								final FLData flData = rdr.readFLData(i);
								// flData == null <=> row gap
								if (flData != null) {
									convertRow(flData.bytes, bunch);
								}
							}
							
							// Save data length, row pointers and block sizes.
							dataLength = bunch.dataLength;
							rowPtrs = bunch.rowPtrs;
							blockSizes = packer.blockSizes();
						} // packer.close() - packed data is flushed to ro_file 
						  // advancing the ro_file's current position.
					} finally {
						store.vlDataFile.close();
					}
				} finally {
					store.flDataFile.close();
				}
				
				final int nobsRowPtr = Utils.lor(rowPtrs[rowPtrs.length - 1]);
				final long startRowPtrs = ro_file.position();
				
				// Compress and write row pointers and block sizes to ro_file at
				// the current file position.
				saveRPsAndBSs(rowPtrs, nobsRowPtr, blockSizes, buf1);
				
				// Create the RO store layout and put it into the table layout.
				layout.replace(Table_.lt_store, ROStore.createLayout(nofRows,
														startData, dataLength, startRowPtrs,
														nobsRowPtr, (int) blockSizes.size()));
			}
		}
	}
	
	/**
	 * For each table in the specified array of tables this method computes the
	 * list of gap indices sorted in ascending order.
	 * The keys of the returned map are the tables and the values are the lists
	 * of gap indices.
	 * <p>
	 * Add one to a gap index to get the index of the corresponding row gap.
	 * 
	 * @param  tables The tables of the database.
	 *
	 * @return The gaps map.
	 * 
	 * @throws ImplementationRestrictionException If the number of row gaps in
	 *         at least one of the tables is greater than {@code
	 *         Integer.MAX_VALUE}.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final Map<Table_, long[]> computeGapsMap(Table_[] tables) throws
									ImplementationRestrictionException, FileIOException {
		final Map<Table_, long[]> gapsMap = new HashMap<>(tables.length * 4 / 3 +
																									1);
		for (Table_ table : tables) {
			final WRStore store = (WRStore) table.store();
			gapsMap.put(table, store.flFileSpace.gaps());
		}
		return gapsMap;
	}
	
	/**
	 * Converts the specified WR database to an RO database.
	 * The result of this method is a newly created file, the RO database file,
	 * having the specified file path.
	 * 
	 * @param  db The WR database to convert.
	 * @param  roDbFilePath The path of the RO database file.
	 *         The value must be the path of a non-existing file.
	 *         
	 * @throws NullPointerException If at least one of the arguments is {@code
	 *         null}.
	 * @throws ImplementationRestrictionException If at least one of the tables
	 *         of the WR database has more than {@code Integer.MAX_VALUE} rows
	 *         <em>or</em> if the length of the byte representation of a stored
	 *         column value of a table of the WR database exceeds {@code
	 *         Integer.MAX_VALUE} <em>or</em> if at least one of the tables of
	 *         the WR database has too many columns <em>or</em> if the number of
	 *         row gaps in at least one of the tables of the WR database is
	 *         greater than {@code Integer.MAX_VALUE}.
	 * @throws ShutdownException If the database's file channel provider is
	 *         shut down.
	 *         A reason for this exception to happen is that the WR database is
	 *         closed.
	 * @throws CryptoException If decrypting the byte representation of a stored
	 *         column value of a table of the WR database fails.
	 *         This exception never happens if the WR database does not apply
	 *         encryption.
	 * @throws FileIOException If the specified file already exists or if some
	 *         other I/O error occurs.
	 */
	private final void convert(Database_ db, Path roDbFilePath) throws
							NullPointerException, ImplementationRestrictionException,
							ShutdownException, CryptoException, FileIOException {
		final Table_[] tables = (Table_[]) db.getTables();
		final Map<Table_, long[]> gapsMap = computeGapsMap(tables);
		
		// Create RO database file.
		try (FileIO ro_file = new FileIO(roDbFilePath, WRITE, CREATE_NEW)) {
			// Set the position where the data of the first table starts.
			ro_file.position(8);
			
			// Create the layout of the RO database. Fields on the level of the
			// tables as well as of the stores are yet to be adapted.
			final Layout layout = db.createRoLayout();
			
			// Convert the tables.
			final Layout tablesLayout = layout.getLayout(Database_.lt_tables);
			final TableConverter tc = new TableConverter(db, gapsMap, ro_file);
			final Buffer buf1 = new Buffer();
			final Buffer buf2 = new Buffer();
			final Buffer buf3 = new Buffer();
			for (Table_ table : tables) {
				tc.run(table, tablesLayout.getLayout(table.name()), buf1, buf2,
																								buf3);
			}
			
			// Write position of layout at beginning of ro_file.
			ro_file.write((ByteBuffer) ByteBuffer.allocate(8).putLong(
															ro_file.position()).rewind(), 0);
			// Compress and save layout
			try (GZIPOutputStream outZip = new GZIPOutputStream(
																ro_file.getOutputStream())) {
				layout.toOutputStream(outZip, layout.indent());
			} catch (IOException e) {
				throw new FileIOException(ro_file.path, e);
			} // outZip.close(), ro_file.fc.close()
		} // ro_file
	}
	
	/**
	 * Converts the specified WR database to an RO database.
	 * The result of this method is a newly created file, the RO database file,
	 * having the specified file path.
	 * <p>
	 * This method actually opens a read zone, converts the WR database and
	 * closes the read zone.
	 * Concurrent writes can therefore not take place while this method is
	 * running.
	 * 
	 * @param  db The WR database to convert.
	 * @param  roDbFilePath The path of the RO database file.
	 *         The value must be the path of a non-existing file.
	 * 
	 * @throws NullPointerException If at least one of the arguments is {@code
	 *         null}.
	 * @throws ImplementationRestrictionException If at least one of the tables
	 *         of the WR database has more than {@code Integer.MAX_VALUE} rows
	 *         <em>or</em> if the length of the byte representation of a stored
	 *         column value of a table of the WR database exceeds {@code
	 *         Integer.MAX_VALUE} <em>or</em> if at least one of the tables of
	 *         the WR database has too many columns <em>or</em> if the number of
	 *         row gaps in at least one of the tables of the WR database is
	 *         greater than {@code Integer.MAX_VALUE}.
	 * @throws ShutdownException If the database's file channel provider or the
	 *         synchroniziation manager is shut down.
	 *         A reason for this exception to happen is that the WR database is
	 *         closed.
	 * @throws CryptoException If decrypting the byte representation of a stored
	 *         column value of a table of the WR database fails.
	 *         This exception never happens if the WR database does not apply
	 *         encryption.
	 * @throws IOFailureException If the specified file already exists or if
	 *         another I/O error occurs.
	 */
	public final void run(Database_ db, Path roDbFilePath) throws
							NullPointerException, ImplementationRestrictionException,
							ShutdownException, CryptoException, IOFailureException {
		try (ReadZone rz = db.openReadZone()) {
			convert(db, roDbFilePath);
		} catch (FileIOException e) {
			throw new IOFailureException(db, e);
		}
	}
}