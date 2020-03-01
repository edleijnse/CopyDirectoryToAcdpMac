/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import acdp.ColVal;
import acdp.Information.WRStoreInfo;
import acdp.Table.ValueChanger;
import acdp.Table.ValueSupplier;
import acdp.design.SimpleType;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CreationException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.DeleteConstraintException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.IllegalReferenceException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.MaximumException;
import acdp.exceptions.MissingEntryException;
import acdp.exceptions.ShutdownException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.Buffer;
import acdp.internal.Column_;
import acdp.internal.Database_;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.Ref_;
import acdp.internal.Table_;
import acdp.internal.Verifyer.Reporter;
import acdp.internal.misc.Utils_;
import acdp.internal.misc.ZipEntryCollector;
import acdp.internal.store.Store;
import acdp.internal.store.wr.BytesToObject.IBytesToObject;
import acdp.internal.store.wr.FLDataReader.FLData;
import acdp.internal.store.wr.FLDataReader.IFLDataReader;
import acdp.internal.store.wr.FLFileAccommodate.Spec;
import acdp.internal.store.wr.ObjectToBytes.IObjectToBytes;
import acdp.internal.types.AbstractArrayType;
import acdp.internal.types.ArrayOfRefType_;
import acdp.internal.types.ArrayType_;
import acdp.internal.types.RefType_;
import acdp.internal.types.Type_;
import acdp.misc.Layout;
import acdp.misc.Utils;
import acdp.types.Type;
import acdp.types.Type.Scheme;

/**
 * Defines a concrete store which supports the reading and writing of table
 * data.
 * <p>
 * It follows a description of the format of an <em>FL data block</em> of a row.
 * <p>
 * Each FL data block is subdivided into the <em>header</em> followed by the
 * <em>body</em>, both of a length greater than zero bytes.
 * If the sum of the lengths of the header and the body is less than 8 then the
 * body is followed by a sequence of extra bytes, called the <em>excess</em>,
 * which has a length such that the length of the header, the body and the
 * excess sums up to 8.
 * Thus, an FL data block has at least 8 bytes.
 * <p>
 * <b>Header</b><br>
 * Let us view the byte array of the header from left to right, each byte
 * consisting of 8 bits, like this:
 * 
 * <pre>
 * 0 1 2 3 4 5 6 7  0 1 2 3 4 5 6 7  ... 0 1 2 3 4 5 6 7
 * |  first Byte |  | second Byte |      |  nHth Byte  |</pre>
 * 
 * Then the individual bits and bytes of the header have the following meaning:
 * 
 * <ul>
 *    <li>Bit 0 of the first byte marks a row to be a "real" row or  a <em>row
 *        gap</em>.
 *        If a row is a row gap then the FL data block once housed a "real" row
 *        which was deleted at a later time.
 *        The FL data block is now under control of the {@link FLFileSpace}
 *        class which may reuse it when it has to allocate file space for a new
 *        "real" row.</li>
 *    <li>The following {@code m} bits are not used.
 *        With {@code p} explained in the next paragraph, the exact value of
 *        {@code m} is such that {@code m} is the smallest integer for which
 *        {@code m} + {@code p} + 1 is divisible by 8 without a remainder,
 *        hence, {@code m} varies from 0 to 7.</li>
 *    <li>Next come {@code p} bits where {@code p} is the number of columns
 *        that need one extra bit of information to indicate if a value in a
 *        particular <em>nullable</em> column is equal to the NULL value.
 *        (A nullable column is a column which allows the NULL value.)
 *        These {@code p} bits are called the <em>null information</em>.</li>
 *    <li>The bytes of the header discussed so far are called the
 *        <em>bitmap</em> of the row.
 *        Its length is equal to ({@code m} + {@code p} + 1) / 8 bytes which is
 *        greater than zero and less than or equal to 8 because the gap flag
 *        described above always takes one bit and {@code p} is not allowed to
 *        exceed 63 bits.
 *        If any column of this table or another table references this table
 *        then the bitmap is followed by {@code q} bytes housing the
 *        <em>reference counter</em>.
 *        The reference counter indicates how many rows reference this row.
 *        For an <em>unreferenced table</em>, that is a table which is not
 *        referenced by any column of this table or another table, the value of
 *        {@code q} is equal to zero.
 *        Otherwise, the value of {@code q} is equal to the value of the {@code
 *        nobsRefCount} field found in the layout.
 *        (See the description of the {@linkplain acdp.tools.Setup Setup
 *        Tool} to learn about the {@code nobsRefCount} field.)</li>
 * </ul>
 * <p>
 * <b>Body</b><br>
 * The body is subdivided into <em>sections</em> of different lengths each
 * section housing the fixed length <em>FL column data</em> of a particular
 * column.
 * The sequence of FL column data is sorted according to the order defined by
 * the <em>table definition</em>.
 * <p>
 * <b>Excess</b><br>
 * Let {@code s} and {@code t} denote the length of the header and the body,
 * respectively.
 * Then the body is followed by what we call the <em>excess</em> if and only if
 * {@code s} + {@code t} is less than 8.
 * The excess is a sequence of bytes of an undefined content having a length
 * equal to the value of 8 - {@code s} - {@code t}.
 * The excess guarantees that an FL data block has at least 8 bytes which is
 * a requirement for the {@link FLFileSpace} class.
 * <p>
 * Thus, for an unreferenced table with, say, one non-nullable column and three
 * nullable columns that each need one extra NULL-bit information and all of the
 * four columns storing {@code Integer} values, the length of an FL data block
 * will be equal to 17 bytes: One byte for the header ({@code m=4}, {@code p=3},
 * {@code q=0}) and 16 bytes for the body.
 * There is no excess because the size of the FL data block is greater than 8
 * bytes.
 *
 * @author Beat Hoermann
 */
public final class WRStore extends Store {
	// Keys of layout entries.
	public static final String lt_flDataFile = "flDataFile";
	public static final String lt_vlDataFile = "vlDataFile";
	public static final String lt_nobsRowRef = "nobsRowRef";
	public static final String lt_nobsOutrowPtr = "nobsOutrowPtr";
	public static final String lt_nobsRefCount = "nobsRefCount";
	
	/**
	 * The value of the {@linkplain #nobsRowRef} property used whenever ACDP
	 * needs to set it.
	 */
	private static final int NOBS_ROW_REF = 8;
	/**
	 * The value of the {@linkplain #nobsOutrowPtr} property used whenever ACDP
	 * needs to set it.
	 */
	private static final int NOBS_OUTROW_PTR = 8;
	/**
	 * The value of the {@linkplain #nobsRefCount} property used whenever ACDP
	 * needs to set it.
	 */
	private static final int NOBS_REF_COUNT = 8;
	
	/**
	 * Tests if the specified column is a <em>VL column</em>.
	 * <p>
	 * A VL column is column storing its data in the VL file space, hence,
	 * an OUTROW column or an INROW A[OUTROW ST] column.
	 * 
	 * @param  col The column, not allowed to be {@code null}.
	 * 
	 * @return The boolean value {@code true} if and only if the specified column
	 *         is a VL column.
	 */
	private static final boolean isVLCol(Column_<?> col) {
		Type_ type = col.type();
		return type.scheme() == Scheme.OUTROW || type instanceof ArrayType_ &&
						((ArrayType_) type).elementType().scheme() == Scheme.OUTROW;
	}
	
	/**
	 * Finds out if this store needs a VL data file.
	 * 
	 * @param  tableDef The table definition, not allowed to be {@code null}.
	 * 
	 * @return The boolean value {@code true} if this store needs a VL data
	 *         file, {@code false} otherwise.
	 */
	private static final boolean vlDataFileNeeded(Column_<?>[] tableDef) {
		boolean found = false;
		
		int i = 0;
		while (i < tableDef.length && !found) {
			found = isVLCol(tableDef[i++]);
		}
		
		return found;
	}

	/**
	 * Creates a WR store layout.
	 * <p>
	 * Besides some other fields this method adds the {@code nobsRowRef} field
	 * to the newly created empty layout and, provided that certain conditions
	 * are satisfied, also the {@code nobsOutrowPtr} and the {@code nobsRefCount}
	 * fields.
	 * See the description of the {@linkplain acdp.tools.Setup Setup Tool}
	 * to learn about these fields.
	 * 
	 * @param  referenced If {@code true} then the table associated with the
	 *         store is referenced by itself or by another table within the
	 *         database.
	 * @param  tableDef The table definition of the associated table, not
	 *         allowed to be {@code null}.
	 * @param  tableName The name of the associated table, not allowed to be
	 *         {@code null} and not allowed to be an empty string.
	 *         
	 * @return The WR store layout.
	 */
	public static final Layout createLayout(boolean referenced,
												Column_<?>[] tableDef, String tableName) {
		Layout layout = new Layout();
		
		final boolean vlDataFileNeeded = vlDataFileNeeded(tableDef);
		
		layout.add(lt_nobsRowRef, String.valueOf(NOBS_ROW_REF));
		if (vlDataFileNeeded) {
			layout.add(lt_nobsOutrowPtr, String.valueOf(NOBS_OUTROW_PTR));
		}
		if (referenced) {
			layout.add(lt_nobsRefCount, String.valueOf(NOBS_REF_COUNT));
		}
		
		layout.add(lt_flDataFile, tableName + "_fld");
		if (vlDataFileNeeded) {
			layout.add(lt_vlDataFile, tableName + "_vld");
		}
		
		return layout;
	}
	
	/**
	 * Creates a new and empty file, failing if the file already exists.
	 * If the specified path string turns out to denote a relative path then
	 * it is resolved against the specified layout directory.
	 * 
	 * @param  pathStr The path string, not allowed to be {@code null} or an
	 *         empty string.
	 * @param  layoutDir The directory of the layout, not allowed to be
	 *         {@code null}.
	 *         The layout's directory is used to convert relative file paths
	 *         contained in the layout to absolute file paths.
	 * 
	 * @throws NullPointerException If the path string denotes a relative path
	 *         and the layout directory is {@code null}.
	 * @throws IllegalArgumentException If the path string is {@code null} or an
	 *         empty string.
	 * @throws InvalidPathException If the path string is invalid.
	 * @throws FileIOException If the file already exists or if another I/O
	 *         error occurs.
	 */
	private static final void createFile(String pathStr, Path layoutDir) throws
											NullPointerException, IllegalArgumentException,
												InvalidPathException, FileIOException {
		final Path path = Utils.buildPath(pathStr, layoutDir);
		try {
			Files.createFile(path);
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
	}
	
	/**
	 * Creates the files used by the store described in the specified layout,
	 * failing if at least one of the files already exist.
	 * The created files are empty.
	 * 
	 * @param  layout The layout of the store, not allowed to be {@code null}.
	 * @param  layoutDir The directory of the layout, not allowed to be
	 *         {@code null}.
	 *         The layout's directory is used to convert relative file paths
	 *         contained in the layout to absolute file paths.
	 * 
	 * @throws NullPointerException If {@code layout} is {@code null} or if
	 *         the path string of a file in the layout denotes a relative path
	 *         and the layout directory is {@code null}.
	 * @throws MissingEntryException If a required entry in the layout is
	 *         missing.
	 * @throws IllegalArgumentException If a path string of a file in the
	 *         layout is an empty string.
	 * @throws InvalidPathException If a path string of a file in the layout is
	 *         invalid.
	 * @throws IOFailureException If a file already exists or if another I/O
	 *         error occurs.
	 */
	public static final void createFiles(Layout layout, Path layoutDir) throws
										MissingEntryException, IllegalArgumentException,
										InvalidPathException, IOFailureException {
		try {
			createFile(layout.getString(lt_flDataFile), layoutDir);
			if (layout.contains(lt_vlDataFile)) {
				createFile(layout.getString(lt_vlDataFile), layoutDir);
			}
		} catch (FileIOException e) {
			throw new IOFailureException(e);
		}
	}
	
	/**
	 * Just implements the {@code WRStoreInfo} interface.
	 * 
	 * @author Beat Hoermann
	 */
	public static final class Info implements WRStoreInfo {
		private final WRStore store;
		private final FLFileSpace flFileSpace;
		private final VLFileSpace vlFileSpace;
		
		/**
		 * Creates the info object of the specified WR store.
		 * 
		 * @param  wrStore The WR store, not allowed to be {@code null}.
		 * 
		 * @throws NullPointerException If {@code store} is {@code null}.
		 */
		public Info(WRStore wrStore) throws NullPointerException {
			this.store = wrStore;
			this.flFileSpace = wrStore.flFileSpace;
			this.vlFileSpace = wrStore.vlFileSpace;
		}
		
		@Override
		public final Path flDataFile() {
			return store.flDataFile.path;
		}

		@Override
		public final Path vlDataFile() {
			return store.vlDataFile.path;
		}

		@Override
		public final long flUsed() {
			return (flFileSpace.nofBlocks() - flFileSpace.nofGaps()) * store.n;
		}

		@Override
		public final long flUnused() {
			return flFileSpace.nofGaps() * store.n;
		}

		@Override
		public final long vlUsed() {
			if (vlFileSpace != null)
				// VL file space exists && database is writable.
				return vlFileSpace.allocated();
			else if (store.vlDataFile.path != null)
				// VL file space exists && database is not writable.
				return new VLFileSpace(store.vlDataFile, null).allocated();
			else {
				// No VL file space exists.
				return 0;
			}
		}
		
		@Override
		public final long vlUnused() {
			if (vlFileSpace != null)
				// VL file space exists && database is writable.
				return vlFileSpace.deallocated();
			else if (store.vlDataFile.path != null)
				// VL file space exists && database is not writable.
				return new VLFileSpace(store.vlDataFile, null).deallocated();
			else {
				// No VL file space exists.
				return 0;
			}
		}

		@Override
		public final int nobsRowRef() {
			return store.nobsRowRef;
		}

		@Override
		public final int nobsOutrowPtr() {
			return store.nobsOutrowPtr;
		}

		@Override
		public final int nobsRefCount() {
			return store.nobsRefCount;
		}
		
		@Override
		public final long highestRefCount() throws IOFailureException {
			if (store.nobsRefCount == 0) {
				return -1;
			}
			// store.nobsRefCount > 0.
			
			long highest = 0;
			
			final int nBM = store.nBM;
			final int nRC = store.nobsRefCount;
			
			final long nofBlocks = store.flFileSpace.nofBlocks();
			// Note that an empty array of columns results in a "whole FL data
			// reader".
			IFLDataReader flDataReader = FLDataReader.createNextFLDataReader(
								new Column_<?>[0], store, 0, nofBlocks, new Buffer());
			try {
				store.flDataFile.open();
				try {
					for (long index = 0; index < nofBlocks; index++) {
						final FLData flData = flDataReader.readFLData(index);
						// flData == null <=> row gap
						if (flData != null) {
							final long rc = Utils.unsFromBytes(flData.bytes, nBM, nRC);
							if (rc > highest) {
								highest = rc;
							}
						}
					}
				} finally {
					store.flDataFile.close();
				}
			} catch(FileIOException e) {
				throw new IOFailureException(e);
			}
			
			return highest;
		}

		@Override
		public final int blockSize() {
			return store.n;
		}

		@Override
		public final long numberOfBlocks() {
			return flFileSpace.nofBlocks();
		}

		@Override
		public final long maxNumberOfBlocks() {
			return Utils_.bnd8[store.nobsRowRef];
		}

		@Override
		public final long sizeOfVlDataFile() {
			try {
				return Files.size(store.vlDataFile.path);
			} catch (IOException e) {
				return -1;
			}
		}

		@Override
		public final long maxSafeSizeOfVlDataFile() {
			final long max = Utils_.bnd8[store.nobsOutrowPtr];
			return store.nobsOutrowPtr == 8 ? max : (store.nobsOutrowPtr == 0 ?
																					-1 : max + 1);
		}
	}
	
	/**
	 * A global buffer houses three reusable {@linkplain Buffer byte buffers}
	 * each having a maximum capacity equal to the value passed via the
	 * constructor.
	 * <p>
	 * A database has only one single instance of this class and this instance
	 * can  only be accessed via an instance of a {@link WRStore}.
	 * <p>
	 * What is the motivation for such a global buffer?
	 * Some ACDP database operations use relatively large buffers to reduce the
	 * number of file reads and file writes.
	 * Since the repeated process of allocating and deallocating such relatively
	 * large portions of main memory can be time consuming, the goal is to reuse
	 * as often as possible a once allocated relatively large buffer at the cost
	 * of a higher memory footprint.
	 * This can be achieved by globally providing such a buffer.
	 * However, care must be taken whenever different threads running in parallel
	 * need access to such a global resource.
	 * Luckily, ACDP never executes writes in parallel, thus making the use of a
	 * global buffer attractive for operations involving writes.
	 * <p>
	 * Of course, it is necessary to prevent subroutines from messing up a global
	 * buffer.
	 *
	 * @author Beat Hoermann
	 */
	public static final class GlobalBuffer {
		/**
		 * The global buffer <em>number one</em>, never {@code null}.
		 * <p>
		 * Note that the {@code gb1.buf(int)} method may return a reference to a
		 * byte buffer that is identical to a reference returned by a previous
		 * invocation of this method, hence, the buffers returned by this method
		 * may not be memory independent.
		 * <p>
		 * To get a buffer that is guaranteed to be memory independent from the
		 * buffer returned by the {@code gb1.buf(int)} method invoke the
		 * {@code gb2.buf(int)} or the {@code gb3.buf(int)} method.
		 */
		private final Buffer gb1;
		
		/**
		 * The global buffer <em>number two</em>, never {@code null}.
		 * <p>
		 * Note that the {@code gb2.buf(int)} method may return a reference to a
		 * byte buffer that is identical to a reference returned by a previous
		 * invocation of this method, hence, the buffers returned by this method
		 * may not be memory independent.
		 * <p>
		 * To get a buffer that is guaranteed to be memory independent from the
		 * buffer returned by the {@code gb2.buf(int)} method invoke the
		 * {@code gb1.buf(int)} or the {@code gb3.buf(int)} method.
		 */
		private final Buffer gb2;
		
		/**
		 * The global buffer <em>number three</em>, never {@code null}.
		 * <p>
		 * Note that the {@code gb3.buf(int)} method may return a reference to a
		 * byte buffer that is identical to a reference returned by a previous
		 * invocation of this method, hence, the buffers returned by this method
		 * may not be memory independent.
		 * <p>
		 * To get a buffer that is guaranteed to be memory independent from the
		 * buffer returned by the {@code gb3.buf(int)} method invoke the
		 * {@code gb1.buf(int)} or the {@code gb2.buf(int)} method.
		 */
		private final Buffer gb3;
		
		/**
		 * The constructor.
		 * <p>
		 * Some sources recommend using a multiple of 4 KiB = 4096 bytes to be a
		 * good value for reading some data from a disk into main memory.
		 * 
		 * @param maxCap The maximum capacity of the byte buffers returned by the
		 *       {@code b1.buf(int)}, {@code b2.buf(int)}, and  {@code
		 *       b3.buf(int)} methods.
		 */
		public GlobalBuffer(int maxCap) {
			gb1 = new Buffer(maxCap);
			gb2 = new Buffer(maxCap);
			gb3 = new Buffer(maxCap);
		}
	}
	
	/**
	 * Wraps a {@link Column_} object and keeps additional information about that
	 * column.
	 * <p>
	 * Strictly speaking, the properties {@link #o2b} and {@link #b2o} cannot
	 * be considered as "additional information about the column" but "augmented
	 * column functionality".
	 * 
	 * @author Beat Hoermann
	 */
	static final class WRColInfo {
		/**
		 * The column, never {@code null}.
		 */
		final Column_<?> col;
		
		/**
		 * Used for coding and decoding the bitmap if the value in this column
		 * is allowed to be {@code null}.
		 * <p>
		 * The value differs from zero if and only if the type of the column is
		 * either a nullable INROW ST or an INROW A[INROW ST] or an INROW A[RT].
		 * <p>
		 * If the value is different from zero then it is equal to 2<sup>{@code
		 * m}</sup>, where 0 &le; {@code m} &le; 62.
		 */
		long nullBitMask;
		
		/**
		 * The index where the FL column data starts within the FL data block.
		 */
		int offset;
		
		/**
		 * The length of the FL column data, greater than or equal to 1.
		 */
		int len;
		
		/**
		 * If the column is an RT or an A[RT] column then this is the store of
		 * the referenced table, {@code null} otherwise.
		 */
		WRStore refdStore;
		
		/**
		 * If the column is an array type then this is the number of bytes needed
		 * to save the number of elements of the array value.
		 * In all other cases {@code sizeLen} is not defined.
		 */
		int sizeLen;
		
		/**
		 * If the column is an OUTROW ST or an INROW A[OUTROW ST] or an OUTROW
		 * A[ST/RT] then this is the number of bytes needed to save the length of
		 * the byte representation of any value.
		 * In all other cases {@code lengthLen} is not defined. 
		 */
		int lengthLen;
		
		/**
		 * The converter that converts a value to its FL data, never {@code
		 * null}.
		 * <p>
		 * Read the {@link ObjectToBytes} class description to learn more about
		 * this important function.
		 */
		IObjectToBytes o2b;
		
		/**
		 * The converter that converts some FL data to its value, never {@code
		 * null}.
		 * <p>
		 * Read the {@link BytesToObject} class description to learn more about
		 * this important function.
		 */
		IBytesToObject b2o;
		
		/**
		 * The constructor.
		 * 
		 * @param col The column, not allowed to be {@code null}.
		 */
		private WRColInfo(Column_<?> col) {
			this.col = col;
		}
		
		@Override
		public final int hashCode() {
			return col.name().hashCode();
		}
		
		@Override
		public final boolean equals(Object obj) {
			if (obj instanceof WRColInfo)
				return this.col.equals(((WRColInfo) obj).col);
			else {
				return false;
			}
		}
	}
	
	/**
	 * Maps a column to its column information.
	 */
	final Map<Column_<?>, WRColInfo> colInfoMap;
	
	/**
	 * The column informations.
	 * The order and length of the array is in strict accordance with the table
	 * definition.
	 */
	final WRColInfo[] colInfoArr;
	
	/**
	 * The global buffer <em>number one</em>, never {@code null}.
	 * <p>
	 * Note that the {@code gb1.buf(int)} method may return a reference to a
	 * byte buffer that is identical to a reference returned by a previous
	 * invocation of this method, hence, the buffers returned by this method
	 * may not be memory independent.
	 * <p>
	 * To get a buffer that is guaranteed to be memory independent from the
	 * buffer returned by the {@code gb1.buf(int)} method invoke the
	 * {@code gb2.buf(int)} or the {@code gb3.buf(int)} method.
	 */
	final Buffer gb1;
	/**
	 * The global buffer <em>number two</em>, never {@code null}.
	 * <p>
	 * Note that the {@code gb2.buf(int)} method may return a reference to a
	 * byte buffer that is identical to a reference returned by a previous
	 * invocation of this method, hence, the buffers returned by this method
	 * may not be memory independent.
	 * <p>
	 * To get a buffer that is guaranteed to be memory independent from the
	 * buffer returned by the {@code gb2.buf(int)} method invoke the
	 * {@code gb1.buf(int)} or the {@code gb3.buf(int)} method.
	 */
	final Buffer gb2;
	/**
	 * The global buffer <em>number three</em>, never {@code null}.
	 * <p>
	 * Note that the {@code gb3.buf(int)} method may return a reference to a
	 * byte buffer that is identical to a reference returned by a previous
	 * invocation of this method, hence, the buffers returned by this method
	 * may not be memory independent.
	 * <p>
	 * To get a buffer that is guaranteed to be memory independent from the
	 * buffer returned by the {@code gb3.buf(int)} method invoke the
	 * {@code gb1.buf(int)} or the {@code gb2.buf(int)} method.
	 */
	final Buffer gb3;
	
	/**
	 * The store's layout.
	 */
	private final Layout layout;
	
	/**
	 * The database.
	 */
	private final Database_ db;
	
	/**
	 * The FL data file, never {@code null}.
	 */
	final FileIO flDataFile;
	/**
	 * The VL data file, never {@code null}.
	 * <p>
	 * The value can be considered final after the constructor has been executed.
	 * It can't be declared final because the service operations "modify column"
	 * and "insert column" may require a store not having a VL file space yet to
	 * be able to store some outrow data.
	 */
	FileIO vlDataFile;

	/**
	 * The FL file space, never <code>null</code>.
	 * <p>
	 * The value can be considered final after the {@link #initialize} method
	 * has been executed.
	 */
	FLFileSpace flFileSpace;
	/**
	 * The VL file space or <code>null</code> if and only if the table stores
	 * no outrow data.
	 * <p>
	 * The value can be considered final after the constructor has been executed.
	 * It can't be declared final because the service operations "modify column"
	 * and "insert column" may require a store that has no VL file space yet to
	 * be able to store some outrow data.
	 */
	VLFileSpace vlFileSpace;
	
	/**
	 * The number of bytes required for referencing a row, &ge; 1 and &le; 8.
	 * <p>
	 * See the section "Explanation of the {@code nobsRowRef}, {@code
	 * nobsOutrowPtr} and {@code nobsRefCount} Settings" in the description of
	 * the {@linkplain acdp.tools.Setup Setup Tool} for further information.
	 */
	final int nobsRowRef;
	/**
	 * The number of bytes required for referencing any outrow data, &ge; 0 and
	 * &le; 8.
	 * If the value is equal to zero then the layout is missing the corresponding
	 * field, meaning that this store has no VL file space.
	 * <p>
	 * See the section "Explanation of the {@code nobsRowRef}, {@code
	 * nobsOutrowPtr} and {@code nobsRefCount} Settings" in the description of
	 * the {@linkplain acdp.tools.Setup Setup Tool} for further information.
	 * <p>
	 * The value can be considered final after the constructor has been executed.
	 * It can't be declared final because the service operations "modify column"
	 * and "insert column" may require a store not having a VL file space yet to
	 * be able to store some outrow data.
	 */
	int nobsOutrowPtr;
	/**
	 * The number of bytes used by the reference counter in the header, &ge; 0
	 * and &le; 8.
	 * If the value is equal to zero then the layout is missing the corresponding
	 * field, meaning that this store is not referenced by this or any other
	 * store.
	 * <p>
	 * See the section "Explanation of the {@code nobsRowRef}, {@code
	 * nobsOutrowPtr} and {@code nobsRefCount} Settings" in the description of
	 * the {@linkplain acdp.tools.Setup Setup Tool} for further information.
	 */
	final int nobsRefCount;
	
	/**
	 * The insert operation.
	 */
	final Insert insert;
	/**
	 * The update operation.
	 */
	final Update update;
	/**
	 * The delete operation.
	 */
	final Delete delete;

	/**
	 * The size of the bitmap in bytes, &gt; 0 and &le; 8, see the class
	 * description.
	 * <p>
	 * The value can be considered final after the {@link #initialize} method
	 * has been executed.
	 */
	int nBM;
	/**
	 * The number of header bytes, &gt; 0 and &le; 16, see the class description.
	 * <p>
	 * The value can be considered final after the {@link #initialize} method
	 * has been executed.
	 */
	int nH;
	/**
	 * The number of excess bytes, &ge; 0 and &le; 6, see the class description.
	 * <p>
	 * The value can be considered final after the {@link #initialize} method
	 * has been executed.
	 */
	int nE;
	/**
	 * The size of an FL data block, &ge; 8, see the class description.
	 * <p>
	 * The value can be considered final after the {@link #initialize} method
	 * has been executed.
	 */
	int n;
	
	/**
	 * Fills the {@code colInfoMap} and the {@code colInFoArr} properties.
	 * 
	 * @param tableDef The table definition of the store's table.
	 */
	private final void fillColInfoMapArr(Column_<?>[] tableDef) {
		
		int i = 0;
		for (Column_<?> col : tableDef) {
			WRColInfo colInfo = new WRColInfo(col);
			
			colInfoMap.put(col, colInfo);
			colInfoArr[i++] = colInfo;
		}
	}
	
	/**
	 * Checks the specified layout.
	 * 
	 * @param layout The layout of the store.
	 * @param table The table.
	 * @param vlDataFileNeeded Indicates if the VL data file is needed.
	 * @param referenced Indicates if the table is referenced by a table.
	 * 
	 * @throws CreationException If the specified layout is invalid.
	 */
	private final void checkLayout(Layout layout, Table_ table,
									boolean vlDataFileNeeded, boolean referenced) throws
																				CreationException {
		try {
			if (layout.getString(lt_flDataFile).isEmpty())
				throw new IllegalArgumentException("Entry \"" + lt_flDataFile +
																		"\" is an empty string.");
			else if (!vlDataFileNeeded && layout.contains(lt_vlDataFile))
				throw new IllegalArgumentException("Entry \"" + lt_vlDataFile +
																"\" not used. Remove entry.");
			else if (vlDataFileNeeded && layout.getString(lt_vlDataFile).isEmpty())
				throw new IllegalArgumentException("Entry \"" + lt_vlDataFile +
																		"\" is an empty string.");
			else if (layout.getString(lt_nobsRowRef).isEmpty())
				throw new IllegalArgumentException("Entry \"" + lt_nobsRowRef +
																		"\" is an empty string.");
			else if (!vlDataFileNeeded && layout.contains(lt_nobsOutrowPtr))
				throw new IllegalArgumentException("Entry \"" + lt_nobsOutrowPtr +
																"\" not used. Remove entry.");
			else if (vlDataFileNeeded &&
											layout.getString(lt_nobsOutrowPtr).isEmpty()) 
				throw new IllegalArgumentException("Entry \"" + lt_nobsOutrowPtr +
																		"\" is an empty string.");
			else if (!referenced && layout.contains(lt_nobsRefCount))
				throw new IllegalArgumentException("Entry \"" + lt_nobsRefCount +
																"\" not used. Remove entry.");
			else if (referenced && layout.getString(lt_nobsRefCount).isEmpty()) {
				throw new IllegalArgumentException("Entry \"" + lt_nobsRefCount +
																		"\" is an empty string.");
			}
		} catch (Exception e) {
			throw new CreationException(table, "Invalid WR store layout.", e);
		}
	}
	
	/**
	 * Tests whether the specified file path exists, whether it points to a file
	 * and whether the file is either writable or readable depending on the
	 * store being writable or read-only.
	 * 
	 * @param  filepath The path of the file.
	 * 
	 * @throws CreationException If the path does not exist, points to a
	 *         directory or the file is not writable/readable.
	 */
	private final void check(Path filepath) throws CreationException {
		boolean ready = true;
		try {
			ready = !Files.isDirectory(filepath) && Files.isReadable(filepath) &&
												(!writable || Files.isWritable(filepath));
		} catch (Exception e) {
			ready = false;
		}
		if (!ready) {
			throw new CreationException(table, "Backing file not ready for " +
													"being opened. Check the existence " +
													"of the backing file: " + filepath);
		}
	}
	
	/**
	 * The constructor.
	 * 
	 * @param  layout The layout of the store.
	 * @param  layoutDir The directory of the layout.
	 *         The layout's directory is used to convert relative file paths
	 *         contained in the layout to absolute file paths.
	 * @param  referenced If {@code true} then the associated table is referenced
	 *         by itself or by another table within the database.
	 * @param  globalBuffer The global buffer, not allowed to be {@code null}.
	 * @param  table The associated table of this new store instance.
	 * 
	 * @throws NullPointerException If one of the parameters is {@code null}.
	 * @throws InvalidPathException If the path string of a data file in the
	 *         layout is invalid.
	 * @throws CreationException If the store can't be created due to any other
	 *         reason including problems with the layout and the backing files
	 *         of the store.
	 */
	public WRStore(Layout layout, Path layoutDir, boolean referenced,
								GlobalBuffer globalBuffer, Table_ table) throws
								NullPointerException, InvalidPathException,
																				CreationException {
		super(table);
		checkLayout(layout, table, vlDataFileNeeded(table.tableDef()),
																						referenced);
		final Column_<?>[] tableDef = table.tableDef();
		this.colInfoMap = new HashMap<>(tableDef.length * 4 / 3 + 1);
		this.colInfoArr = new WRColInfo[tableDef.length];
		fillColInfoMapArr(tableDef);
		
		this.gb1 = globalBuffer.gb1;
		this.gb2 = globalBuffer.gb2;
		this.gb3 = globalBuffer.gb3;
		this.layout = layout;
		this.db = table.db();
		final Path flDataPath = Utils.buildPath(layout.getString(lt_flDataFile),
																							layoutDir);
		check(flDataPath);
		this.flDataFile = new FileIO(flDataPath, db.fcProvider());
		
		if (layout.contains(lt_vlDataFile)) {
			final Path vlDataPath = Utils.buildPath(layout.getString(
																	lt_vlDataFile), layoutDir);
			check(vlDataPath);
			this.vlDataFile = new FileIO(vlDataPath, db.fcProvider());
			this.vlFileSpace = new VLFileSpace(vlDataFile, db.fssTracker());
		}
		else {
			this.vlDataFile = new FileIO(null, db.fcProvider());
			this.vlFileSpace = null;
		}
		
		try {
			this.nobsRowRef = Integer.parseInt(layout.getString(lt_nobsRowRef));
		} catch (NumberFormatException e) {
			throw new CreationException(table, "Value of \"" + lt_nobsRowRef +
											"\" layout field is not an integer: " +
											layout.getString(lt_nobsRowRef) + ".", e);
		}
		if (nobsRowRef < 1 || 8 < nobsRowRef) {
			throw new CreationException(table, "Wrong integer value for " +
											"layout field \"" + lt_nobsRowRef + "\": " +
																				nobsRowRef + ".");
		}
		
		if (layout.contains(lt_nobsOutrowPtr)) {
			try {
				this.nobsOutrowPtr = Integer.parseInt(layout.getString(
																				lt_nobsOutrowPtr));
			} catch (NumberFormatException e) {
				throw new CreationException(table, "Value of \"" +
							lt_nobsOutrowPtr + "\" layout field is not an integer: " +
											layout.getString(lt_nobsOutrowPtr) + ".", e);
			}
			if (nobsOutrowPtr < 1 || 8 < nobsOutrowPtr) {
				throw new CreationException(table, "Wrong integer value for " +
							"layout field \"" + lt_nobsOutrowPtr + "\": " +
																			nobsOutrowPtr + ".");
			}
		}
		else {
			this.nobsOutrowPtr = 0;
		}
		
		if (layout.contains(lt_nobsRefCount)) {
			try {
				this.nobsRefCount = Integer.parseInt(layout.getString(
																				lt_nobsRefCount));
			} catch (NumberFormatException e) {
				throw new CreationException(table, "Value of \"" + lt_nobsRefCount +
											"\" layout field is not an integer: " +
											layout.getString(lt_nobsRefCount) + ".", e);
			}
			if (nobsRefCount < 1 || 8 < nobsRefCount) {
				throw new CreationException(table, "Wrong integer value for " +
											"layout field \"" + lt_nobsRefCount + "\": " +
																				nobsRefCount + ".");
			}
		}
		else {
			this.nobsRefCount = 0;
		}
		
		// Create and set the operations.
		insert = new Insert(this);
		update = new Update(this);
		delete = new Delete(this);
		setReadOp(new WRReadOp(this));
	}

	/**
	 * Taking the specified array type having an <em>inrow</em> storage scheme
	 * this method computes and returns the number of bytes needed to persist a
	 * value of this type.
	 * 
	 * @param  at The array type having an <em>inrow</em> storage scheme.
	 * @param  ci The column info object.
	 * 
	 * @return The number of bytes needed to persist a value of this type,
	 *         greater than or equal to 1.
	 * 
	 * @throws ImplementationRestrictionException If the number of bytes needed
	 *         to persist a value of the specified array type exceeds
	 *         Java's maximum array size.
	 */
	private final int lenInrowArray(AbstractArrayType at, WRColInfo ci) throws
														ImplementationRestrictionException {
		int maxSize = at.maxSize();
		long len;
		if (at instanceof ArrayType_) {
			SimpleType<?> st = ((ArrayType_) at).elementType();
			if (st.scheme() == Scheme.INROW ) {
				// INROW A[INROW ST]
				len = ci.sizeLen + (long) maxSize * st.length();
				if (st.nullable()) {
					len += Utils.bmLength(maxSize);
				}
				if (len > Integer.MAX_VALUE) {
					throw new ImplementationRestrictionException(table, "The " +
							"number of bytes needed to persist a value of column \"" +
							ci.col + "\" being of an inrow array type with an inrow " +
							"element type exceeds Java\'s maximum array size: " +
							len + ".");
				}
			}	
			else {
				// INROW A[OUTROW ST]
				len = ci.lengthLen + nobsOutrowPtr;
			}
		}
		else {
			// INROW ArrayOfRefType_
			len = ci.sizeLen + (long) maxSize * ci.refdStore.nobsRowRef;
			if (len > Integer.MAX_VALUE) {
				throw new ImplementationRestrictionException(table, "The number " +
							"of bytes needed to persist a value of column \"" +
							ci.col + "\" being of an inrow array of references " +
							"type exceeds Java\'s maximum array size: " + len + ".");
			}
			
		}
		return (int) len;
	}
	
	/**
	 * Initializes the {@code refdStore}, the {@code sizeLen}, the {@code
	 * lengthLen}, and the {@code len} fields of the column information for
	 * the column given by the specified column information object.
	 * Note that these fields are not depending on the position of the column
	 * within the row.
	 * <p>
	 * This method assumes that all tables of the database are created and
	 * initialized.
	 * 
	 * @param ci The column information object to initialize.
	 * 
	 * @throws ImplementationRestrictionException If the column is of
	 *         INROW A[INROW ST] or INROW A[RT] and the number of bytes
	 *         needed to persist an array of maximum size exceeds Java's
	 *         maximum array size.
	 */
	private final void initBasicColInfo(WRColInfo ci) throws
														ImplementationRestrictionException {
		final Type_ type = ci.col.type();
		
		// Initialize "refdStore".
		if (type instanceof RefType_ || type instanceof ArrayOfRefType_) {
			// Column referencing a table.
			String refdTable = ci.col.refdTable();
			ci.refdStore = (WRStore) ((Table_) table.db().getTable(refdTable)).
																								store();
		}
		else {
			ci.refdStore = null;
		}
			
		// Initialize "sizeLen", "lengthLen".
		if (type instanceof SimpleType && type.scheme() == Scheme.OUTROW)
			ci.lengthLen = ((SimpleType<?>) type).length();
		else if (type instanceof AbstractArrayType) {
			final int maxSize = ((AbstractArrayType) type).maxSize();
			ci.sizeLen = Utils.lor(maxSize);
			long maxLength;
			if (type instanceof ArrayType_) {
				SimpleType<?> st = ((ArrayType_) type).elementType();
				final int stLen = st.length();
				maxLength = (long) maxSize * (st.scheme() == Scheme.INROW ?
														stLen : stLen + Utils_.bnd4[stLen]);
				if (st.nullable()) {
					maxLength += Utils.bmLength(maxSize);
				}
			}
			else {
				// type instanceof ArrayOfRefType_
				maxLength = (long) maxSize * ci.refdStore.nobsRowRef;
			}
			ci.lengthLen = Utils.lor(ci.sizeLen + maxLength);
		}
			
		// Initialize "len".
		if (type.scheme() == Scheme.INROW) {
			// INROW
			if (type instanceof SimpleType)
				ci.len = ((SimpleType<?>) type).length();
			else if (type instanceof RefType_)
				ci.len = ci.refdStore.nobsRowRef;
			else {
				// AbstractArrayType
				AbstractArrayType at = (AbstractArrayType) type;
				ci.len = lenInrowArray(at, ci);
			}
		}
		else {
			// OUTROW
			ci.len = ci.lengthLen + nobsOutrowPtr;
		}
	}
	
	/**
	 * Initializes all but the {@code offset} field of the column information
	 * and returns the size of the bitmap in bytes.
	 * <p>
	 * This method assumes that all tables of the database are created and
	 * initialized.
	 *         
	 * @return The size of the bitmap in bytes, always greater than zero and
	 *         less than or equal to 8.
	 *         
	 * @throws ImplementationRestrictionException If the table has too many
	 *         columns needing a separate null information.
	 */
	private final int initColInfoAllButOffset() throws
														ImplementationRestrictionException {
		final ObjectToBytes o2bFactory = new ObjectToBytes(this);
		final BytesToObject b2oFactory = new BytesToObject(this);
		long mask = 1L;
		int niLen = 0;
		
		for (WRColInfo ci : colInfoArr) {
			// Initialize "nullBitMask".
			final Type_ type = ci.col.type();
			if (type.scheme() == Scheme.INROW &&
				 (type instanceof SimpleType && ((SimpleType<?>) type).nullable() ||
				  type instanceof ArrayOfRefType_ || type instanceof ArrayType_ &&
					 	((ArrayType_) type).elementType().scheme() == Scheme.INROW)) {
				// INROW nullable ST || INROW A[RT] || INROW A[INROW ST]
				// Other types may also allow the null-value but the null-value can
				// be expressed either as a row index equal to 0 (RefType_) or as a
				// pointer to an outrow file position equal to 0.
				ci.nullBitMask = mask;
				mask <<= 1;
				niLen++;
			}
			else {
				ci.nullBitMask = 0;
			}
			// Initialize "len", "refdStore", "sizeLen", "lengthLen".
			initBasicColInfo(ci);
			// Initialize "o2b" and "b2o".
			ci.o2b = o2bFactory.create(ci);
			ci.b2o = b2oFactory.create(ci);
		}
		
		if (niLen > 63) {
			throw new ImplementationRestrictionException(table, "Table has too " +
									"many columns needing a separate null information.");
		}
		// niLen <= 63 guarantees that there is room for one bit left which can be
		// used to mark the row to be a "real" row and not a row gap.
		
		return Utils.bmLength(niLen + 1); // first bit is row gap flag.
	}
	
	/**
	 * Initializes this store.
	 * <p>
	 * This method assumes that all tables of the database are created and
	 * initialized.
	 * 
	 * @throws ImplementationRestrictionException If the table has too many
	 *         columns needing a separate null information or if the size of the
	 *         FL data block exceeds {@link Integer#MAX_VALUE}.
	 * @throws CreationException If the FL file space can't be created.
	 */
	public final void initialize() throws ImplementationRestrictionException,
																				CreationException {
		// Initialize all but the "offset" field of the column information and
		// initialize "nBM".
		nBM = initColInfoAllButOffset();
		
		// Initialize "nH".
		nH = nBM + nobsRefCount;
		
		// Compute net length of the FL data block.
		n = nH;
		for (WRColInfo ci : colInfoArr) { 
			n += ci.len;
			if (n < 0) {
				throw new ImplementationRestrictionException(table, "The " +
									"number of bytes needed to save an FL data block " +
						         "exceeds Java\'s maximum array size. Use outrow " +
									"storage scheme instead of inrow storage scheme.");
			}
		}
		
		// Compute excess and if it exists add it to "n".
		if (n >= 8)
			nE = 0;
		else {
			nE = 8 - n;
			// nE > 0;
			n += nE;
			// n == 8;
		}
		// n >= 8
		
		// Initialize the "offset" field of the column information.
		int offset = nH;
		for (WRColInfo ci : colInfoArr) {
			// Initialize "offset".
			ci.offset = offset;
			offset += ci.len;
		}
		
		// Create FL file space.
		// n >= 8
		if (writable)
			flFileSpace = new FLFileSpace(flDataFile, db.fssTracker(), n);
		else {
			// Create FL file space in read-only mode.
			flFileSpace = new FLFileSpace(flDataFile, null, n);
		}
	}
	
	/**
	 * Converts the specified row index to a file position.
	 * 
	 * @param  rowIndex The row index, must be greater than zero.
	 * 
	 * @return The file position.
	 */
	final long riToPos(long rowIndex) {
		return flFileSpace.indexToPos(rowIndex - 1);
	}
	
	/**
	 * Converts the specifed file position to a row index.
	 * 
	 * @param  pos The file position, must be greater than or equal to zero.
	 * 
	 * @return The row index, greater than zero.
	 */
	final long posToRi(long pos) {
		return flFileSpace.posToIndex(pos) + 1;
	}
	
	@Override
	protected final long numberOfRowGaps() {
		return flFileSpace.nofGaps();
	}
	
	@Override
	public final long refToRi(Ref_ ref) throws IllegalReferenceException {
		final long ri = ref.rowIndex();
		if (ri < 1 || flFileSpace.nofBlocks() < ri) {
			throw new IllegalReferenceException(table, ref, false);
		}
		return ri;
	}
	
	@Override
	public final long numberOfRows() {
		return flFileSpace.nofBlocks() - flFileSpace.nofGaps();
	}

	@Override
	public final Ref_ insert(Object[] values) throws
							UnsupportedOperationException, NullPointerException,
							IllegalArgumentException, IllegalReferenceException,
							MaximumException, CryptoException, ShutdownException,
							ACDPException, UnitBrokenException, IOFailureException {
		if (!writable) {
			throw new UnsupportedOperationException(ACDPException.prefix(table) +
						"Database is read-only. Can't write to a WR store that " +
						"is write protected.");
		}
		return (Ref_) insert.execute(values);
	}

	@Override
	public final void delete(Ref_ ref) throws
							UnsupportedOperationException, NullPointerException,
							IllegalArgumentException, IllegalReferenceException,
							ShutdownException, ACDPException, UnitBrokenException,
							IOFailureException {
		if (!writable) {
			throw new UnsupportedOperationException(ACDPException.prefix(table) +
						"Database is read-only. Can't write to a WR store that is " +
						"write protected.");
		}
		delete.execute(ref);
	}

	@Override
	public final void update(Ref_ ref, ColVal<?>[] colVals) throws
							UnsupportedOperationException, NullPointerException,
							IllegalArgumentException, IllegalReferenceException,
							MaximumException, CryptoException, ShutdownException,
							ACDPException, UnitBrokenException, IOFailureException {
		if (!writable) {
			throw new UnsupportedOperationException(ACDPException.prefix(table) +
						"Database is read-only. Can't write to a WR store that is " +
						"write protected.");
		}
		update.execute(new Object[] { ref, colVals });
	}

	@Override
	public final void updateAll(ColVal<?>[] colVals) throws
							UnsupportedOperationException, NullPointerException,
							IllegalArgumentException, MaximumException,
							CryptoException, ShutdownException,
							ACDPException, UnitBrokenException, IOFailureException {
		if (!writable) {
			throw new UnsupportedOperationException(ACDPException.prefix(table) +
						"Database is read-only. Can't write to a WR store that is " +
						"write protected.");
		}
		new UpdateAllColVals(this).execute(colVals);
	}
	
	@Override
	public final void updateAllSupplyValues(Column_<?> col,
													ValueSupplier<?> valueSupplier) throws
						UnsupportedOperationException, NullPointerException,
						IllegalArgumentException, ImplementationRestrictionException,
						MaximumException, CryptoException, ShutdownException,
						ACDPException, UnitBrokenException, IOFailureException {
		if (!writable) {
			throw new UnsupportedOperationException(ACDPException.prefix(table) +
						"Database is read-only. Can't write to a WR store that is " +
						"write protected.");
		}
		new UpdateAllValueSupplier(this, col, valueSupplier).execute(null);
	}
	
	@Override
	public final void updateAllChangeValues(Column_<?> col,
														ValueChanger<?> valueChanger) throws 
						UnsupportedOperationException, NullPointerException,
						IllegalArgumentException, ImplementationRestrictionException,
						MaximumException, CryptoException, ShutdownException,
						ACDPException, UnitBrokenException, IOFailureException {
		if (!writable) {
			throw new UnsupportedOperationException(ACDPException.prefix(table) +
						"Database is read-only. Can't write to a WR store that is " +
						"write protected.");
		}
		new UpdateAllValueChanger(this, col, valueChanger).execute(null);
	}
	
	/**
	 * Eliminates the gaps of unused memory areas within the VL file space of
	 * this WR store and truncates the corresponding data file.
	 * <p>
	 * Since this method opens an ACDP zone and executes its main task within
	 * that zone, invoking this method can be done during a session.
	 * <p>
	 * This method has no effect if the store has no VL file space.
	 * 
	 * @throws ACDPException If the database is read-only or if this method is
	 *         invoked within a read zone or a unit.
	 * @throws ShutdownException If the database is closed.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	public final void compactVL() throws ACDPException, ShutdownException,
																				IOFailureException {
		new VLCompactor().run(this);
	}
	
	/**
	 * Eliminates the gaps of unused FL data blocks within the FL file space of
	 * this WR store and truncates the corresponding data file.
	 * <p>
	 * Since this method opens an ACDP zone and executes its main task within
	 * that zone, invoking this method can be done during a session.
	 * <p>
	 * <em>Note that example references may be invalidated by compacting the FL
	 * file space of a WR store.</em>
	 * 
	 * @throws ImplementationRestrictionException If the number of row gaps in
	 *         the store's table or in at least one of the tables referencing
	 *         that table is greater than {@code Integer.MAX_VALUE}.
	 * @throws ACDPException If the database is read-only or if this method is
	 *         invoked within a read zone or a unit.
	 * @throws ShutdownException If the database is closed.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	public final void compactFL() throws ImplementationRestrictionException,
								ACDPException, ShutdownException, IOFailureException {
		new FLCompactor().run(this);
	}
	
	/**
	 * Deletes all rows of the table associated with this WR store and truncates
	 * the corresponding data files.
	 * <p>
	 * Note that if the table contains at least one row which is referenced by
	 * a foreign row then this method throws a {@code DeleteConstraintException}.
	 * No changes are made to the database in such a situation.
	 * <p>
	 * Since this method opens an ACDP zone and executes its main task within
	 * that zone, invoking this method can be done during a session.
	 * 
	 * @throws DeleteConstraintException If the table associated with this WR
	 *         store contains at least one row which is referenced by a foreign
	 *         row.
	 *         This exception never happens if none of the tables in the database
	 *         reference the table associated with this WR store.
	 * @throws ACDPException If the database is read-only or if this method is
	 *         invoked within a read zone or a unit.
	 * @throws ShutdownException If the database is closed.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	public final void truncate() throws DeleteConstraintException, ACDPException,
													ShutdownException, IOFailureException {
		new Truncator().run(this);
	}
	
	/**
	 * Adds the data files of this store to a zip archive.
	 * 
	 * @param zec The zip entry collector.
	 * @param flat The information whether the data files should be added flat,
	 *        hence, without any path information or whether the data files
	 *        should be added with the paths as they appear in the store layout.
	 * 
	 * @throws IOException If an I/O error occurs.
	 */
	public final void zip(ZipEntryCollector zec, boolean flat) throws
																						IOException {
		if (flat) {
			zec.add(flDataFile.path);
			if (vlDataFile.path != null) {
				zec.add(vlDataFile.path);
			}
		}
		else {
			zec.add(lt_flDataFile, layout);
			if (layout.contains(lt_vlDataFile)) {
				zec.add(lt_vlDataFile, layout);
			}
		}
	}
	
	/**
	 * Deletes the files used by the store.
	 * <p>
	 * Note that this store and hence the whole database won't work properly
	 * anymore after this method has been executed.
	 * The database should be closed as soon as possible.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void deleteFiles() throws FileIOException {
		flDataFile.delete();
		if (vlDataFile.path != null) {
			vlDataFile.delete();
		}
	}
	
	/**
	 * Creates a column info object from the specified column, initializes
	 * the {@code refdStore}, the {@code sizeLen}, the {@code lengthLen}, and
	 * the {@code len} fields, and sets the {@code nullBitMask} field equal to
	 * zero.
	 * <p>
	 * This method does not initialize the {@code offset}, {@code o2b} and
	 * {@code b2o} fields.
	 * <p>
	 * A client may want to derive an {@code IObjectToBytes} converter from the
	 * returned column info object.
	 * Note, however, that the so constructed {@code IObjectToBytes} converter
	 * will be restricted in that the {@link IObjectToBytes#convert} method
	 * will return the same bitmap which was given to it in its {@code bitmap}
	 * parameter, regardless of any argument given to this method and
	 * irrespective of the type of the column.
	 * 
	 * @param  col The column, not allowed to be {@code null}.
	 * 
	 * @return The column info object, never {@code null}.
	 * 
	 * @throws ImplementationRestrictionException If the column is of
	 *         INROW A[INROW ST] or INROW A[RT] and the number of bytes
	 *         needed to persist an array of maximum size exceeds Java's
	 *         maximum array size.
	 */
	final WRColInfo createCi(Column_<?> col) throws
														ImplementationRestrictionException {
		final WRColInfo ci = new WRColInfo(col);
		initBasicColInfo(ci);
		ci.nullBitMask = 0;
		return ci;
	}
	
	/**
	 * Computes the <em>gaps ratio</em> in the VL file space of this store.
	 * <p>
	 * The gaps ratio in the VL file space is the size of the {@linkplain
	 * Info#vlUnused() unused VL file space} in relation to its total size.
	 * For instance, a value equal to 0.9 means that 9 out of 10 bytes in the
	 * VL file space are unused.
	 * If there is no unused VL file space then this method returns 0.
	 * If this store has no VL file space then this method returns -1.0.
	 * 
	 * @return The gaps ratio or -1 if the store has no VL file space.
	 */
	public final double computeGapsRatio() {
		if (!layout.contains(lt_vlDataFile))
			return -1.0;
		else {
			// VL file space still exists.
			final long d = vlFileSpace.deallocated();
			final long total = vlFileSpace.allocated() + d;
			return total > 0 ? ((double) d) / total : 0.0;
		}
	}
	
	/**
	 * Indicates whether this store is referenced.
	 * 
	 * @return The boolean value {@code true} if this store is referenced by
	 *         this or another store, {@code false} if this store is
	 *         unreferenced.
	 */
	public final boolean isReferenced() {
		return nobsRefCount > 0;
	}
	
	/**
	 * Returns the stores referenced by this store.
	 * 
	 * @return The stores referenced by this store, never {@code null} but may
	 *         be empty.
	 */
	public final Set<WRStore> refdStores() {
		final Set<WRStore> set = new HashSet<>();
		
		for (WRColInfo ci : colInfoArr) {
			if (ci.refdStore != null) {
				set.add(ci.refdStore);
			}
		}
		
		return set;
	}
	
	/**
	 * Returns the subset of the specified set of stores that reference this
	 * store.
	 * 
	 * @param  stores The set of stores, not allowed to be {@code null}.
	 * 
	 * @return The stores contained in the specified set of stores referencing
	 *         this store, never {@code null} but may be empty.
	 */
	public final Set<WRStore> refdBy(Set<WRStore> stores) {
		final Set<WRStore> refdBy = new HashSet<>();
		
		for(WRStore store : stores) {
			if (store.refdStores().contains(this)) {
				refdBy.add(store);
			}
		}
		
		return refdBy;
	}
	
	/**
	 * Returns the store that is referenced by the specified column.
	 *
	 * @param  col The column, not allowed to be {@code null}.
	 * 
	 * @return The referenced store or {@code null} if this column does not
	 *         reference a store.
	 */
	public final WRStore refdStore(Column_<?> col) {
		return colInfoMap.get(col).refdStore;
	}
	
	/**
	 * Removes the reference counter from this store.
	 * <p>
	 * Invoke this method only if you are sure that the reference counter exists
	 * and that it must now be removed because this store's table is no longer
	 * referenced by this table or any other table.
	 * <p>
	 * This method changes the store's layout.
	 * Furthermore, this method may reduce the size of the store's FL data
	 * blocks and therefore the size of the store's FL data file.
	 * <p>
	 * To run properly the FL data file is not allowed to be already open at the
	 * time this method is invoked.
	 * <p>
	 * Note also that this store and hence the whole database won't work
	 * properly anymore after this method has been executed.
	 * The database should be closed as soon as possible.
	 * <p>
	 * Note that the format of the FL data file may be corrupted if this method
	 * throws a {@code FileIOException}.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1},
	 * {@linkplain WRStore.GlobalBuffer GB2}.
	 * 
	 * @throws MissingEntryException If the store has no reference counter.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void removeRefCount() throws MissingEntryException,
																					FileIOException {
		layout.remove(lt_nobsRefCount);
		FLFileAccommodate.spot(nBM, -nobsRefCount).run(this);
	}
	
	/**
	 * Adds the reference counter to this store.
	 * <p>
	 * Invoke this method only if you are sure that this store has no reference
	 * counter and that it must be installed because this store's table is
	 * referenced by a table which is newly added to the database.
	 * <p>
	 * This method changes the store's layout.
	 * Furthermore, this method may increase the size of the store's FL data
	 * blocks and therefore the size of the store's FL data file.
	 * <p>
	 * To run properly the FL data file is not allowed to be already open at the
	 * time this method is invoked.
	 * <p>
	 * Note that this store and hence the whole database won't work properly
	 * anymore after this method has been executed.
	 * The database should be closed as soon as possible.
	 * <p>
	 * Note also that the format of the FL data file may be corrupted if this
	 * method throws a {@code FileIOException}.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1},
	 * {@linkplain WRStore.GlobalBuffer GB2}.
	 * 
	 * @throws IllegalArgumentException If this store already has a reference
	 *         counter.
	 * @throws ImplementationRestrictionException If the new size of the FL data
	 *         blocks exceeds {@code Integer.MAX_VALUE}.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void installRefCount() throws IllegalArgumentException,
									ImplementationRestrictionException, FileIOException {
		layout.add(lt_nobsRefCount, String.valueOf(NOBS_REF_COUNT));
		FLFileAccommodate.spot(nBM, NOBS_REF_COUNT).run(this);
	}
	
	/**
	 * Installs the VL file space if the store has no VL file space yet and the
	 * specified column is a VL column.
	 * 
	 * @param  col The new column, not allowed to be {@code null}.
	 * @param  layoutDir The directory of the layout.
	 * 
	 * @throws NullPointerException If {@code col} is {@code null} or if {@code
	 *         layoutDir} is {@code null}, provided that the layout directory
	 *         is needed.
	 * @throws FileIOException If the FL data file already exists or if another
	 *         I/O error occurs.
	 */
	private final void installVLFileSpace(Column_<?> col, Path layoutDir) throws
													NullPointerException, FileIOException {
		if (nobsOutrowPtr == 0 && isVLCol(col)) {
			final String fn = table.name() + "_vld";
			
			createFile(fn, layoutDir);
			layout.add(lt_vlDataFile, fn);
			layout.add(lt_nobsOutrowPtr, String.valueOf(NOBS_OUTROW_PTR));
			
			// The ColInsert/ColModify.run-methods rely on the store being able to
			// write to the VL file space.
			vlDataFile = new FileIO(Utils.buildPath(fn, layoutDir),
																					db.fcProvider());
			vlFileSpace = new VLFileSpace(vlDataFile, db.fssTracker());
			nobsOutrowPtr = NOBS_OUTROW_PTR;
		}
	}
	
	/**
	 * Inserts the specified column into this store.
	 * <p>
	 * If {@code installRefCount} is equal to {@code true} then this method not
	 * only inserts the specified column but the reference counter as well and
	 * adjusts the layout accordingly.
	 * <p>
	 * If the specified column is a VL column and it is the only VL column of
	 * the table then this method creates the VL data file and adjusts the
	 * layout accordingly.
	 * <p>
	 * To run properly the FL data file is not allowed to be already open at the
	 * time this method is invoked.
	 * <p>
	 * Note that this store and hence the whole database won't work properly
	 * anymore after this method has been executed.
	 * The database should be closed as soon as possible.
	 * <p>
	 * Note also that the format of the FL data file may be corrupted if this
	 * method throws an exception.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1},
	 * {@linkplain WRStore.GlobalBuffer GB2},
	 * {@linkplain WRStore.GlobalBuffer GB3}.
	 * 
	 * @param  store The WR store, not allowed to be {@code null}.
	 * @param  col The column to be inserted, not allowed to be {@code null}.
	 * @param  index The index within the table definition where the column is
	 *         to be inserted, must satisfy 0 &le; {@code index} &le; {@code n},
	 *         where {@code n} denotes the number of columns in the table.
	 * @param  initialValue The initial value, must be {@linkplain
	 *         acdp.types.Type#isCompatible compatible} with the type of the
	 *         column.
	 *         This value must be {@code null} if the column is a reference
	 *         column.
	 * @param  installRefCount Indicates whether the reference counter must be
	 *         installed or not.
	 *         The value must be equal to {@code true} if and only if the FL data
	 *         blocks do not contain a reference counter and the reference
	 *         counter must be installed.
	 * @param  layoutDir The directory of the layout, not allowed to be {@code
	 *         null}.
	 * 
	 * @throws NullPointerException If {@code store} or {@code col} is {@code
	 *         null} or if {@code col} is not a column of the store's table or
	 *         if {@code layoutDir} is {@code null} when it's needed.
	 * @throws IllegalArgumentException If the length of the byte representation
	 *         of the initial value (or one of the elements if the initial value
	 *         is an array value) exceeds the maximum length allowed by the
	 *         simple column type.
	 * @throws MaximumException If a new memory block in the VL file space must
	 *         be allocated and its file position exceeds the maximum allowed
	 *         position
	 * @throws CryptoException If encryption fails.
	 *         This exception never happens if the database does not apply
	 *         encryption.
	 * @throws ImplementationRestrictionException If the Null-info must be
	 *         expanded by a single bit but the bitmap is too large for being
	 *         expanded or if the new size of the FL data blocks exceeds {@code
	 *         Integer.MAX_VALUE}.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void insertCol(WRStore store, Column_<?> col, int index,
			Object initialValue, boolean installRefCount, Path layoutDir) throws
									NullPointerException, IllegalArgumentException,
									MaximumException, CryptoException, 
									ImplementationRestrictionException, FileIOException {
		// Create VL data file and adjust the layout accordingly if store has no
		// VL file space yet and column is VL column.
		installVLFileSpace(col, layoutDir);
		
		// Insert the column and, depending on the installRefCount argument, the
		// reference counter.
		new ColInsert().run(this, col, index, initialValue, (installRefCount ?
																			NOBS_REF_COUNT : 0));
		// Adjust the layout if reference counter must be installed.
		if (installRefCount) {
			layout.add(lt_nobsRefCount, String.valueOf(NOBS_REF_COUNT));
		}
	}
	
	/**
	 * Removes the VL file space if the specified column to be removed or to
	 * be modified is a VL column and if after the removal of that column
	 * or if after the modification of that column the table has no VL column
	 * left.
	 * 
	 * @param  col0 The column to be removed or to be modified, not allowed
	 *         to be {@code null}.
	 * @param  col1 The column replacing the column to be modified, may be
	 *         {@code null}.
	 * 
	 * @throws NullPointerException If {@code col0} is {@code null}.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void removeVLFileSpace(Column_<?> col0, Column_<?> col1) throws
													NullPointerException, FileIOException {
		if (isVLCol(col0)) {
			// Compute the number of VL columns.
			int m = 0;
			for (WRColInfo ci : colInfoArr) {
				if (isVLCol(ci.col)) {
					m++;
				}
			}
			if (m == 1 && (col1 == null || !isVLCol(col1))) {
				// The column to be removed or modified is a singular VL column.
				vlDataFile.delete();
				layout.remove(lt_vlDataFile);
				layout.remove(lt_nobsOutrowPtr);
			}
		}
	}
	
	/**
	 * Removes the specified column from this store.
	 * <p>
	 * If {@code removeRefCount} is equal to {@code true} then this method not
	 * only removes the specified column but the reference counter as well and
	 * adjusts the layout accordingly.
	 * <p>
	 * If the specified column is a VL column and it is the only VL column of
	 * the table then this method deletes the VL data file and adjusts the
	 * layout accordingly.
	 * <p>
	 * To run properly the FL data file is not allowed to be already open at the
	 * time this method is invoked.
	 * <p>
	 * Note that this store and hence the whole database won't work properly
	 * anymore after this method has been executed.
	 * The database should be closed as soon as possible.
	 * <p>
	 * Note also that the format of the FL data file may be corrupted if this
	 * method throws an exception.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1},
	 * {@linkplain WRStore.GlobalBuffer GB2},
	 * {@linkplain WRStore.GlobalBuffer GB3}.
	 * 
	 * @param  col The column to be removed, not allowed to be {@code null}.
	 *         The column must be a column of the store's table.
	 * @param  removeRefCount Indicates whether the reference counter must be
	 *         removed or not.
	 *         The value must be equal to {@code true} if and only if the FL data
	 *         blocks contain a reference counter and the reference counter must
	 *         be removed.
	 * 
	 * @throws NullPointerException If {@code col} is {@code null} or if {@code
	 *         col} is not a column of the store's table.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void removeCol(Column_<?> col, boolean removeRefCount) throws
													NullPointerException, FileIOException {
		// Remove the column and, depending on the removeRefCount argument, the
		// reference counter.
		new ColRemove().run(this, col, removeRefCount);
		
		// Adjust the layout if reference counter must be removed.
		if (removeRefCount) {
			layout.remove(lt_nobsRefCount);
		}
		
		// Delete VL data file and adjust the layout accordingly if col is a
		// singular VL column.
		removeVLFileSpace(col, null);
	}
	
	/**
	 * Modifies the specified column of this store.
	 * <p>
	 * This method makes the following assumptions:
	 * 
	 * <ol>
	 *    <li>The table is not empty.</li>
	 *    <li>The type descriptors of the specified columns are different.</li>
	 *    <li>The value changer is not {@code null} or if it is {@code null} then
	 *        the modification is not a trivial one (see below).</li>
	 *    <li>If the value changer is not {@code null} then none of the column
	 *        types is A[RT].</li>
	 *    <li>If the value changer is {@code null} then both column types are ST
	 *        having the same value types OR both column types are A[RT] OR both
	 *        column types are A[ST] with ST having the same values type.</li>
	 * </ol>
	 * <p>
	 * If the value changer is {@code null} then a trivial modification is a
	 * modification that does not require the accommodation of the FL data file.
	 * <p>
	 * If the specified column replacing the column to be modified is a VL
	 * column and it is the only VL column of the table then this method
	 * creates the VL data file and adjusts the layout accordingly.
	 * <p>
	 * If the column replacing the column to be modified is not a VL column and
	 * the specified column to be modified is a VL column and it is the only VL
	 * column of the table then this method deletes the VL data file and adjusts
	 * the layout accordingly.
	 * <p>
	 * To run properly the FL data file is not allowed to be already open at the
	 * time this method is invoked.
	 * <p>
	 * Note that this store and hence the whole database won't work properly
	 * anymore after this method has been executed.
	 * The database should be closed as soon as possible.
	 * <p>
	 * Note also that the format of the FL data file may be corrupted if this
	 * method throws an exception.
	 * <p>
	 * The value resulting from applying the specified value changer must be
	 * {@linkplain Type#isCompatible compatible} with the type of the specified
	 * column replacing the column to be modified.
	 * This method does not explicitly check this precondition in all situations.
	 * In any case, if this precondition is not satisfied then this method
	 * throws an exception, however, this may be an exception of a type not
	 * listed below.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1},
	 * {@linkplain WRStore.GlobalBuffer GB2},
	 * {@linkplain WRStore.GlobalBuffer GB3}.
	 * 
	 * @param  <T> The type of the column's values.
	 * 
	 * @param  col0 The column to be modified, not allowed to be {@code null}.
	 * @param  col1 The column replacing the column to be modified, not allowed
	 *         to be {@code null}.
	 * @param  valueChanger The value changer, may be {@code null}.
	 * @param  layoutDir The directory of the layout, not allowed to be {@code
	 *         null}.
	 * 
	 * @throws NullPointerException If one of the arguments not allowed to be
	 *         {@code null} is {@code null} or if a value of a simple column
	 *         type is set equal to {@code null} but the column type forbids the
	 *         {@code null} value or if the value is an array value and this
	 *         condition is satisfied for at least one element contained in the
	 *         array.
	 * @throws IllegalArgumentException If the length of the byte representation
	 *         of a value (or one of the elements if the value is an array
	 *         value) exceeds the maximum length allowed by the simple column
	 *         type.
	 *         This exception also happens if the value is an array value and
	 *         the size of the array exceeds the maximum length allowed by
	 *         the array column type.
	 * @throws MaximumException If a new memory block in the VL file space
	 *         must be allocated and its file position exceeds the maximum
	 *         allowed position.
	 * @throws CryptoException If encryption or decryption fails.
	 *         This exception never happens if the WR database does not apply
	 *         encryption or if the column type is an A[RT].
	 * @throws ImplementationRestrictionException If the Null-info must be
	 *         expanded by a single bit but the bitmap is too large for being
	 *         expanded <em>or</em> if the byte representation stored in the VL
	 *         file space is too large to fit into the inrow column section
	 *         <em>or</em> if the number of bytes needed to persist a value of
	 *         an array type exceeds Java's maximum array size.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final <T> void modifyCol(Column_<?> col0, Column_<T> col1,
									ValueChanger<T> valueChanger, Path layoutDir) throws
									NullPointerException, IllegalArgumentException,
									MaximumException, CryptoException,
									ImplementationRestrictionException, FileIOException {
		// Create VL data file and adjust the layout accordingly if store has no
		// VL file space yet and new column is VL column.
		installVLFileSpace(col1, layoutDir);
		
		// Modify the column.
		new ColModify().run(this, col0, col1, valueChanger);
		
		// Delete VL data file and adjust the layout accordingly if col is a
		// singular VL column.
		removeVLFileSpace(col0, col1);
	}
	
	/**
	 * Checks if the specified value, which is supposed to be the new value for
	 * the {@code nobsRowRef} property, is legal.
	 * 
	 * @param  value The proposed new value for the {@code nobsRowRef} property.
	 * 
	 * @return The boolean value {@code true} if and only if the new value for
	 *         the {@code nobsRowRef} property differs from the old one.
	 * 
	 * @throws IllegalArgumentException If the specified value is less than 1 or
	 *         greater than 8 or if the specified value is too small with respect
	 *         to the number of rows and row gaps in the store.
	 */
	public final boolean checkNobsRowRef(int value) throws
																	IllegalArgumentException {
		if (value == nobsRowRef) {
			return false;
		}
		// value != nobsRowRef
		if (value < 1 || 8 < value) {
			throw new IllegalArgumentException(ACDPException.prefix(table) +
							"Wrong integer value for \"nobsRowRef\" parameter: " +
																					value + ".");
		}
		// 0 < value <= 8
		
		final long nBlocks = flFileSpace.nofBlocks();
		final long nGaps = flFileSpace.nofGaps();
		final long maxBlocks = Utils_.bnd8[value];
		
		if (nBlocks > maxBlocks) {
			String msg = "New value for \"nobsRowRef\" allows " + maxBlocks +
							" FL memory blocks at most, but FL file space contains " +
							nBlocks + " memory blocks of which " + nGaps +
							" are deallocated FL memory blocks.";
			if (nBlocks - nGaps <= maxBlocks) {
				msg += " You may want to compact the FL file space.";
			}
			throw new IllegalArgumentException(ACDPException.prefix(table) + msg);
		}
		return true;
	}
	
	/**
	 * Changes all columns of this store referencing the specified store with
	 * respect of the specified new value of the {@code nobsRowRef} parameter
	 * of {@code refdStore}.
	 * <p>
	 * This method has no effect if the specified new value of the {@code
	 * nobsRowRef} parameter is identical to the current value.
	 * <p>
	 * To run properly the FL data file is not allowed to be already open at the
	 * time this method is invoked.
	 * <p>
	 * Note that the FL file space and hence the whole database won't work
	 * properly anymore after this method has been executed.
	 * The database should be closed as soon as possible.
	 * <p>
	 * Note also that the format of the FL data file may be corrupted if this
	 * method throws an exception.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1},
	 * {@linkplain WRStore.GlobalBuffer GB2},
	 * {@linkplain WRStore.GlobalBuffer GB3}.
	 * 
	 * @param  refdStore The store referenced by at least one of the columns
	 *         of this store, not allowed to be {@code null}.
	 * @param  newNobsRowRef The new value of the {@code nobsRowRef} parameter
	 *         of {@code refdStore}, must pass the {@code checkNobsRowRef}
	 *         method.
	 * 
	 * @throws NullPointerException If {@code refdStore} is {@code null}.
	 * @throws MaximumException If a new memory block in the VL file space
	 *         must be allocated and its file position exceeds the maximum
	 *         allowed position.
	 * @throws ImplementationRestrictionException If the number of bytes needed
	 *         to persist an array of references in an inrow column exceeds
	 *         Java's maximum array size.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void changeRowRefLen(WRStore refdStore,
																		int newNobsRowRef) throws
									NullPointerException, MaximumException,
									ImplementationRestrictionException, FileIOException {
		if (newNobsRowRef != refdStore.nobsRowRef) {
			new ChangeRefLen().run(this, refdStore, newNobsRowRef);
		}
	}
	
	/**
	 * Changes the {@code nobsRowRef} field in the store layout.
	 * 
	 * @param value The new value of the {@code nobsRowRef} table parameter.
	 */
	public final void changeNobsRowRefInLayout(int value) {
		layout.replace(lt_nobsRowRef, String.valueOf(value));
	}
	
	/**
	 * Checks if the specified value, which is supposed to be the new value for
	 * the {@code nobsOutrowPtr} property, is legal.
	 * 
	 * @param  value The proposed new value for the {@code nobsOutrowPtr}
	 *         property.
	 *         
	 * @return The boolean value {@code true} if and only if the new value for
	 *         the {@code nobsOutrowPtr} property differs from the old one.
	 * 
	 * @throws IllegalArgumentException If the store has no VL file space or if
	 *         the specified value is less than 1 or greater than 8 or if the
	 *         specified value is too small with respect to the size of the VL
	 *         file space.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final boolean checkNobsOutrowPtr(int value) throws
												IllegalArgumentException, FileIOException {
		if (value == nobsOutrowPtr) {
			return false;
		}
		// value != nobsOutrowPtr
		if (value < 1 || 8 < value) {
			throw new IllegalArgumentException(ACDPException.prefix(table) +
							"Wrong integer value for \"nobsOutrowPtr\" parameter: " +
																					value + ".");
		}
		// 0 < value <= 8
		if (nobsOutrowPtr == 0) 
			throw new IllegalArgumentException(ACDPException.prefix(table) +
															"The table has no VL file space.");
		else {
			final long maxSize = Utils_.bnd8[value];
			final long size;
			try {
				size = Files.size(vlDataFile.path);
			} catch (IOException e) {
				throw new FileIOException(vlDataFile.path, e);
			}
			if (size >= maxSize) {
				throw new IllegalArgumentException(ACDPException.prefix(table) +
							"New value for \"nobsOutrowPtr\" allows a maximum " +
							"size of the VL data file of " + maxSize + " bytes, but " +
							"VL data file has a size of " + size + " bytes.");
			}
		}
		return true;
	}
	
	/**
	 * Changes all VL columns of this store with respect to the specified new
	 * value of the {@code nobsOutrowPtr} parameter.
	 * <p>
	 * This method has no effect if the specified new value of the {@code
	 * nobsOutrowPtr} parameter is identical to the current value.
	 * <p>
	 * To run properly the FL data file is not allowed to be already open at the
	 * time this method is invoked.
	 * <p>
	 * Note that the FL file space and hence the whole database won't work
	 * properly anymore after this method has been executed.
	 * The database should be closed as soon as possible.
	 * <p>
	 * Note also that the format of the FL data file may be corrupted if this
	 * method throws an exception.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1}.
	 * 
	 * @param  newNobsOutrowPtr The new value of the {@code nobsOutrowPtr}
	 *         parameter, must pass the {@code checkNobsOutrowPtr} method.
	 * 
	 * @throws ImplementationRestrictionException If the new size of the FL data
	 *         blocks exceeds {@code Integer.MAX_VALUE}.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void changeOutrowPtrLen(int newNobsOutrowPtr) throws
									NullPointerException, MaximumException,
									ImplementationRestrictionException, FileIOException {
		if (newNobsOutrowPtr != nobsOutrowPtr) {
			final int cLen = newNobsOutrowPtr - nobsOutrowPtr;
			final Spec spec = FLFileAccommodate.newSpec();
			for (WRColInfo ci : colInfoArr) {
				if (isVLCol(ci.col)) {
					spec.spot(ci.offset + ci.lengthLen, cLen);
				}
			}
			// Specification has at least one spot by assumption.
			spec.run(this);
		}
	}
	
	/**
	 * Changes the {@code nobsOutrowPtr} field in the store layout.
	 * 
	 * @param value The new value of the {@code nobsOutrowPtr} table parameter.
	 */
	public final void changeNobsOutrowPtrInLayout(int value) {
		layout.replace(lt_nobsOutrowPtr, String.valueOf(value));
	}
	
	/**
	 * Checks if the specified value, which is supposed to be the new value for
	 * the {@code nobsRefCount} property, is legal.
	 * 
	 * @param  value The proposed new value for the {@code nobsRefCount}
	 *         property.
	 *         
	 * @return The boolean value {@code true} if and only if the new value for
	 *         the {@code nobsRefCount} property differs from the old one.
	 * 
	 * @throws IllegalArgumentException If the store is not referenced by any
	 *         store of the database or if the specified value is less than 1 or
	 *         greater than 8 or if the specified value is too small with respect
	 *         to the row having the highest value of the reference counter.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	public final boolean checkNobsRefCount(int value) throws
											IllegalArgumentException, IOFailureException {
		if (value == nobsRefCount) {
			return false;
		}
		// value != nobsRefCount
		if (value < 1 || 8 < value) {
			throw new IllegalArgumentException(ACDPException.prefix(table) +
							"Wrong integer value for \"nobsRefCount\" parameter: " +
																					value + ".");
		}
		// 0 < value <= 8
		if (nobsRefCount == 0) 
			throw new IllegalArgumentException(ACDPException.prefix(table) +
						"The table is not referenced by any table of the database.");
		else {
			final long maxRefCount = Utils_.bnd8[value];
			final long highest = new Info(this).highestRefCount();
			if (highest > maxRefCount) {
				throw new IllegalArgumentException(ACDPException.prefix(table) +
						"New value for \"nobsRefCount\" restricts the maximum " +
						"value of the reference counter to " + maxRefCount +
						", but table has a row with a reference counter equal to " +
						highest + ".");
			}
		}
		return true;
	}
	
	/**
	 * Changes the reference counter of this store with respect to the
	 * specified new value of the {@code nobsRefCount} parameter.
	 * <p>
	 * This method has no effect if the specified new value of the {@code
	 * nobsRefCount} parameter is identical to the current value.
	 * <p>
	 * To run properly the FL data file is not allowed to be already open at the
	 * time this method is invoked.
	 * <p>
	 * Note that the FL file space and hence the whole database won't work
	 * properly anymore after this method has been executed.
	 * The database should be closed as soon as possible.
	 * <p>
	 * Note also that the format of the FL data file may be corrupted if this
	 * method throws an exception.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1}.
	 * 
	 * @param  newNobsRefCount The new value of the {@code nobsRefCount}
	 *         parameter, must pass the {@code checkNobsRefCount} method.
	 * 
	 * @throws ImplementationRestrictionException If the new size of the FL data
	 *         blocks exceeds {@code Integer.MAX_VALUE}.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void changeRefCountLen(int newNobsRefCount) throws
									NullPointerException, MaximumException,
									ImplementationRestrictionException, FileIOException {
		if (newNobsRefCount != nobsRefCount) {
			FLFileAccommodate.spot(nBM, newNobsRefCount - nobsRefCount).run(this);
		}
	}
	
	/**
	 * Changes the {@code nobsRefCount} field in the store layout.
	 * 
	 * @param value The new value of the {@code nobsRefCount} table parameter.
	 */
	public final void changeNobsRefCountInLayout(int value) {
		layout.replace(lt_nobsRefCount, String.valueOf(value));
	}
	
	/**
	 * Runs a series of tests to find out whether the integrity of the table
	 * data contained in this store is harmed or not, and, provided that the
	 * {@code fix} argument is set to {@code true}, this method makes an attempt
	 * to fix any detected violations, although not all types of integrity
	 * violations can be automatically fixed.
	 * <p>
	 * Note that this method never throws an exception.
	 * Exceptions and any detected violations are reported with the help of
	 * the specified reporter.
	 * <p>
	 * Invoke this method within a session created solely for the purpose of
	 * verifying the integrity of the table data.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1}.
	 * 
	 * @param  fix The information whether an attempt should be made to fix any
	 *         detected violations of the integrity of the table data.
	 * @param  rp The reporter, not allowed to be {@code null}.
	 * @param  gapsCache The cache for the gaps arrays of the stores.
	 *         The map must contain an entry for each store of the database.
	 *         
	 * @return The boolean value {@code true} if and only if all tests
	 *         successfully terminated, that is, none of the tests was aborted
	 *         and no integrity violation was detected or the {@code fix}
	 *         argument is set to {@code true} and all detected integrity
	 *         violations could be fixed.
	 */
	public final boolean verifyIntegrity(boolean fix, Reporter rp,
															Map<WRStore, long[]> gapsCache) {
		return new VerifyIntegrity().run(this, fix, rp, gapsCache);
	}
}