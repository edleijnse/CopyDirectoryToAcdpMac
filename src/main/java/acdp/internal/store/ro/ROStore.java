/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.ro;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

import acdp.ColVal;
import acdp.Information.ROStoreInfo;
import acdp.Table.ValueChanger;
import acdp.Table.ValueSupplier;
import acdp.design.SimpleType;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CreationException;
import acdp.exceptions.IllegalReferenceException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.internal.Column_;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.Ref_;
import acdp.internal.Table_;
import acdp.internal.misc.FileChannelProvider;
import acdp.internal.misc.Utils_;
import acdp.internal.store.Store;
import acdp.internal.types.AbstractArrayType;
import acdp.internal.types.ArrayOfRefType_;
import acdp.internal.types.ArrayType_;
import acdp.internal.types.RefType_;
import acdp.internal.types.Type_;
import acdp.misc.Layout;
import acdp.misc.Utils;
import acdp.types.Type.Scheme;

/**
 * Defines a concrete store which supports the readable access to the table's
 * data saved in a single file called the <em>database file</em>.
 * The table's data is saved in a <em>packed</em> format which means that the
 * data is compressed and optionally encrypted.
 * This store is a read-only store, hence, it does not support the writing of
 * table data.
 * <p>
 * The table data starts at a certain position within the database file.
 * All positions are relative to this starting position.
 * <p>
 * The <em>table data</em> consists of the <em>row data</em>, the
 * <em>row pointers</em> and the <em>sizes of the data blocks</em>.
 * A row pointer is a position within the table data where a particular row
 * starts.
 * Row pointers are needed because the length of the row data varies from row
 * to row.
 * The row data is packed into a sequence of data blocks.
 * The size of a completely filled <em>unpacked</em> data block is equal to the
 * value of the {@code regularBlockSize} constant.
 * The sizes of the <em>packed</em> data blocks, however, vary from block to
 * block.
 * <p>
 * The data of each row in the table is stored row after row in the row data
 * section of the table data.
 * Each row in turn consists of a fixed length <em>row header</em> followed by
 * the <em>row body</em> which is of variable length.
 * The row body is further subdivided into sections of different lengths
 * housing the <em>column data</em> which is equal to the byte representation
 * of a value of a particular column.
 * The sequence of column data is sorted according to the order defined by the
 * <em>table definition</em>.
 * <p>
 * It follows a description of the row header.
 * <p>
 * Each row header has the same length of {@code nH} bytes.
 * This sequence of bytes is subdivided into a sequence of {@code nNI} bytes,
 * called the <em>null information</em> followed by a sequence of {@code nH -
 * nNI} bytes, called the <em>length information</em>, both explained below.
 * <p>
 * Let us view the byte array of the row header from left to right, each byte
 * consisting of 8 bits, like this:
 * 
 * <pre>
 * 0 1 2 3 4 5 6 7  0 1 2 3 4 5 6 7  ... 0 1 2 3 4 5 6 7
 * |  first Byte |  | second Byte |      |  nHth Byte  |</pre>
 * 
 * Then the individual bits and bytes of the row header have the following
 * meaning:
 * <ul>
 *    <li>The first {@code m} bits are not used.
 *        With {@code p} explained in the next paragraph, the exact value of
 *        {@code m} is such that {@code m} is the smallest integer for which
 *        {@code m} + {@code p} is divisible by 8 without a remainder, hence,
 *        {@code m} varies from 0 to 7.
 *        (In practice, {@code m} + {@code p} will be less than or equal to
 *        64 since {@code p} does not exceed 64, hence, {@code nNI} =
 *        ({@code m} + {@code p}) / 8 is never greater than 8.)</li>
 *    <li>Next come {@code p} bits where {@code p} is the number of
 *        <em>nullable</em> columns.
 *        (A <em>nullable</em> column is a column that allows the NULL value.)
 *        These {@code p} bits, called the <em>null information</em>, indicate
 *        if a particular nullable column has a value equal to NULL.</li>
 *    <li>It follows the <em>length information</em>, explained below, which is
 *        made of the following {@code nH - nNI} bytes.</li>
 * </ul>
 * <p>
 * The length of a byte representation of a value is either fix or variable
 * depending on the type of the column.
 * The length is fix if the type of the column is an INROW ST or an RT.
 * For any column of another column type the length of the byte representation
 * of a value is variable and saved in the <em>length information</em> of the
 * row header, column after column according to the order defined by the table
 * definition.
 *
 * @author Beat Hoermann
 */
public final class ROStore extends Store {
	// Keys of layout entries.
	private static final String lt_nofRows = "nofRows";
	private static final String lt_startData = "startData";
	private static final String lt_dataLength = "dataLength";
	private static final String lt_startRowPtrs = "startRowPtrs";
	private static final String lt_nobsRowPtr = "nobsRowPtr";
	private static final String lt_nofBlocks = "nofBlocks";
	
	/**
	 * Creates an RO store layout filled with the specified values.
	 * 
	 * @param  nofRows The number of rows in the table.
	 * @param  startData The position within the database file where the table
	 *         data starts.
	 * @param  dataLength The number of bytes of the unpacked table data.
	 * @param  startRowPtrs The position within the database file where the row
	 *         pointers of the table start.
	 * @param  nobsRowPtr The number of bytes of the byte representation of a
	 *         row pointer.
	 * @param  nofBlocks The number of data blocks the table data consists of.
	 * 
	 * @return The RO store layout filled with the specified values.
	 */
	public static final Layout createLayout(int nofRows, long startData,
				long dataLength, long startRowPtrs, int nobsRowPtr, int nofBlocks) {
		final Layout layout = new Layout();
		
		layout.add(lt_nofRows, String.valueOf(nofRows));
		layout.add(lt_startData, String.valueOf(startData));
		layout.add(lt_dataLength, String.valueOf(dataLength));
		layout.add(lt_startRowPtrs, String.valueOf(startRowPtrs));
		layout.add(lt_nobsRowPtr, String.valueOf(nobsRowPtr));
		layout.add(lt_nofBlocks, String.valueOf(nofBlocks));
		
		return layout;
	}
	
	/**
	 * The regular size of an unpacked block of table data.
	 * (All blocks have this size with one exception: The last block may be
	 * smaller.)
	 */
	public static final int regularBlockSize = 65535; // 65535 == Utils.bnd4[2]
	/**
	 * The number of bytes used to encode the size of a table data block, for
	 * instance, 2 if {@code regularBlockSize} is equal to 65535.
	 */
	public static final int nobsBlockSize = Utils.lor(regularBlockSize);
	
	/**
	 * Just implements the {@code ROStoreInfo} interface.
	 * 
	 * @author Beat Hoermann
	 */
	public static final class Info implements ROStoreInfo {
		private final ROStore store;
		
		/**
		 * Creates the information object of the specified RO store.
		 * 
		 * @param  roStore The RO store, not allowed to be {@code null}.
		 * 
		 * @throws NullPointerException If {@code store} is {@code null}.
		 */
		public Info(ROStore roStore) throws NullPointerException {
			this.store = Objects.requireNonNull(roStore);
		}
		
		@Override
		public final int nofRows() {
			return store.nofRows;
		}

		@Override
		public final long startData() {
			return store.startData;
		}

		@Override
		public final long dataLength() {
			return store.conf.dataLength;
		}

		@Override
		public final long startRowPtrs() {
			return store.conf.startRowPtrs;
		}

		@Override
		public final int nobsRowPtr() {
			return store.nobsRowPtr;
		}

		@Override
		public final int nofBlocks() {
			return store.conf.nofBlocks;
		}
	}
	
	/**
	 * Wraps a {@link Column_} object and keeps additional information about that
	 * column.
	 * 
	 * @author Beat Hoermann
	 */
	public static final class ROColInfo {
		/**
		 * The column, never {@code null}.
		 */
		final Column_<?> col;
		
		/**
		 * Used for coding and decoding the null information if the value in this
		 * column is allowed to be {@code null}.
		 * The value is equal to zero if and only if the column is a non-nullable
		 * ST or an RT column.
		 */
		public long nullBitMask;
		
		/**
		 * If the column is an INROW ST or an RT column then this is the length
		 * of the column, otherwise, this value is equal to -1.
		 */
		public int len;
		
		/**
		 * If the column is not an INROW ST and not an RT column then this is the
		 * index where the length information of this column starts within the
		 * array of bytes representing the row header.
		 * If the column is an INROW ST or an RT column then {@code offset} is
		 * not defined.
		 */
		int offset;
		
		/**
		 * If the column is not of an INROW ST and not an RT column then this is
		 * the number of bytes needed to save the length of the byte
		 * representation of a value of the column.
		 * This value is greater than 0 and less than or equal to 4.
		 * (In a WR database a value greater than 4 is possible.)
		 * If the column is an INROW ST or an RT column then {@code lengthLen} is
		 * not defined. 
		 */
		public int lengthLen;
		
		/**
		 * If the column is an RT or A[RT] column then this is the number of
		 * bytes required for referencing a row in the referenced table.
		 * If the column is of a different type then {@code nobsRowRef} is not
		 * defined.
		 */
		public int nobsRowRef;
		
		/**
		 * If the column is an array column then this is the number of bytes
		 * needed to save the number of elements of the array value.
		 * In all other cases {@code sizeLen} is not defined.
		 */
		int sizeLen;
		
		/**
		 * The constructor.
		 * 
		 * @param col The column, not allowed to be {@code null}.
		 */
		ROColInfo(Column_<?> col) {
			this.col = col;
		}
		
		@Override
		public final int hashCode() {
			return col.name().hashCode();
		}
		
		@Override
		public final boolean equals(Object obj) {
			if (obj instanceof ROColInfo)
				return this.col.equals(((ROColInfo) obj).col);
			else {
				return false;
			}
		}
	}
	
	/**
	 * Initializes the {@code nullBitMask} field of the column information and
	 * returns the number of bytes used by the null information.
	 * 
	 * @param  colInfoArr The column informations.
	 * @param  table The table.
	 * 
	 * @return The number of bytes used by the null information, always greater
	 *         than or equal to zero and less than or equal to 8.
	 *         
	 * @throws ImplementationRestrictionException If the table has too many
	 *         columns.
	 */
	private static final int initNullBitMask(ROColInfo[] colInfoArr,
							Table_ table) throws ImplementationRestrictionException {
		long mask = 1L;
		int n = 0;
		for (ROColInfo ci : colInfoArr) {
			Type_ type = ci.col.type();
			
			// Initialize "nullBitMask".
			if (type instanceof SimpleType && !((SimpleType<?>) type).nullable() ||
																		type instanceof RefType_)
				ci.nullBitMask = 0;
			else {
				ci.nullBitMask = mask;
				mask <<= 1;
				n++;
			}
		}
		
		if (n > 64) {
			throw new ImplementationRestrictionException(table,
																"Table has too many columns.");
		}
		
		return Utils.bmLength(n);
	}
	
	/**
	 * Initializes the {@code len}, the {@code offset}, the {@code lengthLen},
	 * the {@code nobsRowRef} and the {@code sizeLen} fields of the column
	 * information and returns the number of bytes of the row header.
	 * <p>
	 * This method assumes that all tables of the database are created and
	 * initialized.
	 * 
	 * @param  colInfoArr The column informations.
	 * @param  nNI The number of bytes used by the null information.
	 * @param  table The table.
	 * 
	 * @return The number of bytes of the row header.
	 */
	private static final int initColInfo(ROColInfo[] colInfoArr, int nNI,
																					Table_ table) {
		int offset = nNI;
		for (ROColInfo ci : colInfoArr) {
			Type_ type = ci.col.type();
			
			// Initialize "nobsRowRef".
			if (type instanceof RefType_ || type instanceof ArrayOfRefType_) {
				// Column referencing a table.
				String refdTable = ci.col.refdTable();
				ci.nobsRowRef = Utils.lor(table.db().getTable(refdTable).
																					numberOfRows());
			}
			
			// Initialize "len", "offset", "lengthLen", "sizeLen".
			if (type.scheme() == Scheme.INROW && type instanceof SimpleType)
				ci.len = ((SimpleType<?>) type).length();
			else if (type instanceof RefType_)
				ci.len = ci.nobsRowRef;
			else {
				// Not INROW SimpleType and not RefType
				ci.len = -1;
				ci.offset = offset;
				
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
						// type instanceof ArrayOfRefType
						maxLength = (long) maxSize * ci.nobsRowRef;
					}
					ci.lengthLen = Utils.lor(ci.sizeLen + maxLength);
				}
				
				offset += ci.lengthLen;
			}
		}
		return offset;
	}
	
	/**
	 * Computes some basic parameters of an RO store from a given WR table.
	 * This class is used in the process of converting a WR table into an RO
	 * table.
	 * 
	 * @author Beat Hoermann
	 */
	public static final class ROStoreParams {
		/**
		 * See {@link ROStore#nH}.
		 */
		public final int nH;
		/**
		 * See {@link ROStore#nNI}.
		 */
		public final int nNI;
		/**
		 * See {@link ROStore#colInfoArr}.
		 */
		public final ROColInfo[] colInfoArr;
		
		/**
		 * Computes some basic parameters of an RO store from the specified
		 * WR table.
		 * 
		 * @param  table The WR table, not allowed to be {@code null}.
		 * 
		 * @throws NullPointerException If {@code table} is {@code null}.
		 * @throws ImplementationRestrictionException If the table has too many
		 *         columns.
		 */
		public ROStoreParams(Table_ table) throws NullPointerException,
														ImplementationRestrictionException {
			final Column_<?>[] tableDef = table.tableDef();
			colInfoArr = new ROColInfo[tableDef.length];
			for (int i = 0; i < tableDef.length; i++) {
				colInfoArr[i] = new ROColInfo(tableDef[i]);
			}
			
			nNI = initNullBitMask(colInfoArr, table);
			nH = initColInfo(colInfoArr, nNI, table);
		}
	}
	
	/**
	 * Maps a column to its column information.
	 */
	final Map<Column_<?>, ROColInfo> colInfoMap;
	
	/**
	 * The column informations.
	 * The order and length of the array is in strict accordance with the table
	 * definition.
	 */
	final ROColInfo[] colInfoArr;
	
	/**
	 * The values from the store layout.
	 */
	private final LayoutValues conf;
	/**
	 * The database file or {@code null} if and only if the operating mode is
	 * equal to -3 or -2.
	 */
	final FileIO dbFile;
	/**
	 * The sizes of the packed table data blocks or {@code null} if and only if
	 * the operating mode is equal to -3.
	 */
	final byte[] blockSizes;
	/**
	 * The table data or {@code null} if and only if the operating mode is
	 * different from -3 and -2.
	 * For an operating mode equal to -2 the size of this byte array is
	 * typically substantially smaller than for an operating mode equal to -3.
	 * The size of the table data may be zero.
	 */
	final byte[] tableData;
	/**
	 * The file position where the table data starts within the database file.
	 * Should not be used if the operating mode is either -3 or -2.
	 */
	final long startData;
	/**
	 * For a row with index {@code i} <code>(0 &le; i &lt;
	 * rowPointers.length/nobsRowPtr)</code> the value returned by
	 * {@code Utils.unsFromBytes(rowPointers, i*nobsRowPtr, nobsRowPtr)} is equal
	 * to the relative file position or the absolute array index where a row
	 * starts within the unpacked database file or the unpacked table data,
	 * respectively.
	 */
	final byte[] rowPointers;
	/**
	 * The number of bytes of the byte representation of a row pointer within
	 * {@code rowPointers}.
	 */
	final int nobsRowPtr;
	/**
	 * The numer of rows in the table.
	 */
	final int nofRows;

	/**
	 * The number of bytes used by the null information, greater than or equal to
	 * zero and less than or equal to 8.
	 * (This value is equal to the number of header bytes {@code nH} minus the
	 * number of bytes used by the length information.)
	 * <p>
	 * Consult the class description to learn about the null information and the
	 * length information of a row.
	 */
	final int nNI;
	/**
	 * The number of header bytes, greater than zero, see the class description.
	 * <p>
	 * The value can be considered final after the {@link #initialize} method
	 * has been executed.
	 */
	int nH;
	
	/**
	 * Fills the {@code colInfoMap} and the {@code colInFoArr} properties.
	 * 
	 * @param tableDef The table definition of the store's table.
	 */
	private final void fillColInfoMapArr(Column_<?>[] tableDef) {
		
		int i = 0;
		for (Column_<?> col : tableDef) {
			ROColInfo colInfo = new ROColInfo(col);
			
			colInfoMap.put(col, colInfo);
			colInfoArr[i++] = colInfo;
		}
	}
	
	/**
	 * Keeps the various values of an RO store layout together.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class LayoutValues {
		/**
		 * The number of rows in the table, &ge; 0.
		 */
		final int nofRows;
		/**
		 * The position within the database file where the table data starts,
		 * &gt; 0.
		 */
		final long startData;
		/**
		 * The number of bytes of the unpacked table data, &ge; 0.
		 */
		final long dataLength;
		/**
		 * The position within the database file where the row pointers of the
		 * table start, &gt; 0.
		 * If {@code n} denotes the number of bytes of the packed table data then
		 * {@code n == startRowPtrs - startData} and {@code n <= dataLength}.
		 */
		final long startRowPtrs;
		/**
		 * The number of bytes of a row pointer, &ge; 0.
		 */
		final int nobsRowPtr;
		/**
		 * The number of data blocks the table data consists of, &ge; 0.
		 */
		final int nofBlocks;
		
		/**
		 * Creates an object of this class, extracts and keeps together the
		 * various values from the specified store layout and tests if they are
		 * valid.
		 * 
		 * @param  layout The layout of the store.
		 * @param  opMode The operating mode of the RO database.
		 * @param  table The Table.
		 * 
		 * @throws CreationException If at least one layout value is invalid.
		 */
		LayoutValues(Layout layout, int opMode, Table_ table) throws
																				CreationException {
			try {
				nofRows = Integer.parseInt(layout.getString(lt_nofRows));
				startData = Long.parseLong(layout.getString(lt_startData));
				dataLength = Long.parseLong(layout.getString(lt_dataLength));
				startRowPtrs = Long.parseLong(layout.getString(lt_startRowPtrs));
				nobsRowPtr = Integer.parseInt(layout.getString(lt_nobsRowPtr));
				nofBlocks = Integer.parseInt(layout.getString(lt_nofBlocks));
				if (nofRows < 0)
					throw new IllegalArgumentException("Entry \"" + lt_nofRows +
									"\" is a negative integer: " + nofRows + ".");
				else if (startData <= 0)
					throw new IllegalArgumentException("Entry \"" + lt_startData +
									"\" is an integer less than or equal to zero: " +
																				startData + ".");
				else if (dataLength < 0)
					throw new IllegalArgumentException("Entry \"" + lt_dataLength +
									"\" is a negative integer: " + dataLength + ".");
				else if (startRowPtrs <= 0)
					throw new IllegalArgumentException("Entry \"" + lt_startRowPtrs +
									"\" is an integer less than or equal to zero: " +
																				startRowPtrs + ".");
				else if (nobsRowPtr < 0 || nobsRowPtr > 8)
					throw new IllegalArgumentException("Entry \"" + lt_nobsRowPtr +
									"\"  is a negative integer or greater than 8: " +
																				nobsRowPtr + ".");
				else if (nofBlocks < 0)
					throw new IllegalArgumentException("Entry \"" + lt_nofBlocks +
									"\" is a negative integer: " + nofBlocks + ".");
				else if (nofRows * nobsRowPtr < 0)
					throw new IllegalArgumentException("Too many rows for RO " +
									"format: " + nofRows + ", " + nobsRowPtr + ".");
				else if (nofBlocks * nobsBlockSize < 0)
					throw new IllegalArgumentException("Too much data for RO " +
									"format: " + nofBlocks + ", " + nobsBlockSize + ".");
				else if (startData > startRowPtrs)
					throw new IllegalArgumentException("Entry \"" + lt_startData +
									"\" is larger than entry \"" + lt_startRowPtrs +
									"\": " + startData + ", " + startRowPtrs + ".");
				else if (opMode == -3 || opMode == -2) {
					final long diff = startRowPtrs - startData;
					if (opMode == -3 && dataLength > Integer.MAX_VALUE) {
						String str ="Too much data for RO format and operating " +
																"mode -3: " + dataLength + ".";
						if (diff <= Integer.MAX_VALUE)
							str += " Use operating mode -2.";
						else {
							str += " Use operating mode different from -3 and -2.";
						}
						throw new IllegalArgumentException(str);
					}
					else if (opMode == -2 && diff > Integer.MAX_VALUE) {
						throw new IllegalArgumentException("Too much data for RO " +
										"format and operating mode -2: " + startRowPtrs +
										", " + startData + ". Use operating mode " +
										"different from -3 and -2.");
					}
				}
			} catch (Exception e) {
				throw new CreationException(table, "Invalid RO store layout.", e);
			}
		}
	}
	
	/**
	 * Reads {@code n} bytes from the specified input stream into the specified
	 * byte array, where {@code n} denotes the size of the byte array.
	 * 
	 * @param  data The byte array which keeps the read data.
	 * @param  is The input stream.
	 * 
	 * @throws NullPointerException If the value of an argument is {@code null}.
	 * @throws IOException If an I/O error occurs.
	 */
	private final void read(byte[] data, InputStream is) throws
															NullPointerException, IOException {
		int length = data.length;
		int n = is.read(data);
		while (n != -1 && n < length) {
			n += is.read(data, n, length - n);
		}
		if (n == -1) {
			throw new IOException("Unexpected end of stream.");
		}
	}
	
	/**
	 * Reads the row pointers and the block sizes from the specified database
	 * file into the specified byte arrays.
	 * 
	 * @param  pos The position where to start reading from.
	 * @param  dbPath The path of the file containing the row pointers and the
	 *         block sizes.
	 * @param  rowPointers The byte array of row pointers, not allowed to be
	 *         {@code null}.
	 * @param  blockSizes The byte array of the blocks, not allowed to be
	 *         {@code null}.
	 * 
	 * @throws NullPointerException If the value of an argument is {@code null}.
	 * @throws CreationException If an I/O error occurs.
	 */
	private final void readIndizes(long pos, Path dbPath, byte[] rowPointers,
												byte[] blockSizes) throws
												NullPointerException, CreationException {
		try (InputStream is = new FileInputStream(dbPath.toFile())) {
			is.skip(pos);
			try (InputStream zipIS = new GZIPInputStream(is)) {
				read(rowPointers, zipIS);
				read(blockSizes, zipIS);
			}
		} catch (IOException e) {
			throw new CreationException(table, new FileIOException(dbPath, e));
		}
	}
	
	/**
	 * Reads the packed (compressed and encrypted) table data as blocks from the
	 * specified database file, unpacks (decrypts and uncompresses) each block
	 * of data and returns the unpacked table data as a byte array.
	 * 
	 * @param  dbPath The path to the database file, not allowed to be {@code
	 *         null}.
	 * @param  startData The file position where the table data start within the
	 *         database file.
	 * @param  dataLength The number of bytes of the unpacked table data, must be
	 *         greater than or equal to zero and less than or equal to {@code
	 *         Integer.MAX_VALUE}.
	 * @param  blockSizes The sizes of the packed table data blocks, not allowed
	 *         to be {@code null}.
	 * 
	 * @return The unpacked table data, never {@code null}.
	 *         The length of the returned byte array is equal to the value of
	 *         the {@code dataLength} parameter.
	 * 
	 * @throws CreationException If an I/O error occurs or if a GZIP format
	 *         error occurs.
	 */
	private final byte[] readAndUnpackTableData(Path dbPath, long startData,
						long dataLength, byte[] blockSizes) throws CreationException {
		// 0 <= dataLength <= Integer.MAX_VALUE as per assumption.
		final byte[] tableData = new byte[(int) dataLength];
		
		if (dataLength > 0) {
			// The number of bytes yet to be unpacked.
			int left = (int) dataLength;
			// Read and unpack 10 blocks at once.
			final int maxLen = 10 * regularBlockSize;
			// The position where to start unpacking the table data.
			int pos = 0;
			
			try {
				// We need a file channel provider because the unpacker relies on
				// a {@code FileIO} object that is constructed with a file channel
				// provider.
				final FileChannelProvider fcp = new FileChannelProvider(-1);
				try (Unpacker unpacker = new Unpacker(table.db(), new FileIO(dbPath,
																fcp), startData, blockSizes)) {
					do {
						// left > 0
						final int len = left < maxLen ? left : maxLen;
						unpacker.unpack(pos, len, tableData, pos);
						pos += len;
						left -= len;
					} while (left > 0);
				} catch (IOException e) {
					throw new FileIOException(dbFile.path, e);
				} // unpacker.close()
				finally {
					fcp.shutdown();
				}
			} catch (Exception e) {
				throw new CreationException(table, e);
			}
		}
		
		return tableData;
	}
	
	/**
	 * Reads the packed table data.
	 * 
	 * @param  dbPath The path of the database file.
	 * @param  startData The file position where the table data start within the
	 *         database file.
	 * @param  startRowPtrs The file position where the row pointers start.
	 * 
	 * @return The packed table data, never {@code null} but may be empty.
	 *         The length of the returned byte array is equal to the difference
	 *         {@code startRowPtrs - startData}.
	 * 
	 * @throws CreationException If an I/O error occurs.
	 */
	private final byte[] readPackedTableData(Path dbPath, long startData,
											long startRowPtrs) throws CreationException {
		// startData > 0, startRowPtrs > 0, startRowPtrs >= startData,
		// startRowPtrs - startData <= Integer.MAX_VALUE as per assumption.
		final byte[] tableData = new byte[(int) (startRowPtrs - startData)];
		
		if (tableData.length > 0) {
			final ByteBuffer buf = ByteBuffer.wrap(tableData);
			try (FileIO file = new FileIO(dbPath)) {
				file.read(buf, startData);
			} catch (FileIOException e) {
				throw new CreationException(table, e);
			}
		}
		
		return tableData;
	}
	
	/**
	 * The constructor.
	 * 
	 * @param  layout The layout of the store.
	 * @param  dbPath The path of the database file.
	 * @param  opMode The operating mode of the RO database.
	 * @param  table The associated table of this new store instance.
	 * 
	 * @throws NullPointerException If {@code dbPath} or {@code table} is {@code
	 *         null}.
	 * @throws ImplementationRestrictionException If the table has too many
	 *         columns.
	 * @throws CreationException If the store can't be created due to any other
	 *         reason including problems with the layout and an I/O error while
	 *         reading the database file.
	 */
	public ROStore(Layout layout, Path dbPath, int opMode, Table_ table) throws
							NullPointerException, ImplementationRestrictionException,
																				CreationException {
		super(table);

		final Column_<?>[] tableDef = table.tableDef();
		this.colInfoMap = new HashMap<>(tableDef.length * 4 / 3 + 1);
		this.colInfoArr = new ROColInfo[tableDef.length];
		fillColInfoMapArr(tableDef);
		
		conf = new LayoutValues(layout, opMode, table);
		
		// Read row pointers and block sizes
		final byte[] rowPointers = new byte[conf.nofRows * conf.nobsRowPtr];
		final byte[] blockSizes = new byte[conf.nofBlocks * nobsBlockSize];
		if (conf.nofRows > 0) {
			// Implies rowPointer.length > 0 && blockSizes.length > 0
			readIndizes(conf.startRowPtrs, dbPath, rowPointers, blockSizes);
		}
		
		// Initialize dbFile, blockSizes and tableData.
		if (opMode == -3) {
			// MU mode: Memory Unpacked.
			this.dbFile = null;
			this.blockSizes = null;
			this.tableData = readAndUnpackTableData(dbPath, conf.startData,
																	conf.dataLength, blockSizes);
		}
		else if (opMode == -2) {
			// MP mode: Memory packed.
			this.dbFile = null;
			this.blockSizes = blockSizes;
			this.tableData = readPackedTableData(dbPath, conf.startData,
																				conf.startRowPtrs);
		}
		else {
			// FP mode: File packed.
			this.dbFile = new FileIO(dbPath, table.db().fcProvider());
			this.blockSizes = blockSizes;
			this.tableData = null;
		}
		
		this.startData = conf.startData;
		this.rowPointers = rowPointers;
		this.nobsRowPtr = conf.nobsRowPtr;
		this.nofRows = conf.nofRows;
		
		this.nNI = initNullBitMask(colInfoArr, table);
		
		// Create and set the read-only operation.
		setReadOp(new ROReadOp(this));
	}
	
	/**
	 * Initializes this store.
	 * <p>
	 * This method assumes that all tables of the database are created and
	 * initialized.
	 */
	public final void initialize() {
		nH = initColInfo(colInfoArr, nNI, table);
	}
	
	@Override
	protected final long numberOfRowGaps() {
		return 0L;
	}
	

	@Override
	public final long refToRi(Ref_ ref) throws IllegalReferenceException {
		final long ri = ref.rowIndex();
		if (ri < 1 || nofRows < ri) {
			throw new IllegalReferenceException(table, ref, false);
		}
		return ri;
	}
	
	@Override
	public final long numberOfRows() {
		return nofRows;
	}
	
	@Override
	public final Ref_ insert(Object[] values) throws
																UnsupportedOperationException {
		throw new UnsupportedOperationException(ACDPException.prefix(table) +
								"Database is read-only. Can't write to an RO store.");
	}

	@Override
	public final void delete(Ref_ ref) throws UnsupportedOperationException {
		throw new UnsupportedOperationException(ACDPException.prefix(table) +
								"Database is read-only. Can't write to an RO store.");
	}

	@Override
	public final void update(Ref_ ref, ColVal<?>[] colVals) throws
																UnsupportedOperationException {
		throw new UnsupportedOperationException(ACDPException.prefix(table) +
								"Database is read-only. Can't write to an RO store.");
	}

	@Override
	public final void updateAll(ColVal<?>[] colVals) throws
																UnsupportedOperationException {
		throw new UnsupportedOperationException(ACDPException.prefix(table) +
								"Database is read-only. Can't write to an RO store.");
	}
	
	@Override
	public final void updateAllSupplyValues(Column_<?> col,
			ValueSupplier<?> valueSupplier) throws UnsupportedOperationException {
		throw new UnsupportedOperationException(ACDPException.prefix(table) +
								"Database is read-only. Can't write to an RO store.");
	}

	@Override
	public final void updateAllChangeValues(Column_<?> col,
				ValueChanger<?> valueChanger) throws UnsupportedOperationException {
		throw new UnsupportedOperationException(ACDPException.prefix(table) +
								"Database is read-only. Can't write to an RO store.");
	}
}
