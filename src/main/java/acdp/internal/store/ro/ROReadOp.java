/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.ro;

import java.lang.AutoCloseable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import acdp.Column;
import acdp.design.SimpleType;
import acdp.exceptions.ACDPException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.ShutdownException;
import acdp.internal.Column_;
import acdp.internal.FileIOException;
import acdp.internal.Ref_;
import acdp.internal.Row_;
import acdp.internal.store.Bag;
import acdp.internal.store.ReadOp;
import acdp.internal.store.RefST;
import acdp.internal.store.ro.ROStore.ROColInfo;
import acdp.internal.types.AbstractArrayType;
import acdp.internal.types.ArrayOfRefType_;
import acdp.internal.types.ArrayType_;
import acdp.internal.types.RefType_;
import acdp.internal.types.Type_;
import acdp.misc.Utils;
import acdp.types.Type.Scheme;

/**
 * The read-only operation of an RO table.
 * <p>
 * A read-only operation of an RO table is always <em>unsynchronized</em>.
 * See the description of the {@code acdp.internal.core.SyncManager} class to
 * learn about "synchronized database operations".
 *
 * @author Beat Hoermann
 */
final class ROReadOp extends ReadOp {
	/**
	 * The table's store.
	 */
	private final ROStore store;
	/**
	 * The "memory unpacked" section loader or {@code null} if and only if the
	 * operating mode of the database is different from "memory unpacked".
	 */
	private final MURowSectionLoader muSectionLoader;

	/**
	 * Constructs the read-only operation for an RO table.
	 * 
	 * @param  store The store this read-only operation belongs to, not allowed
	 *         to be {@code null}.
	 */
	ROReadOp(ROStore store) {
		super(store);
		this.store = store;
		this.muSectionLoader = store.blockSizes == null ?
											new MURowSectionLoader(store.tableData) : null;
	}
	
	/**
	 * Creates a row loader for an RO read-only operation.
	 * 
	 * @param  cols The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of {@code table}.
	 *         If the array of columns is empty then this method behaves as if
	 *         the array of columns is identical to the table definition.
	 * @param  capacity The size of the internal buffer in bytes.
	 *         If the RO database runs in "memory packed" or "memory unpacked"
	 *         mode then no internal buffer is used and this value is ignored.
	 *         Otherwise, set this value equal to zero if you intend to load the
	 *         data of a single random row.
	 *         However, if you intend to iterate the rows of a table, a value
	 *         greater than zero should be chosen.
	 *         (The size of the internal buffer may be larger than this value).
	 * 
	 * @return The created row loader, never {@code null}.
	 * 
	 * @throws NullPointerException If the array of columns is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If the specified array of columns
	 *         contains at least one column that is not a column of {@code
	 *         table}.
	 */
	private final IRowLoader createRowLoader(Column<?>[] cols,
			int capacity) throws NullPointerException, IllegalArgumentException {
		Objects.requireNonNull(cols, ACDPException.prefix(table) +
													"Column array not allowed to be null.");
		// Check if columns are columns of table.
		if (cols.length > 0) {
			final Map<Column_<?>, ROColInfo> colInfoMap = store.colInfoMap;
			for (Column<?> col : cols) {
				if (colInfoMap.get(col) == null) {
					throw new IllegalArgumentException(ACDPException.prefix(table) +
														"Column \"" + col +
														"\" is not a column of this table.");
				}
			}
		}
		
		return new RORowLoader(muSectionLoader != null ? muSectionLoader :
										new FMPRowSectionLoader(capacity, store), cols);
	}
	
	@Override
	protected final IRowLoader createRowLoader(Column<?>[] cols) throws
										NullPointerException, IllegalArgumentException {
		return createRowLoader(cols, 0);
	}

	@Override
	protected final RowAdvancer createRowAdvancer(long start, long end,
									Column<?>[] cols) throws IllegalArgumentException {
		// Create the row loader with a buffer of 150'000 bytes. If the database
		// runs in "file packed" mode then this results in the buffering of about
		// 10 data blocks, depending on the compression rate of the packed data.
		return new RowAdvancer(createRowLoader(cols, 150000), start, end);
	}
	
	/**
	 * Provides the {@link #load} method which loads a particular <em>section of
	 * the row</em> into memory.
	 * A row section may be the section of the row housing the row header or one
	 * of the column sections of the row.
	 * A column section in turn houses the byte representation of a value of a
	 * particular column.
	 * In any case, the result of loading a row section is an array of bytes.
	 * <p>
	 * A row section is always <em>unpacked</em>, even if the data of the row
	 * was originally saved as <em>packed</em> data.
	 * (Packed data is compressed and optionally encrypted.)
	 * <p>
	 * Unlike other classes that implement {@link AutoCloseable}, a row section
	 * loader can still be used without any restriction after the {@code close}
	 * method has been invoked.
	 * However, to avoid resource leaks, the {@code close} method must be the
	 * last method invoked by the client before the lifetime of an instance of
	 * this class ends <em>unless</em> no method was called at all or invoking
	 * the {@code close} method has no effect.
	 * 
	 * @author Beat Hoermann
	 */
	private static interface IRowSectionLoader extends AutoCloseable {
		/**
		 * Loads a particular section of the row into the specified bag,
		 * unpacking the involved data if necessary.
		 * 
		 * @param  pos The position within the database file relative to the
		 *         starting position of the table data within the database file
		 *         where the section of the row starts.
		 *         This value must be greater than or equal to zero and such that
		 *         {@code pos + len} is less than or equal to the total length of
		 *         the unpacked table data.
		 * @param  len The length of the row section, must be greater than or
		 *         equal to zero.
		 * @param  bag The bag containing the loaded row section of length {@code
		 *         len}, not allowed to be {@code null}.
		 * 
		 * @throws ShutdownException If the file channel provider is shut down
		 *         due to a closed database.
		 *         This exception never happens if the operating mode of the RO
		 *         database is "memory packed" or "memory unpacked".
		 * @throws IOFailureException If an I/O error occurs or if a GZIP format
		 *         error occurs.
		 *         This exception never happens if the operating mode of the RO
		 *         database is "memory unpacked".
		 */
		void load(long pos, int len, Bag bag) throws ShutdownException,
																				IOFailureException;
		/**
		 * Closes this row section loader.
		 * 
		 * @throws IOFailureException if an I/O error occurs.
		 */
		@Override
		void close() throws IOFailureException;
	}
	
	/**
	 * A row section loader which "loads" a particular row section from an array
	 * of bytes containing the unpacked data of the table.
	 * (MU stands for "Memory Unpacked".)
	 * <p>
	 * Invoking the {@code close} method has no effect.
	 *
	 * @author Beat Hoermann
	 */
	private static final class MURowSectionLoader implements IRowSectionLoader {
		/**
		 * The unpacked table data, never {@code null}.
		 */
		private final byte[] tableData;
		
		/**
		 * The constructor.
		 * 
		 * @param tableData The unpacked table data, not allowed to be {@code
		 *        null}.
		 */
		private MURowSectionLoader(byte[] tableData) {
			this.tableData = tableData;
		}

		@Override
		public final void load(long pos, int len, Bag bag) {
			bag.bytes = tableData;
			bag.offset = (int) pos;
		}
		
		@Override
		public final void close() {
		}
	}
	
	/**
	 * A row section loader which loads a particular row section from a data
	 * source keeping the FL data as <em>packed</em> data.
	 * The data source is eithter the database file or an array of bytes
	 * containing the packed data of the table.
	 * (FMP stands for "File or Memory Packed".)
	 * 
	 * @author Beat Hoermann
	 */
	private final class FMPRowSectionLoader implements IRowSectionLoader {
		/**
		 * The {@code Unpacker} instance, never {@code null}.
		 */
		private final Unpacker unpacker;
		
		/**
		 * The constructor.
		 *
		 * @param  capacity The capacity of the internal buffer, hence, the size
		 *         of the internal buffer in bytes.
		 *         If the RO database runs in "memory packed" mode then no
		 *         internal buffer is used and this value is ignored.
		 *         Otherwise, set this value equal to zero if you intend to
		 *         load the data of a single random row.
		 *         However, if you intend to iterate the rows of a table, a value
		 *         greater than zero should be chosen.
		 *         (The size of the internal buffer may be larger then this
		 *         value).
		 * @param  store The store of the table, not allowed to be {@code null}.
		 */
		FMPRowSectionLoader(int capacity, ROStore store) {
			this.unpacker = new Unpacker(capacity, store);
		}
		
		@Override
		public final void load(long pos, int len, Bag bag) throws
													ShutdownException, IOFailureException {
			try {
				final byte[] bytes = bag.bytes;
				if (bytes.length >= len && len > 0)
					unpacker.unpack(pos, len, bytes, 0);
				else {
					final byte[] data = new byte[len];
					unpacker.unpack(pos, len, data, 0);
					bag.bytes = data;
				}
			} catch (IOException e) {
				if (store.dbFile.path != null)
					throw new IOFailureException(table, new FileIOException(
																			store.dbFile.path, e));
				else {
					throw new IOFailureException(table, e);
				}
			}
			bag.offset = 0;
		}
		
		@Override
		public final void close() throws IOFailureException {
			try {
				unpacker.close();
			} catch (IOException e) {
				if (store.dbFile.path != null)
					throw new IOFailureException(table, new FileIOException(
																			store.dbFile.path, e));
				else {
					throw new IOFailureException(table, e);
				}
			}
		}
	}
	
	/**
	 * The row loader of an RO table.
	 * 
	 * @author Beat Hoermann
	 */
	private final class RORowLoader implements IRowLoader {
		/**
		 * The row section loader, never {@code null}.
		 */
		private final IRowSectionLoader rsl;
		/**
		 * The bag, never {@code null}.
		 * This bag is reused when loading the various sections of a row.
		 * Even the byte array {@code Bag.bytes} may be reused!
		 */
		private final Bag bag;
		/**
		 * The array of columns, never {@code null} but may be empty.
		 */
		private final Column<?>[] cols;
		/**
		 * The column map or {@code null} if {@code cols} is empty.
		 * Maps a column contained in {@code cols} to its index within {@code
		 * cols}.
		 */
		private final Map<Column<?>, Integer> colMap;
		/**
		 * The number of columns in the resulting row, greater than zero.
		 */
		private final int nofCols;
		
		/**
		 * The constructor.
		 * 
		 * @param rsl The row section loader, not allowed to be {@code null}.
	    * @param cols The array of columns, not allowed to be {@code null}.
	    *        The columns must be columns of {@code table}.
	    *        If the array of columns is empty then this method behaves as if
	    *        the array of columns is identical to the table definition.
		 */
		RORowLoader(IRowSectionLoader rsl, Column<?>[] cols) {
			this.rsl = rsl;
			this.bag = new Bag(0);
			this.cols = cols;
			final int n = cols.length;
			if (n == 0) {
				this.colMap = null;
				this.nofCols = store.colInfoArr.length;
			}
			else {
				// cols.length > 0 && n == cols.length
				this.colMap = new HashMap<>(n * 4 / 3 + 1);
				for (int i = 0; i < n; i++) {
					colMap.put(cols[i], i);
				}
				this.nofCols = n;
			}
		}

		@Override
		public final Row_ load(long index) throws ShutdownException,
																				IOFailureException {
			// Precondition: index >= 0
			try {
				// Convert index to pos.
				final long pos = Utils.unsFromBytes(store.rowPointers,
										(int) index * store.nobsRowPtr, store.nobsRowPtr);
				// Load row header.
				rsl.load(pos, store.nH, bag);
				final byte[] rh = Arrays.copyOfRange(bag.bytes, bag.offset,
																			bag.offset + store.nH);
				final Object[] vals = new Object[nofCols]; // vals[i] == null
				final long ni = store.nNI == 0 ? 0L : Utils.unsFromBytes(rh,
																							store.nNI);
				final ROColInfo[] ciArr = store.colInfoArr;
				long offset = pos + store.nH;
				
				for (int i = 0; i < ciArr.length; i++) {
					final ROColInfo ci = ciArr[i];
					if ((ni & ci.nullBitMask) ==  0) {
						// The value is not marked as being null.
							
						// Get the length of the byte representation.
						// In an RO database the length of a byte representation
						// never exceeds Integer.MAX_VALUE.
						final int length = ci.len > -1 ? ci.len : (int) Utils.
												unsFromBytes(rh, ci.offset, ci.lengthLen);
						// Load the byte representation of the value and convert the
						// value to an object.
						if (colMap == null) {
							// colMap == null <=> cols.length == 0
							rsl.load(offset, length, bag);
							vals[i] = convertValue(ci, bag, length);
						}
						else {
							final Integer k = colMap.get(ci.col);
							if (k != null) {
								rsl.load(offset, length, bag);
								vals[k] = convertValue(ci, bag, length);
							}
						}
						
						offset += length;
					}
					// else: The value is marked as being null.
					//       The byte representation is of zero length.
				}
					
				return new Row_(table, cols, vals);
			} finally {
				rsl.close();
			}
		}
	}
	
	/**
	 * Converts the specified byte representation of a non-null value of an ST
	 * column to an instance of {@code Object}.
	 * 
	 * @param  st The simple type, not allowed to be {@code null}.
	 * @param  bag The bag containing the byte representation, not allowed to be
	 *         {@code null}.
	 * @param  length The length of the byte representation.
	 * 
	 * @return The value as an instance of {@code Object}.
	 *         The returned value is {@linkplain SimpleType#isCompatible(Object)
	 *         compatible} with the type of the column.
	 */
	private final Object valST(SimpleType<?> st, Bag bag, int length) {
		if (st.scheme() == Scheme.INROW)
			return st.convertFromBytes(bag.bytes, bag.offset);
		else {
			return st.convertFromBytes(bag.bytes, bag.offset, length);
		}
	}
	
	/**
	 * Converts the byte representation of a non-null array element to an
	 * instance of {@code Object} and updates {@code bag.offset}.
	 * 
	 * @param  st The type of the elements of the array value, not allowed to be
	 *         {@code null}.
	 * @param  stLen The length of the specified simple type.
	 * @param  inrow If {@code true} then the specified simple type has an inrow
	 *         storage scheme, otherwise it has an outrow storage scheme.
	 * @param  bag The bag containing the byte representation, not allowed to be
	 *         {@code null}.
	 * 
	 * @return The array element as an instance of {@code Object}.
	 */
	private final Object valEl(SimpleType<?> st, int stLen, boolean inrow,
																							Bag bag) {
		final Object val;
		if (inrow) {
			val = st.convertFromBytes(bag.bytes, bag.offset);
			bag.offset += stLen;
		}
		else {
			final int len = (int) Utils.unsFromBytes(bag.bytes, bag.offset, stLen);
			bag.offset += stLen;
			val = st.convertFromBytes(bag.bytes, bag.offset, len);
			bag.offset += len;
		}
		return val;
	}
	
	/**
	 * Converts the byte representation of a non-null and non-empty array value
	 * to an array of {@code Object}.
	 * <p>
	 * This method follows the storage scheme of an array column type.
	 * 
	 * @param  st The type of the elements of the array value, not allowed to be
	 *         {@code null}.
	 * @param  ci The column info object, not allowed to be {@code null}.
	 *         The column must be an array column.
	 * @param  bag The bag containing the byte representation of a non-null and
	 *         non-empty array value.
	 *         
	 * @return The array value as an array of {@code Object}, never {@code null}
	 *         and never empty.
	 *         The returned array value is {@linkplain
	 *         ArrayType_#isCompatible(Object) compatible} with the type of the
	 *         column.
	 */
	private final Object[] valAAT(SimpleType<?> st, ROColInfo ci, Bag bag) {
		final int size = (int) Utils.unsFromBytes(bag.bytes, bag.offset,
																						ci.sizeLen);
		// size > 0
		final Object[] val = (Object[]) Array.newInstance(st.valueType(), size);
		final int stLen = st.length();
		final boolean inrow = st.scheme() == Scheme.INROW;
		bag.offset += ci.sizeLen;
		
		if (st.nullable()) {
			final byte[] ni = bag.bytes;
			int niIndex = bag.offset;
			bag.offset += Utils.bmLength(size);
			
			byte niByte = ni[niIndex];
			byte mask = 1;
			for (int i = 0; i < size; i++) {
				if (mask == 0) {
					niIndex++;
					niByte = ni[niIndex];
					mask = 1;
				}
				if ((niByte & mask) != 0)
					val[i] = null;
				else {
					val[i] = valEl(st, stLen, inrow, bag);
				}
				mask <<= 1;
			}
		}
		else {
			for (int i = 0; i < size; i++) {
				val[i] = valEl(st, stLen, inrow, bag);
			}
		}
		return val;
	}
	
	/**
	 * Converts the byte representation of a non-null array value stored in
	 * the specified column with the specified array type to an array of {@code
	 * Object}.
	 * 
	 * @param  at The abstract array type of the column, not allowed to be
	 *         {@code null}.
	 * @param  ci The column info object, not allowed to be {@code null}.
	 *         The column must be an array column.
	 * @param  bag The bag containing the byte representation of a non-null
	 *         array value.
	 * @param  length The length of the byte representation.
	 * 
	 * @return The array value as an array of {@code Object}, never {@code null}.
	 *         The returned array value is {@linkplain
	 *         ArrayType_#isCompatible(Object) compatible} with the type of the
	 *         column.
	 */
	private final Object[] valAAT(AbstractArrayType at,
														ROColInfo ci, Bag bag, int length) {
		final SimpleType<?> st = at instanceof ArrayOfRefType_ ?
															RefST.newInstance(ci.nobsRowRef) :
															((ArrayType_) at).elementType();
		final Object[] val;
		if (length == 0)
			val = (Object[]) Array.newInstance(st.valueType(), 0);
		else {
			val = valAAT(st, ci, bag);
		}
		
		return val;
	}
	
	/**
	 * Converts the byte representation of a non-null value stored in the
	 * specified column to an instance of {@code Object}.
	 * 
	 * @param  ci The column info object, not allowed to be {@code null}.
	 * @param  bag The bag containing the byte representation, not allowed to be
	 *         {@code null}.
	 * @param  length The length of the byte representation.
	 * 
	 * @return The value as an instance of {@code Object}.
	 *         The returned value is {@linkplain
	 *         acdp.internal.types.Type_#isCompatible(Object) compatible} with
	 *         the type of the column.
	 */
	private final Object convertValue(ROColInfo ci, Bag bag, int length) {
		final Type_ type = ci.col.type();
		if (type instanceof SimpleType)
			// SimpleType
			return valST((SimpleType<?>) type, bag, length);
		else if (type instanceof RefType_)
			// RefType_
			return Utils.isZero(bag.bytes, bag.offset, ci.len) ? null : new Ref_(
									Utils.unsFromBytes(bag.bytes, bag.offset, ci.len));
		else {
			// AbstractArrayType
			return valAAT((AbstractArrayType) type, ci, bag, length);
		}
	}
}