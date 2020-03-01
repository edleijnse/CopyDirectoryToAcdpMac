/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import acdp.design.SimpleType;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.MaximumException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.Buffer;
import acdp.internal.Column_;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.IUnit;
import acdp.internal.Ref_;
import acdp.internal.Table_;
import acdp.internal.misc.Utils_;
import acdp.internal.CryptoProvider.WRCrypto;
import acdp.internal.store.Bag;
import acdp.internal.store.RefST;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.internal.types.AbstractArrayType;
import acdp.internal.types.ArrayOfRefType_;
import acdp.internal.types.ArrayType_;
import acdp.internal.types.RefType_;
import acdp.internal.types.Type_;
import acdp.misc.Utils;
import acdp.types.Type.Scheme;

/**
 * Defines a converter that converts a value of a given column type to its row
 * data.
 * <p>
 * By invoking the {@link #create(WRStore.WRColInfo)} method, a {@link WRStore}
 * creates for each column of the table an {@link IObjectToBytes} instance which
 * in turn is used by the insert, update operations or by a writing maintenance
 * operation to persist any values passed to them.
 * <p>
 * The conversion involves some important side effects.
 * Please consult the {@link IObjectToBytes} interface description for any
 * details.
 * <p>
 * The {@link BytesToObject} class represents the counterpart to this class.
 *
 * @author Beat Hoermann
 */
final class ObjectToBytes {

	/**
	 * Defines the {@link #convert} method which converts a value of a given
	 * column type to its FL data, ready for being written to the FL data file.
	 * <p>
	 * The conversion involves writing the byte representation of the value to
	 * the VL data file, provided that the value is not equal to {@code null} and 
	 * provided that the column type has an outrow storage scheme or is an
	 * INROW A[OUTROW ST].
	 * Similarly, if the value is equal to {@code null} but the stored value is
	 * not equal to {@code null} then the corresponding allocated VL file space
	 * is deallocated.
	 * Furthermore, if the column type is an RT or A[RT] then the reference
	 * counter of the referenced rows in the referenced tables are updated.
	 * <p>
	 * The {@link BytesToObject.IBytesToObject IBytesToObject} interface
	 * represents the counterpart to this interface.
	 *
	 * @author Beat Hoermann
	 */
	static interface IObjectToBytes {
		/**
		 * Converts the specified value to its FL data.
		 * <p>
		 * Depending on the column type of the value and whether the value is
		 * equal to {@code null} this method either updates the bitmap of the row
		 * or it puts the resulting FL column data, that is, either the byte
		 * representation of the value or a pointer to it, into the {@code
		 * bag.bytes} byte array starting at {@code bag.offset}.
		 * <p>
		 * As a side effect, this method writes the resulting byte representation
		 * to the VL data file, provided that the value is not equal to {@code
		 * null} and provided that the column type has an outrow storage scheme
		 * or is an INROW A[OUTROW ST].
		 * Similarly, if the value is equal to {@code null} but the stored value
		 * is not equal to {@code null} then the corresponding allocated VL file
		 * space is deallocated.
		 * Furthermore, if the column type is an RT or A[RT] then the reference
		 * counter of the referenced rows in the referenced tables are updated.
		 * <p>
		 * The value is assumed to be compatible with the column type.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB2},
		 * {@linkplain WRStore.GlobalBuffer GB3}.
		 * 
		 * @param  val The value to convert, may be {@code null}.
		 * @param  bitmap The bitmap of the row.
		 * @param  bag0 The bag containing the stored FL column data.
		 *         If this value is {@code null} then this method assumes that
		 *         {@code bag.bytes} is filled with zeros over the entire length
		 *         of the FL column data.
		 * @param  unit The unit, may be {@code null}.
		 * @param  bag The bag containing the new FL column data, not allowed to
		 *         be {@code null}.
		 * 
		 * @return The resulting bitmap of the row.
		 * 
		 * @throws NullPointerException If the value is a simple value set equal
		 *         to {@code null} but the column type forbids the {@code null}
		 *         value or if the value is an array value and this condition is
		 *         satisfied for at least one element contained in the array.
		 * @throws IllegalArgumentException If the length of the byte
		 *         representation of the value (or one of the elements if the
		 *         value is an array value) exceeds the maximum length allowed by
		 *         the simple column type.
		 *         This exception also happens if the value is an array value and
		 *         the size of the array exceeds the maximum length allowed by
		 *         the array column type.
		 *         Last, this exception also happens if the value is a reference
		 *         and the reference points to a row that does not exist within
		 *         the referenced table or if the reference points to a row gap
		 *         or if the value is an array of references and this condition
		 *         is satisfied for at least one of the references contained in
		 *         the array.
		 * @throws MaximumException If a new memory block in the VL file space
		 *         must be allocated and its file position exceeds the maximum
		 *         allowed position or if the maximum value of the reference
		 *         counter of a referenced row is exceeded.
		 * @throws CryptoException If encryption fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption or if the column type is an RT or A[RT].
		 * @throws UnitBrokenException If recording before data fails.
		 * @throws FileIOException If an I/O error occurs.
		 *         This exception can happen only if the column type has an
		 *         outrow storage scheme or is an INROW A[OUTROW ST].
		 */
		long convert(Object val, long bitmap, Bag bag0, IUnit unit,
														Bag bag) throws NullPointerException,
								IllegalArgumentException, MaximumException,
								CryptoException, UnitBrokenException, FileIOException;
	}

	/**
	 * The simple type value converter converts a ST value to its byte
	 * representation.
	 * Rather than invoking the conversion methods of the {@code SimpleType}
	 * class directly, the conversion methods of this class take into account
	 * that the WR database may apply encryption.
	 *
	 * @author Beat Hoermann
	 */
	private static abstract class STtoBytes {
		
		/**
		 * A concrete implementation of a simple type value converter for a WR
		 * database that applies encryption.
		 * Note that encryption is not applied to a value of a {@link RefST}
		 * simple type.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class CryptoSTtoBytes extends STtoBytes {
			/**
			 * The crypto object of the WR database, never {@code null}.
			 */
			private final WRCrypto wrCrypto;
			
			/**
			 * The constructor.
			 * 
			 * @param wrCrypto The crypto object of the WR database, not allowed to
			 *        be {@code null}.
			 */
			CryptoSTtoBytes(WRCrypto wrCrypto) {
				this.wrCrypto = wrCrypto;
			}
			
			@Override
			final byte[] toBytes(Object val, SimpleType<?> st) throws
											NullPointerException, IllegalArgumentException,
																					CryptoException {
				byte[] bytes = st.convertToBytes(val);
				if (!(st instanceof RefST)) {
					wrCrypto.encrypt(bytes, 0, bytes.length);
				}
				return bytes;
			}
			
			@Override
			final void toBytes(Object val, SimpleType<?> st, byte[] bytes,
																				int offset) throws
											NullPointerException, IllegalArgumentException,
											IndexOutOfBoundsException, CryptoException {
				final int len = st.convertToBytes(val, bytes, offset);
				if (!(st instanceof RefST)) {
					wrCrypto.encrypt(bytes, offset, len);
				}
			}
		}
		
		/**
		 * A concrete implementation of a simple value converter for a WR database
		 * that does not apply encryption.
		 * This atually boils down to an implementation that directly invokes the
		 * conversion methods of the {@code SimpleType} class.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class PlainSTtoBytes extends STtoBytes {
			
			@Override
			final byte[] toBytes(Object val, SimpleType<?> st) throws
										NullPointerException, IllegalArgumentException {
				return st.convertToBytes(val);
			}
			
			@Override
			final void toBytes(Object val, SimpleType<?> st, byte[] bytes,
																				int offset) throws
											NullPointerException, IllegalArgumentException,
											IndexOutOfBoundsException {
				st.convertToBytes(val, bytes, offset);
			}
		}
		
		/**
		 * Creates a simple type object to bytes converter.
		 * 
		 * @param  wrCrypto The WR crypto object from the WR database.
		 *         This value is {@code null} if and only if the WR database does
		 *         not apply encryption.
		 *         
		 * @return The created simple type object to bytes converter.
		 */
		static final STtoBytes newInstance(WRCrypto wrCrypto) {
			if (wrCrypto != null)
				return new CryptoSTtoBytes(wrCrypto);
			else {
				return new PlainSTtoBytes();
			}
		}
		
		/**
		 * Converts the specified value of the specified simple type to an array
		 * of bytes.
		 * <p>
		 * This method assumes that calling the {@link SimpleType#isCompatible}
		 * method on {@code val} returns {@code true}.
		 * If this is not the case then this method may throw an exception that
		 * is not mentioned below.
		 * 
		 * @param  val The value to convert, not allowed to be {@code null}.
		 * @param  st The simple type, not allowed to be {@code null}.
		 *         
		 * @return The value as an array of bytes, never {@code null}.
		 * 
		 * @throws NullPointerException If {@code val} is {@code null}.
		 * @throws IllegalArgumentException If the length of the byte
		 *         representation of the specified value exceeds the maximum
		 *         number of bytes allowed by the specified simple type.
		 * @throws CryptoException If encrypting the byte array fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 */
		abstract byte[] toBytes(Object val, SimpleType<?> st) throws
					NullPointerException, IllegalArgumentException, CryptoException;
		
		/**
		 * Converts the specified value of the specified simple type to an array
		 * of bytes and puts it into the specified array of bytes starting at the
		 * specified offset.
		 * <p>
		 * This method assumes that calling the {@link SimpleType#isCompatible}
		 * method on {@code val} returns {@code true}.
		 * If this is not the case then this method may throw an exception that
		 * is not mentioned below.
		 * 
		 * @param  val The value to convert, not allowed to be {@code null}.
		 * @param  st The simple type, not allowed to be {@code null}.
		 * @param  bytes The destination byte array.
		 * @param  offset The index within {@code bytes} where to start saving
		 *         the converted value.
		 * 
		 * @throws NullPointerException If {@code val} or {@code bytes} is
		 *         {@code null}.
		 * @throws IllegalArgumentException If the length of the byte
		 *         representation of the specified value exceeds the maximum
		 *         number of bytes allowed by the specified simple type.
		 * @throws IndexOutOfBoundsException If saving the byte represenation
		 *         would cause access of data outside of the array bounds of the
		 *         specified  byte array.
		 * @throws CryptoException If encrypting the byte array fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 */
		abstract void toBytes(Object val, SimpleType<?> st, byte[] bytes,
																				int offset) throws
											NullPointerException, IllegalArgumentException,
											IndexOutOfBoundsException, CryptoException;
	}
	
	/**
	 * Keeps the pointer to some outrow data and its length.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class PtrLength {
		/**
		 * The pointer to the outrow data.
		 */
		long ptr;
		/**
		 * The number of bytes of the outrow data.
		 */
		long length;
	}
	
	/**
	 * The table, never {@code null}.
	 */
	private final Table_ table;
	/**
	 * The table's store, never {@code null}.
	 */
	private final WRStore store;
	/**
	 * The store's VL data file, never {@code null}.
	 */
	private final FileIO vlDataFile;
	/**
	 * The store's VL file space.
	 * This value is {@code null} if the table has no outrow data.
	 */
	private final VLFileSpace vlFileSpace;
	/**
	 * The simple type converter, never {@code null}.
	 */
	private final STtoBytes stc;
	/**
	 * An instance of {@code PtrLength} to be reused whenever some outrow data
	 * must be accessed, never {@code null}.
	 */
	private final PtrLength ptrLength;
	/**
	 * The length of the byte representation of a {@linkplain PtrLength#ptr
	 * pointer to some outrow data}.
	 */
	private final int nobsOutrowPtr;
	
	/**
	 * The constructor.
	 * <p>
	 * The database must be {@linkplain acdp.internal.Database_#isWritable writable}.
	 * 
	 * @param store The WR store of a table, not allowed to be {@code null}.
	 */
	ObjectToBytes(WRStore store) {
		table = store.table;
		this.store = store;
		vlDataFile = store.vlDataFile;
		vlFileSpace = store.vlFileSpace;
		stc = STtoBytes.newInstance(store.table.db().wrCrypto());
		ptrLength = new PtrLength();
		nobsOutrowPtr = store.nobsOutrowPtr;
	}
	
	/**
	 * Depending on the specified column this factory method creates and returns
	 * a new instance of a class implementing the {@code IObjectToBytes}
	 * interface.
	 * 
	 * @param  ci The column info object, not allowed to be {@code null}.
	 * 
	 * @return The created instance of {@code IObjectToBytes}, never {@code
	 *         null}.
	 */
	final IObjectToBytes create(WRColInfo ci) {
		final Type_ type = ci.col.type();
		final Scheme scheme = type.scheme();
		if (type instanceof SimpleType) {
			// SimpleType
			if (scheme == Scheme.INROW)
				// INROW SimpleType
				return new STIn((SimpleType<?>) type, ci);
			else {
				// OUTROW SimpleType
				return new STOut((SimpleType<?>) type, ci);
			}
		}
		else if (type instanceof RefType_)
			// RefType_
			return new RT(ci);
		else {
			// AbstractArrayType
			return new AAT((AbstractArrayType) type, ci);
		}
	}
	
	/**
	 * The converter for an INROW ST column.
	 *
	 * @author Beat Hoermann
	 */
	private final class STIn implements IObjectToBytes {
		private final SimpleType<?> st;
		private final boolean nullable;
		private final long nullBitMask;
		private final Column_<?> col;

		/**
		 * The constructor.
		 * 
		 * @param  st The simple type assumed to have an <em>inrow</em> storage
		 *         scheme, not allowed to be {@code null}.
		 * @param  ci The column info object, not allowed to be {@code null}.
		 */
		STIn(SimpleType<?> st, WRColInfo ci) {
			this.st = st;
			nullable = st.nullable();
			nullBitMask = ci.nullBitMask;
			col = ci.col;
		}
		
		/**
		 * Converts the specified value which is assumed to be compatible with
		 * the simple type passed via the constructor of this class.
		 * 
		 * @param  val The value, may be {@code null}.
		 * @param  bitmap The bitmap of the row.
		 * @param  bag0 Not used by this method.
		 * @param  unit Not used by this method.
		 * @param  bag The bag into which this method puts the byte representation
		 *         of the specified value, provided that the value is not {@code
		 *         null}.
		 *         
		 * @return The resulting bitmap of the row.
		 * 
		 * @throws NullPointerException If {@code val} is {@code null} but the
		 *         column type forbids the {@code null} value.
		 * @throws IllegalArgumentException If the length of the byte
		 *         representation of the specified value exceeds the maximum
		 *         length allowed by simple column type.
		 * @throws CryptoException If encryption fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 */
		@Override
		public final long convert(Object val, long bitmap, Bag bag0, IUnit unit,
														Bag bag) throws NullPointerException,
												IllegalArgumentException, CryptoException {
			if (val == null) {
				if (!nullable) {
					throw new NullPointerException(ACDPException.prefix(table, col) +
												"The value is null but the type of the " +
												"column does not allow the null value.");
				}
				// INROW nullable SimpleType
				bitmap |= nullBitMask;
			}
			else {
				// val != null
				bitmap &= ~nullBitMask;
				stc.toBytes(val, st, bag.bytes, bag.offset);
			}

			return bitmap;
		}
	}
	
	/**
	 * Checks if the specified position exceeds the maximum allowed position.
	 * 
	 * @param  pos The position to test.
	 * 
	 * @throws MaximumException If the specified position is greater than the
	 *         maximum allowed position.
	 */
	private final void checkPos(long pos) throws MaximumException {
		if (pos > Utils_.bnd8[nobsOutrowPtr]) {
			String str = "Maximum size of VL data file exceeded! Compact the " +
																"VL file space of the table!";
			if (nobsOutrowPtr < 8) {
				str += " You may want to refactor the table with a higher value " +
																		"of \"nobsOutrowPtr\".";
			}
			throw new MaximumException(table, str);
		}
	}

	/**
	 * Replaces the outrow data referenced by the value of the {@link #ptrLength}
	 * global property with the specified new outrow data and updates {@code
	 * ptrLength} with the pointer to the newly stored outrow data.
	 * <p>
	 * If there exists no stored outrow data then the pointer and the length of
	 * {@code ptrLength} are assumed to be both set to zero.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB2}.
	 * 
	 * @param  vlData The new outrow data, not allowed to be {@code null} but
	 *         may be of zero length.
	 * @param  unit The unit.
	 * 
	 * @throws MaximumException If a new memory block in the VL file space must
	 *         be allocated and its file position exceeds the maximum allowed
	 *         position.
	 * @throws UnitBrokenException If recording before data fails.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void replaceVLData(ByteBuffer vlData, IUnit unit) throws
								MaximumException, UnitBrokenException, FileIOException {
		// ptrLength.length > 0 if and only if there exists a stored value and
		// it is not null and its length is positive.

		final int len0 = (int) ptrLength.length;
		final int len = vlData.limit();
		
		if (len == 0 || len > len0) {
			// Can't reuse file space.
			// Write vlData.
			final long ptr = vlFileSpace.allocate(len, unit);
			checkPos(ptr);
			vlDataFile.write(vlData, ptr);
			// Deallocate.
			if (len0 > 0) {
				vlFileSpace.deallocate(len0, unit);
			}
			setPtrLength(ptr, len);
		}
		else {
			// 0 < len && len <= len0
			// It follows len0 > 0.
			// Reuse file space.
			final long ptr0 = ptrLength.ptr;
			if (unit != null) {
				// Read first len bytes of stored outrow data.
				final Buffer gb2 = store.gb2;
				final ByteBuffer buf = len <= gb2.maxCap() ? gb2.buf(len) :
																		ByteBuffer.allocate(len);
				vlDataFile.read(buf, ptr0);
				final byte[] vlData0 = buf.array();
				// Record before data and write new outrow data only if necessary.
				if (!Utils.equals(vlData0, vlData.array(), 0, len)) {
					// Record before data.
					unit.record(vlDataFile, ptr0, vlData0, 0, len);
					// Write new outrow data.
					vlDataFile.write(vlData, ptr0);
				}
			}
			else {
				// Do not read stored outrow data.
				vlDataFile.write(vlData, ptr0);
			}

			if (len < len0) {
				// Deallocate.
				vlFileSpace.deallocate(len0 - len, unit);
				setPtrLength(ptr0, len);
			}
		}
	}

	/**
	 * Puts the value of the {@link #ptrLength} global property into the
	 * specified byte array at the specified offset.
	 * 
	 * @param lengthLen The number of bytes needed to save the value of the
	 *        length field of the {@link #ptrLength} global property.
	 * @param toArr The target byte array, not allowed to be {@code null} and
	 *        must be long enough.
	 * @param off The starting index.
	 */
	private final void putPtrLength(int lengthLen, byte[] toArr, int off) {
		Utils.unsToBytes(ptrLength.length, lengthLen, toArr, off);
		Utils.unsToBytes(ptrLength.ptr, nobsOutrowPtr, toArr, off + lengthLen);
	}

	/**
	 * Sets the value of the {@link #ptrLength} global property according to
	 * the information stored in the specified byte array.
	 * 
	 * @param fromArr The byte array, not allowed to be {@code null}.
	 * @param off The starting index.
	 * @param lengthLen The number of bytes needed to save the value of the
	 *        length field of the {@link #ptrLength} global property.
	 */
	private final void setPtrLength(byte[] fromArr, int off, int lengthLen) {
		ptrLength.ptr = Utils.unsFromBytes(fromArr, off + lengthLen,
																					nobsOutrowPtr);
		ptrLength.length = Utils.unsFromBytes(fromArr, off, lengthLen);
	}
	
	/**
	 * Sets the value of the {@link #ptrLength} global property to the specified
	 * pointer and length values.
	 * 
	 * @param ptr The pointer value.
	 * @param length The length value.
	 */
	private final void setPtrLength(long ptr, long length) {
		ptrLength.ptr = ptr;
		ptrLength.length = length;
	}

	/**
	 * The converter for an OUTROW ST column.
	 *
	 * @author Beat Hoermann
	 */
	private final class STOut implements IObjectToBytes {
		private final SimpleType<?> st;
		private final boolean nullable;
		private final int lengthLen;
		private final Column_<?> col;
		
		/**
		 * The constructor.
		 * 
		 * @param  st The simple type assumed to have an <em>outrow</em> storage
		 *         scheme, not allowed to be {@code null}.
		 * @param  ci The column info object, not allowed to be {@code null}.
		 */
		STOut(SimpleType<?> st, WRColInfo ci) {
			this.st = st;
			nullable = st.nullable();
			lengthLen = ci.lengthLen;
			col = ci.col;
		}
		
		/**
		 * Converts the specified value which is assumed to be compatible with
		 * the simple type passed via the constructor of this class.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB2}.
		 * 
		 * @param  val The value, may be {@code null}.
		 * @param  bitmap The bitmap of the row, not used by this method.
		 * @param  bag0 The bag containing the stored FL column data.
		 *         If this value is {@code null} then this method assumes that
		 *         {@code bag.bytes} is filled with zeros over the entire length
		 *         of the FL column data.
		 * @param  unit The unit.
		 * @param  bag The bag into which this method puts the pointer to the
		 *         byte representation of the specified value.
		 *         
		 * @return The unchanged bitmap of the row.
		 * 
		 * @throws NullPointerException If {@code val} is {@code null} but the
		 *         column type forbids the {@code null} value.
		 * @throws IllegalArgumentException If the length of the byte
		 *         representation of the specified value exceeds the maximum
		 *         length allowed by the simple column type.
		 * @throws MaximumException If a new memory block in the VL file space
		 *         must be allocated and its file position exceeds the maximum
		 *         allowed position.
		 * @throws CryptoException If encryption fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 * @throws UnitBrokenException If recording before data fails.
		 * @throws FileIOException If an I/O error occurs.
		 */
		@Override
		public final long convert(Object val, long bitmap, Bag bag0, IUnit unit,
																					Bag bag) throws
								NullPointerException, IllegalArgumentException,
								MaximumException, CryptoException, UnitBrokenException,
																				FileIOException {
			if (bag0 == null) {
				// Insert: bag.bytes is assumed to contain zeros for the column
				//         data.
				if (val == null) {
					if (!nullable) {
						throw new NullPointerException(ACDPException.prefix(table,
												col) +
												"The value is null but the type of the " +
												"column does not allow the null value.");
					}
				}
				else {
					setPtrLength(0, 0);
					final ByteBuffer vlData = ByteBuffer.wrap(stc.toBytes(val, st));
					replaceVLData(vlData, unit);
					putPtrLength(lengthLen, bag.bytes, bag.offset);
				}
			}
			else {
				if (val == null) {
					if (!nullable) {
						throw new NullPointerException(ACDPException.prefix(table,
												col) +
												"The value is null but the type of the " +
												"column does not allow the null value.");
					}
					final long len0 = Utils.unsFromBytes(bag0.bytes, bag0.offset,
																							lengthLen);
					// Deallocate.
					if (len0 > 0) {
						store.vlFileSpace.deallocate(len0, unit);
					}
					setPtrLength(0, 0);
				}
				else {
					setPtrLength(bag0.bytes, bag0.offset, lengthLen);
					replaceVLData(ByteBuffer.wrap(stc.toBytes(val, st)), unit);
				}
				putPtrLength(lengthLen, bag.bytes, bag.offset);
			}
			
			return bitmap;
		}
	}
	
	/**
	 * The converter for an RT column.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class RT implements IObjectToBytes {
		private final int len;
		private final WRStore refdStore;
		
		/**
		 * The constructor.
		 * 
		 * @param  ci The column info object, not allowed to be {@code null}.
		 *         The type of the column must be a reference type.
		 */
		RT(WRColInfo ci) {
			len = ci.len;
			refdStore = ci.refdStore;
		}

		/**
		 * Converts the specified value which is assumed to be either {@code null}
		 * or an instance of the {@code Ref_} class.
		 * 
		 * @param  val The reference, may be {@code null}.
		 *         The value must be an instance of the {@code Ref_} class.
		 * @param  bitmap The bitmap of the row, not used by this method.
		 * @param  bag0 The bag containing the stored FL column data.
		 *         If this value is {@code null} then this method assumes that
		 *         {@code bag.bytes} is filled with zeros over the entire length
		 *         of the FL column data.
		 * @param  unit The unit.
		 * @param  bag The bag into which this method puts the byte representation
		 *         of the specified reference.
		 *         
		 * @return The unchanged bitmap of the row.
		 * 
		 * @throws IllegalArgumentException If the reference points to a row that
		 *         does not exist within the referenced table or if the reference
		 *         points to a row gap.
		 * @throws MaximumException If the maximum value of the reference counter
		 *         is exceeded.
		 * @throws UnitBrokenException If recording before data fails.
		 * @throws FileIOException If an I/O error occurs.
		 */
		@Override
		public final long convert(Object val, long bitmap, Bag bag0, IUnit unit,
																					Bag bag) throws
								NullPointerException, IllegalArgumentException,
								MaximumException, CryptoException, UnitBrokenException,
																				FileIOException {
			if (bag0 == null) {
				// Insert: bag.bytes is assumed to contain zeros for the column
				//         data.
				if (val != null) {
					final long ri = ((Ref_) val).rowIndex();
					GenericWriteOp.inc(refdStore, ri, 1, unit);
					Utils.unsToBytes(ri, len, bag.bytes, bag.offset);
				}
			}
			else {
				final long ri = val == null ? 0 : ((Ref_) val).rowIndex();
				final long ri0 = Utils.unsFromBytes(bag0.bytes, bag0.offset, len);
				if (ri0 != ri) {
					// Stored ref and new ref are different.
					if (ri0 != 0) {
						GenericWriteOp.inc(refdStore, ri0, -1, unit);
					}
					if (ri != 0) {
						GenericWriteOp.inc(refdStore, ri, 1, unit);
						Utils.unsToBytes(ri, len, bag.bytes, bag.offset);
					}
					else {
						final int off = bag.offset;
						Arrays.fill(bag.bytes, off, off + len, (byte) 0);
					}
				}
			}
			return bitmap;
		}
	}
	
	/**
	 * The converter for a column being of an array column type.
	 * 
	 * @author Beat Hoermann
	 */
	private final class AAT implements IObjectToBytes {
		private final AbstractArrayType aat;
		private final WRColInfo ci;
		private final long nullBitMask;
		private final int len;
		private final int sizeLen;
		private final int lengthLen;
		private final boolean isArrayOfRefType;
		private final SimpleType<?> st;
		private final boolean stNullable;
		private final int stLen;
		private final boolean isSTOutrow;
		private final boolean isAATInrow;
		private final boolean isInAATIn;
		
		/**
		 * The constructor.
		 * 
		 * @param  aat The array column type, not allowed to be {@code null}.
		 * @param  ci The column info object, not allowed to be {@code null}.
		 */
		AAT(AbstractArrayType aat, WRColInfo ci) {
			this.aat = aat;
			this.ci = ci;
			nullBitMask = ci.nullBitMask;
			len = ci.len;
			sizeLen = ci.sizeLen;
			lengthLen = ci.lengthLen;
			isArrayOfRefType = aat instanceof ArrayOfRefType_;
			if (isArrayOfRefType)
				st = RefST.newInstance(ci.refdStore.nobsRowRef);
			else {
				st = ((ArrayType_) aat).elementType();
			}
			stNullable = st.nullable();
			stLen = st.length();
			isSTOutrow = st.scheme() == Scheme.OUTROW;
			isAATInrow = aat.scheme() == Scheme.INROW;
			isInAATIn = isAATInrow && !isSTOutrow;
		}
		
		/**
		 * Puts the byte representation of the non-null <em>inrow</em> elements of
		 * the specified array value into the specified byte array.
		 * 
		 * @param  arr The array value, not allowed to be {@code null}.
		 * @param  toArr The target byte array, not allowed to be {@code null}
		 *         and must be long enough.
		 * @param  offset The starting index.
		 * 
		 * @throws NullPointerException If an element of {@code arr} is equal to
		 *         {@code null}.
		 * @throws IllegalArgumentException If the length of the byte
		 *         representation of at least one of the elements exceeds the
		 *         maximum length allowed by the element type.
		 * @throws CryptoException If encryption fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 */
		private final void putInrowElements(Object[] arr, byte[] toArr,
																				int offset) throws
					NullPointerException, IllegalArgumentException, CryptoException {
			for (Object el : arr) {
				stc.toBytes(el, st, toArr, offset);
				offset += stLen;
			}
		}
		
		/**
		 * Puts the byte representation of the <em>inrow</em> elements of the
		 * specified array value into the specified byte array starting with the
		 * Null-info indicating if an element is equal to {@code null}.
		 * <p>
		 * The {@code toArr} will house the Null-info from {@code offset} to
		 * {@code offset} + {@code nobsNI} - 1 and the elements of {@code arr}
		 * from {@code offset} + {@code nobsNI} to {@code offset} +
		 * {@code nobsNI} + {@code arr.length} * {@code stLen} - 1, where
		 * {@code nobsNI} = ({@code arr.length} - 1) / 8 + 1.
		 * 
		 * @param  arr The array value, not allowed to be {@code null}.
		 * @param  toArr The target byte array, must be long enough.
		 * @param  offset The starting index.
		 * 
		 * @throws IllegalArgumentException If the length of the byte
		 *         representation of at least one of the elements exceeds the
		 *         maximum length allowed by the element type.
		 * @throws CryptoException If encryption fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 */
		private final void putInrowElementsNullInfo(Object[] arr, byte[] toArr,
																				int offset) throws
												IllegalArgumentException, CryptoException {
			byte bmByte = 0;
			byte mask = 1;
			// The position where to write Null-info.
			int bmOffset = offset;
			// The position where to write elements - immediately after Null-info.
			int elOffset = bmOffset + Utils.bmLength(arr.length);
			for (Object el : arr) {
				if (mask == 0) {
					toArr[bmOffset] = bmByte;
					bmByte = 0;
					mask = 1;
					bmOffset++;
				}
				if (el == null)
					bmByte |= mask;
				else {
					stc.toBytes(el, st, toArr, elOffset);
					elOffset += stLen;
				}
				mask <<= 1;
				// bmOffset == offset + k / 8, k being the index of el in arr
			}
			// bmOffset == offset + (arr.length - 1) / 8
			// nobsNI == Utils.bmLength(arr.length) == (arr.length - 1) / 8 + 1
			// bmOffset == offset + nobsNI - 1;
			toArr[bmOffset] = bmByte;
		}
		
		/**
		 * Converts the specified array value which is assumed to be compatible
		 * with the array column type passed via the constructor of this class.
		 * <p>
		 * This method follows the storage scheme of an INROW A[INROW ST/RT].
		 * 
		 * @param  arr The array value, may be {@code null} and may be empty.
		 * @param  bitmap The bitmap of the row.
		 * @param  bag0 The bag containing the byte representation of the stored
		 *         array value.
		 *         If this value is {@code null} then this method assumes that
		 *         {@code bag.bytes} is filled with zeros over the entire length
		 *         of the FL column data.
		 * @param  bag The bag into which this method puts the byte representation
		 *         of the specified array value, provided that it is not {@code
		 *         null}.
		 * 
		 * @return The resulting bitmap of the row.
		 * 
		 * @throws NullPointerException If an element of {@code arr} is equal to
		 *         {@code null} but the element type forbids the {@code null}
		 *         value.
		 * @throws IllegalArgumentException If the length of the byte
		 *         representation of at least one of the elements exceeds the
		 *         maximum length allowed by the element type.
		 * @throws CryptoException If encryption fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 */
		private final long convInAATIn(Object[] arr, long bitmap, Bag bag0,
																					Bag bag) throws
					NullPointerException, IllegalArgumentException, CryptoException {
			// INROW ArrayType_ with INROW SimpleType
			if (arr == null)
				bitmap |= nullBitMask;
			else {
				bitmap &= ~nullBitMask;
				if (arr.length == 0) {
					// bag0 == null indicates an insert operation: bag.bytes is
					// assumed to contain zeros for the FL column data.
					if (bag0 != null) {
						// Set size to 0.
						Utils.unsToBytes(0, sizeLen, bag.bytes, bag.offset);
					}
				}
				else {
					// arr.length > 0
					Utils.unsToBytes(arr.length, sizeLen, bag.bytes, bag.offset);
					if (stNullable)
						putInrowElementsNullInfo(arr, bag.bytes,bag.offset + sizeLen);
					else {
						putInrowElements(arr, bag.bytes, bag.offset + sizeLen);
					}
				}
			}
			return bitmap;
		}
		
		/**
		 * Converts the specified array value which is assumed to be either
		 * {@code null} or empty.
		 * <p>
		 * This method follows the storage scheme of an outrow array type or of
		 * an INROW A[OUTROW ST].
		 * 
		 * @param  arr The array value, must either be {@code null} or empty.
		 * @param  bag0 The bag containing the length of and the pointer to the
		 *         byte representation of the stored array value.
		 *         If this value is {@code null} then this method assumes that
		 *         {@code bag.bytes} is filled with zeros over the entire length
		 *         of the FL column data.
		 * @param  unit The unit.
		 * @param  bag The bag into which this method puts the length of and the
		 *         pointer to the byte representation of the specified array
		 *         value.
		 */
		private final void trivialOtherAAT(Object[] arr, Bag bag0, IUnit unit,
																							Bag bag) {
			if (bag0 != null) {
				// Updater: There exists a stored array value.
				final long len0 = Utils.unsFromBytes(bag0.bytes, bag0.offset,
																							lengthLen);
				if (len0 > 0) {
					// Deallocate stored array value since specified array value is
					// null or empty.
					vlFileSpace.deallocate(len0, unit);
				}
				// Set length and ptr to 0.
				final int off = bag.offset;
				Arrays.fill(bag.bytes, off, off + len, (byte) 0);
			}
			
			if (arr != null) {
				// It follows that arr.length == 0. Set ptr to 1.
				Utils.unsToBytes(1, nobsOutrowPtr, bag.bytes, bag.offset+lengthLen);
			}
		}
		
		/**
		 * The writer ensures that only large blocks of data instead of small
		 * pieces are written to the VL file space in order to improve write
		 * performance.
		 * <p>
		 * The writer writes data to the file position returned by a call to the
		 * {@code VLFileSpace.allocate} method.
		 * Thus, each time the writer writes data to the VL file space, the writer
		 * allocates a <em>new</em> memory block.
		 * Since this class relies on the special property of the {@code
		 * VLFileSpace.allocate} method returning memory blocks that subsequently
		 * follow each other without a gap, all data written by this writer is
		 * stored in a contigous memory block of the VL file space.
		 * <p>
		 * If the {@link #ptr() ptr} method returns 0 then this writer has not
		 * yet written any data to the VL file space.
		 * In such a case the user can invoke the {@link #buffer} method which
		 * returns the internal buffer.
		 * (Any data given to the writer via its {@link #feed} method is cashed
		 * in this internal buffer.)
		 * This makes it possible for the user to store the buffer's content
		 * under his own control, most notably with the purpose of reusing an
		 * already allocated memory block within the VL file space.
		 * 
		 * @author Beat Hoermann
		 */
		private final class Writer extends AbstractWriter {
			private IUnit unit;
			// Remember the file position of the first memory block written to the
			// VL file space.
			private long ptr;
			// Remember the number of written bytes.
			private long length;
			
			/**
			 * The constructor.
			 * 
			 * @param buffer The buffer to apply, not allowed to be {@code null}.
			 * @param unit The unit.
			 */
			Writer(Buffer buffer, IUnit unit) {
				super(40, buffer);
				this.unit = unit;
				ptr = 0;
				length = 0;
			}
			
			/**
			 * Writes the data contained in the specified buffer to the VL file
			 * space at the position returned by the {@code VLFileSpace.allocate}
			 * method.
			 * <p>
			 * Note that the {@code VLFileSpace.allocate} method returns memory
			 * blocks that subsequently follow each other without a gap.
			 * 
			 * @param  buf The buffer.
			 * 
			 * @throws MaximumException If this writer wants to save the buffer
			 *         beyond the maximum allowed position.
			 * @throws UnitBrokenException If recording before data fails.
			 * @throws FileIOException If an I/O error occurs.
			 */
			@Override
			protected final void save(ByteBuffer buf) throws MaximumException,
														UnitBrokenException, FileIOException {
				final int n = buf.limit();
				if (ptr == 0) {
					final long filePos = vlFileSpace.allocate(n, unit);
					checkPos(filePos);
					ptr = filePos;
					vlDataFile.position(filePos);
				}
				else {
					vlFileSpace.allocate(n, unit);
				}
				vlDataFile.write(buf);
				length += n;
			}
			
			/**
			 * Feeds the writer with the specified data.
			 * 
			 * @param  data The data.
			 * 
			 * @throws NullPointerException If {@code data} is {@code null}.
			 * @throws MaximumException If this writer wants to save the buffer
			 *         beyond the maximum allowed position.
			 * @throws UnitBrokenException If recording before data fails.
			 * @throws FileIOException If an I/O error occurs.
			 */
			final void feed(byte[] data) throws NullPointerException,
								MaximumException, UnitBrokenException, FileIOException {
				write(data, 0, data.length);
			}
			
			/**
			 * Returns the total number of bytes written so far to the VL file
			 * space by this writer.
			 * Invoked immediately after a call to the {@code flush} method this
			 * method returns the sum of the lengths of any data so far provided
			 * to this writer via its {@code feed} method.
			 * 
			 * @return The total length of data written so far to the VL file
			 *         space.
			 */
			final long length() {
				return length;
			}
			
			/**
			 * Returns the internal buffer of this writer.
			 * 
			 * @return The internal buffer, never {@code null}.
			 */
			final ByteBuffer buf() {
				return buf;
			}
			
			/**
			 * Returns the file position of the first memory block written to the
			 * VL file space.
			 * 
			 * @return The position of the first written memory block or 0 if no
			 *         memory block was written yet to the VL file space.
			 */
			final long ptr() {
				return ptr;
			}
		}
		
		/**
		 * Computes the <em>null info bitmap</em> for the specified object array.
		 * <p>
		 * Let us view the computed array of bytes {@code a} from left to right,
		 * each byte consisting of 8 bits, like this:
		 * 
		 * <pre>
		 * 0 1 2 3 4 5 6 7  0 1 2 3 4 5 6 7  ... 0 1 2 3 4 5 6 7
		 * |    a[0]     |  |    a[1]     |      |   a[n-1]    |</pre>
		 * 
		 * where {@code n} is equal to the value of {@code
		 * Utils.bmLength(arr.length)}.
		 * Bit {@code i} (0 &le; {@code i} &le; 7) of {@code a[k]} (0 &le;
		 * {@code k} &le; {@code n}) is equal to 1 if and only if {@code
		 * arr[k*8+i]} is  equal to {@code null}.
		 * 
		 * @param  arr The object array.
		 * 
		 * @return The null info bitmap.
		 */
		private final byte[] computeNullInfo(Object[] arr) {
			final byte[] bm = new byte[Utils.bmLength(arr.length)];
			
			byte bmByte = 0;
			byte mask = 1;
			int bmIndex = 0;
			for (Object el : arr) {
				if (mask == 0) {
					bm[bmIndex] = bmByte;
					bmByte = 0;
					mask = 1;
					bmIndex++;
				}
				if (el == null) {
					bmByte |= mask;
				}
				mask <<= 1;
				// bmIndex == k / 8, k being the index of el in arr
			}
			// bmIndex == (arr.length - 1) / 8
			// Utils.bmLength(arr.length) == (arr.length - 1) / 8 + 1
			// bmIndex == Utils.bmLength(arr.length) - 1;
			bm[bmIndex] = bmByte;
			
			return bm;
		}
		
		/**
		 * Finalizes the conversion of a non-trivial array value initiated by the
		 * {@link #nonTrivialOtherAAT} method.
		 * <p>
		 * Updates the {@link #ptrLength} global property with the length of and
		 * the pointer to the byte representation of the non-trivial array value.
		 * <p>
		 * This method assumes that the {@link #ptrLength} global property points
		 * to the byte representation of the stored array value, provided that
		 * a stored array values exists.
		 * Otherwise, the pointer and the length of {@code ptrLength} are assumed
		 * to be both set to zero.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB2},
		 * {@linkplain WRStore.GlobalBuffer GB3}.
		 * 
		 * @param  wr The writer fed with the byte representation of the
		 *         non-trivial array value, not allowed to be {@code null}.
		 * @param  unit The unit.
		 * 
		 * @throws MaximumException If a new memory block in the VL file space
		 *         must be allocated and its file position exceeds the maximum
		 *         allowed position.
		 * @throws UnitBrokenException If recording before data fails.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void finalize(Writer wr, IUnit unit) throws
								MaximumException, UnitBrokenException, FileIOException {
			// The writer is fed with all data. Find out if the VL file space
			// allocated for the currently stored array value can be reused.
			final ByteBuffer buf = wr.buf();
			final int len = buf.position();
			final long len0 = ptrLength.length;
			if (wr.ptr() == 0 && len <= len0) {
				// The writer has not yet been written data to the VL file space
				// and the length of the new outrow data is less than or equal to
				// the length of the stored outrow data.
				// Reuse file space! Note that len > 0!
				buf.flip();
				replaceVLData(buf, unit);
			}
			else {
				// No reuse of VL file space.
				wr.flush();
				final long ptr = wr.ptr();
				if (len0 > 0) {
					vlFileSpace.deallocate(len0, unit);
				}
				setPtrLength(ptr, wr.length());
			}
		}

		/**
		 * Converts the specified array value which is assumed to be neither
		 * {@code null} nor empty and updates the {@link #ptrLength} global
		 * property with the length of and the pointer to the byte representation
		 * of the specified array value.
		 * <p>
		 * This method assumes that the {@link #ptrLength} global property points
		 * to the byte representation of the stored array value, provided that
		 * a stored array values exists.
		 * Otherwise, the pointer and the length of {@code ptrLength} are assumed
		 * to be both set to zero.
		 * <p>
		 * This method follows the storage scheme of an outrow array type or of
		 * an INROW A[OUTROW ST].
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB2},
		 * {@linkplain WRStore.GlobalBuffer GB3}.
		 * 
		 * @param  arr The array value, not allowed to be {@code null} and assumed
		 *         to contain at least one element.
		 * @param  unit The unit.
		 * 
		 * @throws NullPointerException If an element of {@code arr} is equal to
		 *         {@code null} but the element type forbids the {@code null}
		 *         value.
		 * @throws IllegalArgumentException If the length of the byte
		 *         representation of at least one of the elements exceeds the
		 *         maximum length allowed by the element type.
		 * @throws MaximumException If a new memory block in the VL file space
		 *         must be allocated and its file position exceeds the maximum
		 *         allowed position.
		 * @throws CryptoException If encryption fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 * @throws UnitBrokenException If recording before data fails.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void nonTrivialOtherAAT(Object[] arr, IUnit unit) throws
					NullPointerException, IllegalArgumentException, MaximumException,
								CryptoException, UnitBrokenException, FileIOException {
			// Create the writer.
			final Writer wr = new Writer(store.gb3, unit);
			
			// Feed the writer with the size of the array.
			wr.feed(Utils.unsToBytes(arr.length, sizeLen));

			// Create Null-info.
			if (stNullable) {
				// Feed the writer with the Null-info.
				wr.feed(computeNullInfo(arr));
			}

			// Feed the writer with converted elements.
			for (Object el : arr) {
				if (el != null) {
					// Convert the element to a byte array.
					final byte[] vlData;
					if (isSTOutrow) {
						vlData = stc.toBytes(el, st);
						// Send the length of the byte array to the writer.
						wr.feed(Utils.unsToBytes(vlData.length, stLen));
					}
					else {
						vlData = new byte[stLen];
						stc.toBytes(el, st, vlData, 0);
					}
					// Feed the writer with the byte array.
					wr.feed(vlData);
				}
				else if (!stNullable) {
					// Element is null but element type forbids null.
					// Array value obviously not compatible with array type.
					throw new NullPointerException(ACDPException.prefix(table,
									ci.col) + "An element of the array is null but " +
									"the element type does not allow the null value.");				
				}
			}
			// The writer is fed with all data.
			finalize(wr, unit);
		}
		
		/**
		 * Converts the specified array value which is assumed to be compatible
		 * with the array column type passed via the constructor of this class.
		 * <p>
		 * This method follows the storage scheme of an outrow array type or of
		 * an INROW A[OUTROW ST].
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB2},
		 * {@linkplain WRStore.GlobalBuffer GB3}.
		 * 
		 * @param  arr The array value, may be {@code null} and may be empty.
		 * @param  bag0 The bag containing the length of and the pointer to the
		 *         byte representation of the stored array value.
		 *         If this value is {@code null} then this method assumes that
		 *         {@code bag.bytes} is filled with zeros over the entire length
		 *         of the FL column data.
		 * @param  unit The unit.
		 * @param  bag The bag into which this method puts the length of and the
		 *         pointer to the byte representation of the specified array
		 *         value.
		 * 
		 * @throws NullPointerException If an element of {@code arr} is equal to
		 *         {@code null} but the element type forbids the {@code null}
		 *         value.
		 * @throws IllegalArgumentException If the length of the byte
		 *         representation of at least one of the elements exceeds the
		 *         maximum length allowed by the element type.
		 * @throws MaximumException If a new memory block in the VL file space
		 *         must be allocated and its file position exceeds the maximum
		 *         allowed position.
		 * @throws CryptoException If encryption fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 * @throws UnitBrokenException If recording before data fails.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void convOtherAAT(Object[] arr, Bag bag0, IUnit unit,
																					Bag bag) throws
					NullPointerException, IllegalArgumentException, MaximumException,
								CryptoException, UnitBrokenException, FileIOException {
			if (arr == null || arr.length == 0)
				trivialOtherAAT(arr, bag0, unit, bag);
			else {
				// arr.length > 0
				if (bag0 == null)
					setPtrLength(0, 0);
				else {
					// Updater operation.
					setPtrLength(bag0.bytes, bag0.offset, lengthLen);
				}
				nonTrivialOtherAAT(arr, unit);
				putPtrLength(lengthLen, bag.bytes, bag.offset);
			}
		}
		
		/**
		 * Puts the specified row index into the specified map.
		 * 
		 * @param rowIndex the row index.
		 * @param incVal the incrementation value.
		 * @param incDecMap the map.
		 */
		private final void putRowIndex(Long rowIndex, int incVal,
																Map<Long, Integer> incDecMap) {
			Integer val = incDecMap.get(rowIndex);
			if (val == null)
				incDecMap.put(rowIndex, incVal);
			else {
				incDecMap.put(rowIndex, val + incVal);
			}
		}
		
		/**
		 * Updates the reference counters of the referenced rows taking into
		 * account the stored array of references and the new array of references.
		 * <p>
		 * Note that the same reference may occur several times in either the
		 * stored or the new array of references and, of course, a reference may
		 * occur in both arrays at the same time.
		 * 
		 * @param  arr The new array of references, may be {@code null} and may
		 *         be empty.
		 * @param  sr The streamer delivering the byte representations of the
		 *         stored references, may be {@code null}.
		 * @param  unit The unit.
		 * 
		 * @throws IllegalArgumentException If at least one of the new references
		 *         points to a row that does not exist within the referenced
		 *         table or if the reference points to a row gap.
		 * @throws MaximumException If the maximum value of the reference counter
		 *         of a referenced row is exceeded.
		 * @throws UnitBrokenException If recording before data fails.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void incDecRefs(Object[] arr, IStreamer sr,
																				IUnit unit) throws
												IllegalArgumentException, MaximumException,
														UnitBrokenException, FileIOException {
			final WRStore refdStore = ci.refdStore;
			
			// Map: rowIndex -> increment.
			final Map<Long, Integer> incDecMap = new HashMap<>();
			
			// Stored array value.
			if (sr != null) {
				final Bag bag = new Bag();
				sr.pull(sizeLen, bag);
				final int size0 = (int) Utils.unsFromBytes(bag.bytes, bag.offset,
																							sizeLen);
				final int len = refdStore.nobsRowRef;
				for (int k = 0; k < size0; k++) {
					sr.pull(len, bag);
					final Long ri = Utils.unsFromBytes(bag.bytes, bag.offset, len);
					if (ri != 0) {
						putRowIndex(ri, -1, incDecMap);
					}
				}
			}
			
			// New array value
			if (arr != null) {
				for (Object e : arr) {
					final Ref_ ref = (Ref_) e;
					if (ref != null) {
						putRowIndex(ref.rowIndex(), 1, incDecMap);
					}
				}
			}
			
			// Update reference counters of referenced rows.
			for (Entry<Long, Integer> entry : incDecMap.entrySet()) {
				final int n = entry.getValue();
				if (n != 0) {
					GenericWriteOp.inc(refdStore, entry.getKey(), n, unit);
				}
			}
		}
		
		/**
		 * Updates the reference counters of the referenced rows.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB2}.
		 * 
		 * @param  arr The new array of references, may be {@code null} and may
		 *         be empty.
		 * @param  bitmap The bitmap of the row.
		 * @param  bag0 The bag containing the stored FL column data, not used if
		 *         equal to {@code null}.
		 * @param  unit The unit.
		 * 
		 * @throws IllegalArgumentException If at least one of the new references
		 *         points to a row that does not exist within the referenced
		 *         table or if the reference points to a row gap.
		 * @throws MaximumException If the maximum value of the reference counter
		 *         of a referenced row is exceeded.
		 * @throws UnitBrokenException If recording before data fails.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void incDecRefsAofRT(Object[] arr, long bitmap, Bag bag0,
																				IUnit unit) throws
												IllegalArgumentException, MaximumException,
														UnitBrokenException, FileIOException {
			// sr == null is fine for the following cases:
			//    1. bag0 == null
			//    2. Stored array of references is equal to null.
			//    3. Stored array of references is empty.
			IStreamer sr = null;
			if (bag0 != null) {
				// Updater operation.
				if (isAATInrow) {
					if ((bitmap & nullBitMask) == 0) {
						// Stored array value not null.
						sr = new ArrayStreamer(bag0.bytes, bag0.offset);
					}
				}
				else { // OUTROW
					setPtrLength(bag0.bytes, bag0.offset, lengthLen);
					final long length0 = ptrLength.length;
					if (length0 > 0) {
						// Stored array value is neither null nor empty.
						sr = new FileStreamer(vlDataFile, ptrLength.ptr, length0,
																					store.gb2, true);
					}
				}
			}
			
			// Do the job.
			incDecRefs(arr, sr, unit);
		}
		
		/**
		 * Replaces all elements in the specified array of references that are
		 * equal to {@code null} with the null-reference, hence, with the object
		 * {@code Ref_.NULL_REF}.
		 * 
		 * @param arr The array of references.
		 */
		private final void replaceNull(Object[] arr) {
			for (int i = 0; i < arr.length; i++) {
				if (arr[i] == null) {
					arr[i] = Ref_.NULL_REF;
				}
			}
		}

		/**
		 * Converts the specified value which is assumed to be compatible with
		 * the array column type passed via the constructor of this class.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB2},
		 * {@linkplain WRStore.GlobalBuffer GB3}.
		 * 
		 * @param  val The value, may be {@code null}.
		 *         The value must be an instance of {@code Object[]}, hence, it
		 *         must be an array value.
		 * @param  bitmap The bitmap of the row.
		 * @param  bag0 The bag containing the stored FL column data.
		 *         If this value is {@code null} then this method assumes that
		 *         {@code bag.bytes} is filled with zeros over the entire length
		 *         of the FL column data.
		 * @param  unit The unit.
		 * @param  bag The bag into which this method puts the new FL column data.
		 *         
		 * @return The resulting bitmap of the row.
		 * 
		 * @throws NullPointerException If an element of the specified array
		 *         value is equal to {@code null} but the element type of the
		 *         array column type forbids the {@code null} value.
		 *         This can only occur if the column type is an A[ST].
		 * @throws IllegalArgumentException If the size of the specified array
		 *         value exceeds the maximum size allowed by the array column type
		 *         or if the length of the byte representation of at least one of
		 *         the elements exceeds the maximum length allowed by the element
		 *         type or if the specified array value is an array of references
		 *         and at least one of the references points to a row that does
		 *         not exist within the referenced table or if the reference
		 *         points to a row gap.
		 * @throws MaximumException If a new memory block in the VL file space
		 *         must be allocated and its file position exceeds the maximum
		 *         allowed position or if the maximum value of the reference
		 *         counter of a referenced row is exceeded.
		 * @throws CryptoException If encryption fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 * @throws UnitBrokenException If recording before data fails.
		 * @throws FileIOException If an I/O error occurs.
		 */
		@Override
		public final long convert(Object val, long bitmap, Bag bag0, IUnit unit,
																					Bag bag) throws
					NullPointerException, IllegalArgumentException, MaximumException,
								CryptoException, UnitBrokenException, FileIOException {
			final Object[] arr = (Object[]) val;
			if (arr != null) {
				if (arr.length > aat.maxSize()) {
					// Precondition implies arr.length <= aat.maxSize() because we
					// assume val to be compatible with the array column type passed
					// via the constructor.
					// Nevertheless this is double-checked here to prevent corruption
					// of the file format.
					throw new IllegalArgumentException(ACDPException.prefix(table,
												ci.col) + "Size of array value (" +
												arr.length + ") exceeds maximum size (" +
												aat.maxSize() + ").");
				}
			}
			
			if (isArrayOfRefType && (bag0 != null || arr != null)) {
				// Update the reference counters of the referenced rows.
				incDecRefsAofRT(arr, bitmap, bag0, unit);
				// Replace null with Ref_.NULL_REF.
				if (arr != null) {
					replaceNull(arr);
				}
			}
			
			if (isInAATIn)
				bitmap = convInAATIn(arr, bitmap, bag0, bag);
			else {
				convOtherAAT(arr, bag0, unit, bag);
			}
			
			return bitmap;
		}
	}
}
