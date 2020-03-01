/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;

import acdp.design.SimpleType;
import acdp.exceptions.CryptoException;
import acdp.internal.Buffer;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.Ref_;
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
 * Defines a converter that converts the FL data of a value of a given column
 * type to an instance of {@code Object}.
 * <p>
 * By invoking the {@link #create} method, a {@link WRStore} creates for each
 * column of the table an {@link IBytesToObject} instance which in turn is used
 * by the get and iterator operations or by a reading maintenance operation.
 * <p>
 * The conversion may involve reading the byte representation of the value
 * from the VL data file.
 * <p>
 * The {@link ObjectToBytes} class represents the counterpart to this class.
 *
 * @author Beat Hoermann
 */
final class BytesToObject {
	
	/**
	 * Defines the {@link #convert} method which converts the FL data of a value
	 * of a given column type as stored in the FL data file to an instance of
	 * {@code Object}.
	 * <p>
	 * The conversion may involve reading the byte representation of the value
	 * from the VL data file.
	 * <p>
	 * The {@link ObjectToBytes.IObjectToBytes IObjectToBytes} interface
	 * represents the counterpart to this interface.
	 *
	 * @author Beat Hoermann
	 */
	static interface IBytesToObject {
		/**
		 * Converts the specified FL data to an instance of {@code Object}.
		 * <p>
		 * This method reads the byte representation from the VL data file,
		 * provided that the column type has an outrow storage scheme or is an
		 * INROW A[OUTROW ST].
		 *
		 * @param  bitmap The bitmap of the row.
		 * @param  bag The bag containing the FL column data, not allowed to be
		 *         {@code null}.
		 * 
		 * @return The converted value, may be {@code null}.
		 *
		 * @throws CryptoException If decrypting the byte array fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption or if the column type is an RT or A[RT].
		 * @throws FileIOException If an I/O error occurs.
		 *         This exception never happens if the column type has an inrow
		 *         storage scheme but is not an INROW A[OUTROW ST].
		 */
		Object convert(long bitmap, Bag bag) throws CryptoException,
																					FileIOException;
	}
	
	/**
	 * The simple type bytes to object converter converts the byte representation
	 * of an ST value to an instance of {@code Object}.
	 * Rather than invoking the conversion methods of the {@code SimpleType}
	 * class directly, the conversion methods of this class take into account
	 * that the WR database may apply encryption.
	 *
	 * @author Beat Hoermann
	 */
	private static abstract class STtoObject {
		
		/**
		 * A concrete implementation of a simple type bytes to object converter
		 * for a WR database that applies encryption.
		 * Note that encryption is not applied to a value of a {@link RefST}
		 * simple type.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class CryptoSTtoObject extends STtoObject {
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
			private CryptoSTtoObject(WRCrypto wrCrypto) {
				this.wrCrypto = wrCrypto;
			}
			
			@Override
			final <T> T toObject(byte[] bytes, int offset,
												SimpleType<T> st) throws CryptoException {
				if (!(st instanceof RefST)) {
					wrCrypto.decrypt(bytes, offset, st.length());
				}
				return st.convertFromBytes(bytes, offset);
			}
			
			@Override
			final <T> T toObject(byte[] bytes, int offset, int len,
												SimpleType<T> st) throws CryptoException {
				if (!(st instanceof RefST)) {
					wrCrypto.decrypt(bytes, offset, len);
				}
				return st.convertFromBytes(bytes, offset, len);
			}
		}
		
		/**
		 * A concrete implementation of a simple type bytes to object converter
		 * for a WR database that does not apply encryption.
		 * This atually boils down to an implementation that directly invokes the
		 * conversion methods of the {@code SimpleType} class.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class PlainSTtoObject extends STtoObject {
			@Override
			final <T> T toObject(byte[] bytes, int offset, SimpleType<T> st) {
				return st.convertFromBytes(bytes, offset);
			}
			
			@Override
			final <T> T toObject(byte[] bytes, int offset, int len,
																				SimpleType<T> st) {
				return st.convertFromBytes(bytes, offset, len);
			}
		}
		
		/**
		 * Creates a simple type bytes to object converter.
		 * 
		 * @param  wrCrypto The WR crypto object from the WR database.
		 *         This value is {@code null} if and only if the WR database does
		 *         not apply encryption.
		 *         
		 * @return The created simple type bytes to object converter.
		 */
		static final STtoObject newInstance(WRCrypto wrCrypto) {
			if (wrCrypto != null)
				return new CryptoSTtoObject(wrCrypto);
			else {
				return new PlainSTtoObject();
			}
		}
		
		/**
		 * Converts the specified byte representation of an ST value to an
		 * instance of {@code Object}.
		 * <p>
		 * This method should only be invoked if the specified simple type has an
		 * inrow storage scheme.
		 * 
		 * @param <T> The type of the returned object.
		 * 
		 * @param  bytes The byte array containing the byte representation of the
		 *         value, not allowed to be {@code null}.
		 * @param  offset The index of the first byte to convert.
		 * @param  st The simple type, not allowed to be {@code null}.
		 *         
		 * @return The resulting object.
		 * 
		 * @throws CryptoException If decrypting the byte array fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 */
		abstract <T> T toObject(byte[] bytes, int offset, SimpleType<T> st) throws
																					CryptoException;
		
		/**
		 * Converts the specified byte representation of an ST value to an
		 * instance of {@code Object}.
		 * <p>
		 * This method should be invoked only if the specified simple type has an
		 * outrow storage scheme.
		 * 
		 * @param <T> The type of the returned object.
		 * 
		 * @param  bytes The byte array containing the byte representation of the
		 *         value, not allowed to be {@code null}.
		 * @param  offset The index of the first byte to convert.
		 * @param  len The number of bytes to convert.
		 * @param  st The simple type, not allowed to be {@code null}.
		 *         
		 * @return The resulting object.
		 * 
		 * @throws CryptoException If decrypting the byte array fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 */
		abstract <T> T toObject(byte[] bytes, int offset, int len,
													SimpleType<T> st) throws CryptoException;
	}
	
	/**
	 * The store's VL data file, never {@code null}.
	 */
	private final FileIO vlDataFile;
	/**
	 * The simple type converter, never {@code null}.
	 */
	private final STtoObject stc;
	/**
	 * The number of bytes required for referencing any outrow data from a row.
	 */
	private final int nobsOutrowPtr;
	
	/**
	 * The constructor.
	 * <p>
	 * The database may or may not be {@linkplain acdp.internal.Database_#isWritable
	 * writable}.
	 * 
	 * @param store The WR store of a table, not allowed to be {@code null}.
	 */
	BytesToObject(WRStore store) {
		vlDataFile = store.vlDataFile;
		stc = STtoObject.newInstance(store.table.db().wrCrypto());
		nobsOutrowPtr = store.nobsOutrowPtr;
	}
	
	/**
	 * Depending on the specified column this factory method creates and returns
	 * a new instance of a class implementing the {@code IBytesToObject}
	 * interface.
	 * 
	 * @param  ci The column info object, not allowed to be {@code null}.
	 * 
	 * @return The created instance of {@code IBytesToObject}, never {@code
	 *         null}.
	 */
	final IBytesToObject create(WRColInfo ci) {
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
	private final class STIn implements IBytesToObject {
		private final SimpleType<?> st;
		private final long nullBitMask;
		
		/**
		 * The constructor.
		 * 
		 * @param  st The simple type assumed to have an <em>inrow</em> storage
		 *         scheme, not allowed to be {@code null}.
		 * @param  ci The column info object, not allowed to be {@code null}.
		 */
		STIn(SimpleType<?> st, WRColInfo ci) {
			this.st = st;
			nullBitMask = ci.nullBitMask;
		}
		
		/**
		 * Converts the specified FL data which is assumed to originate from a
		 * column with a type equal to the type passed via the constructor of
		 * this class.
		 * 
		 * @param  bitmap The bitmap of the row.
		 * @param  bag The bag containing the FL column data, not allowed to be
		 *         {@code null}.
		 *         
		 * @return The resulting value, may be {@code null}.
		 *
		 * @throws CryptoException If decrypting the byte array fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 */
		@Override
		public final Object convert(long bitmap, Bag bag) throws CryptoException {
			if ((bitmap & nullBitMask) !=  0)
				return null;
			else {
				return stc.toObject(bag.bytes, bag.offset, st);
			}
		}
	}

	/**
	 * The converter for an OUTROW ST column.
	 *
	 * @author Beat Hoermann
	 */
	private final class STOut implements IBytesToObject {
		private final SimpleType<?> st;
		private final int len;
		private final int lengthLen;
		
		/**
		 * The constructor.
		 * 
		 * @param  st The simple type assumed to have an <em>outrow</em> storage
		 *         scheme, not allowed to be {@code null}.
		 * @param  ci The column info object, not allowed to be {@code null}.
		 */
		STOut(SimpleType<?> st, WRColInfo ci) {
			this.st = st;
			len = ci.len;
			lengthLen= ci.lengthLen;
		}

		/**
		 * Reads {@code len} bytes from the VL data file at the specified
		 * position.
		 * <p>
		 * This method does not change the file channel's internal position.
		 * 
		 * @param  pos The position within the file where to read the data from.
		 * @param  len The number of bytes to read.
		 * 
		 * @return The data having a length of {@code len} bytes, never {@code
		 *         null}.
		 * 
		 * @throws IllegalArgumentException If {@code pos} or {@code len} is
		 *         negative.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final byte[] readData(long pos, int len) throws
												IllegalArgumentException, FileIOException {
			final ByteBuffer buf = ByteBuffer.allocate(len);
			vlDataFile.read_(buf, pos);
			return buf.array();
		}
		
		/**
		 * Converts the specified FL data which is assumed to originate from a
		 * column with a type equal to the type passed via the constructor of
		 * this class.
		 * 
		 * @param  bitmap The bitmap of the row.
		 * @param  bag The bag containing the FL column data, not allowed to be
		 *         {@code null}.
		 *         
		 * @return The resulting value, may be {@code null}.
		 *
		 * @throws CryptoException If decrypting the byte array fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 * @throws FileIOException If an I/O error occurs.
		 */
		@Override
		public final Object convert(long bitmap, Bag bag) throws CryptoException,
																					FileIOException {
			final byte[] bytes = bag.bytes;
			final int offset = bag.offset;
			if (Utils.isZero(bytes, offset, len))
				return null;
			else {
				final int length = (int) Utils.unsFromBytes(bytes, offset,
																						lengthLen);
				return stc.toObject(length == 0 ? new byte[0] :
								readData(Utils.unsFromBytes(bytes, offset + lengthLen,
													nobsOutrowPtr), length), 0, length, st);
			}
		}
	}
	
	/**
	 * The converter for an RT column.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class RT implements IBytesToObject {
		private final int len;
		
		/**
		 * The constructor.
		 * 
		 * @param  ci The column info object, not allowed to be {@code null}.
		 *         The column must be an RT column.
		 */
		RT(WRColInfo ci) {
			len = ci.len;
		}
		
		/**
		 * Converts the specified FL data which is assumed to originate from a
		 * column with a type equal to the type passed via the constructor of
		 * this class.
		 * 
		 * @param  bitmap The bitmap of the row.
		 * @param  bag The bag containing the FL column data, not allowed to be
		 *         {@code null}.
		 *         
		 * @return The resulting reference, may be {@code null}.
		 */
		@Override
		public final Object convert(long bitmap, Bag bag) {
			final byte[] bytes = bag.bytes;
			final int offset = bag.offset;
			
			return Utils.isZero(bytes, offset, len) ? null : new Ref_(
													Utils.unsFromBytes(bytes, offset, len));
		}
	}
	
	/**
	 * The converter for a column being of an array column type.
	 *
	 * @author Beat Hoermann
	 */
	private final class AAT implements IBytesToObject {
		private final long nullBitMask;
		private final int len;
		private final int sizeLen;
		private final int lengthLen;
		private final SimpleType<?> st;
		private final boolean stNullable;
		private final int stLen;
		private final Class<?> stTypeObj;
		private final boolean stInrow;
		private final boolean isInAATIn;

		/**
		 * The constructor.
		 * 
		 * @param  aat The array column type, not allowed to be {@code null}.
		 * @param  ci The column info object, not allowed to be {@code null}.
		 */
		AAT(AbstractArrayType aat, WRColInfo ci) {
			nullBitMask = ci.nullBitMask;
			len = ci.len;
			sizeLen = ci.sizeLen;
			lengthLen = ci.lengthLen;
			st = aat instanceof ArrayOfRefType_ ?
												RefST.newInstance(ci.refdStore.nobsRowRef) :
												((ArrayType_) aat).elementType();
			stNullable = st.nullable();
			stLen = st.length();
			stTypeObj = st.valueType();
			stInrow = st.scheme() == Scheme.INROW;
			isInAATIn = aat.scheme() == Scheme.INROW && stInrow;
		}
		
		/**
		 * Converts the specified FL column data which is assumed to originate
		 * from a column with a type equal to the type passed via the constructor
		 * of this class.
		 * <p>
		 * This method follows the storage scheme of an INROW A[INROW ST/RT].
		 * 
		 * @param  bitmap The bitmap of the row.
		 * @param  bag The bag containing the FL column data, not allowed to be
		 *         {@code null}.
		 *         
		 * @return The resulting array value, may be {@code null} and may be an
		 *         empty array.
		 * 
		 * @throws CryptoException If decrypting the byte array fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 */
		private final Object[] convInAATIn(long bitmap, Bag bag) throws
																					CryptoException {
			if ((bitmap & nullBitMask) !=  0)
				return null;
			else {
				final byte[] bytes = bag.bytes;
				final int offset = bag.offset;
				final int size = (int) Utils.unsFromBytes(bytes, offset, sizeLen);
				final Object[] val = (Object[]) Array.newInstance(stTypeObj, size);
				if (size == 0)
					return val;
				else {
					// size > 0
					int index = offset + sizeLen;
			
					if (stNullable) {
						final byte[] ni = bytes;
						int niIndex = index;
						index += Utils.bmLength(size);
					
						byte niByte = ni[niIndex];
						byte mask = 1;
						for (int i = 0; i < size; i++) {
							if (mask == 0) {
								niByte = ni[++niIndex];
								mask = 1;
							}
							if ((niByte & mask) != 0)
								val[i] = null;
							else {
								val[i] = stc.toObject(bytes, index, st);
								index += stLen;
							}
							mask <<= 1;
						}
					}
					else {
						for (int i = 0; i < size; i++) {
							val[i] = stc.toObject(bytes, index, st);
							index += stLen;
						}
					}
					return val;
				}
			}
		}
		
		/**
		 * Pulls the byte representation of the next ST element from the
		 * specified streamer and converts it to an instance of {@code Object}.
		 * 
		 * @param  sr The streamer.
		 * @param  bag A bag, not allowed to be {@code null}.
		 *         We don't want to create a new {@code Bag} instance at each
		 *         invocation of this method.
		 * 
		 * @return The resulting element.
		 * 
		 * @throws CryptoException If decrypting the byte array fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final Object element(IStreamer sr, Bag bag) throws
															CryptoException, FileIOException {
			sr.pull(stLen, bag);
			if (stInrow)
				return stc.toObject(bag.bytes, bag.offset, st);
			else {
				final int len = (int) Utils.unsFromBytes(bag.bytes, bag.offset,
																								stLen);
				sr.pull(len, bag);
				return stc.toObject(bag.bytes, bag.offset, len, st);
			}
		}
		
		/**
		 * Converts the specified FL column data which is assumed to originate
		 * from a column with a type equal to the type passed via the constructor
		 * of this class.
		 * <p>
		 * This method follows the storage scheme of an outrow array type or of
		 * an INROW A[OUTROW ST].
		 * 
		 * @param  bytes The byte array containing the FL column data, not allowed
		 *         to be {@code null}.
		 * @param  offset The index where the FL column data starts within the
		 *         byte array.
		 * 
		 * @return The resulting array value, never {@code null} and never empty.
		 * 
		 * @throws CryptoException If decrypting the byte array fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final Object[] nonTrivialOtherAAT(byte[] bytes, int offset) throws
															CryptoException, FileIOException {
			// Create the streamer.
			final IStreamer sr = new FileStreamer(vlDataFile, Utils.unsFromBytes(
											bytes, offset + lengthLen, nobsOutrowPtr),
											Utils.unsFromBytes(bytes, offset, lengthLen),
																			new Buffer(), false);
			final Bag bag = new Bag();
			// Get the size of the array and create the array.
			// The size is greater than zero by assumption.
			sr.pull(sizeLen, bag);
			final int size = (int) Utils.unsFromBytes(bag.bytes, bag.offset,
																							sizeLen);
			final Object[] val = (Object[]) Array.newInstance(stTypeObj, size);
			
			// Read and convert the individual elements.
			if (stNullable) {
				// Get the null info.
				final int niLen = Utils.bmLength(size);
				sr.pull(niLen, bag);
				final byte[] ni = Arrays.copyOfRange(bag.bytes, bag.offset,
																				bag.offset + niLen);
				int niIndex = 0;
				byte niByte = ni[niIndex];
				byte mask = 1;
				for (int i = 0; i < size; i++) {
					if (mask == 0) {
						niByte = ni[++niIndex];
						mask = 1;
					}
					if ((niByte & mask) != 0)
						val[i] = null;
					else {
						val[i] = element(sr, bag);
					}
					mask <<= 1;
				}
			}
			else {
				for (int i = 0; i < size; i++) {
					val[i] = element(sr, bag);
				}
			}
			return val;
		}
		
		/**
		 * Converts the specified FL column data which is assumed to originate
		 * from a column with a type equal to the type passed via the constructor
		 * of this class.
		 * <p>
		 * This method follows the storage scheme of an outrow array type or of
		 * an INROW A[OUTROW ST].
		 * 
		 * @param  bag The bag containing the FL column data, not allowed to be
		 *         {@code null}.
		 *         
		 * @return The resulting array value, may be {@code null} and may be an
		 *         empty array.
		 * 
		 * @throws CryptoException If decrypting the byte array fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final Object[] convOtherAAT(Bag bag) throws CryptoException,
																					FileIOException {
			final byte[] bytes = bag.bytes;
			final int offset = bag.offset;
			if (Utils.isZero(bytes, offset, len))
				return null;
			else if (Utils.isZero(bytes, offset, lengthLen))
				return (Object[]) Array.newInstance(stTypeObj, 0);
			else {
				// Array value has at least one element.
				return nonTrivialOtherAAT(bytes, offset);
			}
		}
		
		/**
		 * Converts the specified FL data which is assumed to originate from a
		 * column with a type equal to the type passed via the constructor of
		 * this class.
		 * 
		 * @param  bitmap The bitmap of the row.
		 * @param  bag The bag containing the FL column data, not allowed to be
		 *         {@code null}.
		 *         
		 * @return The resulting array value, may be {@code null} and may be an
		 *         empty array.
		 *
		 * @throws CryptoException If decrypting the byte array fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 * @throws FileIOException If an I/O error occurs.
		 *         This exception never happens if the column type is equal to an
		 *         inrow  array type with an inrow element type.
		 */
		@Override
		public final Object convert(long bitmap, Bag bag) throws CryptoException,
																					FileIOException {
			final Object[] val;
			if (isInAATIn)
				// INROW AbstractArrayType with INROW SimpleType
				val = convInAATIn(bitmap, bag);
			else {
				// Other AbstractArrayType
				val = convOtherAAT(bag);
			}

			return val;
		}
	}
}