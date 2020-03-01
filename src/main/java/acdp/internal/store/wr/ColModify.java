/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.nio.ByteBuffer;
import java.util.Arrays;

import acdp.Table.ValueChanger;
import acdp.design.SimpleType;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.MaximumException;
import acdp.internal.Column_;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.Table_;
import acdp.internal.misc.Utils_;
import acdp.internal.store.Bag;
import acdp.internal.store.wr.BytesToObject.IBytesToObject;
import acdp.internal.store.wr.FLFileAccommodate.Presenter;
import acdp.internal.store.wr.FLFileAccommodate.Spec;
import acdp.internal.store.wr.FLFileAccommodate.Updater;
import acdp.internal.store.wr.ObjectToBytes.IObjectToBytes;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.internal.types.AbstractArrayType;
import acdp.internal.types.ArrayOfRefType_;
import acdp.internal.types.ArrayType_;
import acdp.misc.Utils;
import acdp.types.Type;
import acdp.types.Type.Scheme;

/**
 * Provides the {@link #run} method which {@linkplain FLFileAccommodate
 * accommodates} the FL data file around a column to be modified.
 *
 * @author Beat Hoermann
 */
final class ColModify {
	/**
	 * Expands or contracts the Null-info by one bit and computes the adjusted
	 * bitmap.
	 * 
	 * @author Beat Hoermann
	 */
	private interface NiAdjuster {
		/**
		 * Returns the size of the adjusted bitmap in bytes.
		 * 
		 * @return The size of the adjusted bitmap in bytes, &gt; 0 and &le; 8.
		 */
		int nBM();
		
		/**
		 * Returns the adjusted bitmap of the current row.
		 * 
		 * @param  flDataBlock The byte array housing the current FL data block,
		 *         never {@code null}.
		 * @param  offBM The position within the byte array where the bitmap
		 *         starts.
		 * @param  isNull Indicates whether the current value is {@code null}.
		 *         The value must be set to {@code true} if and only if the
		 *         current value is {@code null}.
		 * 
		 * @return The adjusted bitmap of the current row.
		 */
		long bm(byte[] flDataBlock, int offBM, boolean isNull);
	}
	
	/**
	 * Contracts the Null-info by one bit and computes the adjusted bitmap.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class NiC implements NiAdjuster {
		private final int nBM;
		private final int nBM0;
		private final long mask0;
		private final long mask1;
		
		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci0 The column information object of the column to be modified
		 *        before modification, not allowed to be {@code null} and {@code
		 *        ci0.nullBitMask} not allowed to be zero.
		 */
		NiC(WRStore store, WRColInfo ci0) {
			// Compute the length of the Null-info.
			int niLen = 0;
			for (WRColInfo colInfo : store.colInfoArr) {
				if (colInfo.nullBitMask != 0) {
					niLen++;
				}
			}
			// niLen > 0 since ci0.nullBitMask != 0 by assumption.
			
			// The bitmap starts with the row gap marker bit followed by the
			// Null-info. Since the adjusted Null-info will be missing one bit,
			// the size of the adjusted bitmap will be one bit less.
			nBM = Utils.bmLength(niLen);
			
			nBM0 = store.nBM;
			
			// ci0.nullBitMask != 0;
			mask0 = ci0.nullBitMask - 1;
			mask1 = ~mask0;
		}
		
		@Override
		public final int nBM() {
			return nBM;
		}
		
		@Override
		public final long bm(byte[] flDataBlock, int offBM, boolean isNull) {
			final long bm0 = Utils.unsFromBytes(flDataBlock, offBM, nBM0);
			return ((bm0 >>> 1) & mask1) | (bm0 & mask0);
		}
	}
	
	/**
	 * Expands the Null-info by one bit and computes the adjusted bitmap.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class NiE implements NiAdjuster {
		private final int nBM;
		private final int nBM0;
		private final long nullBitMask1;
		private final long mask0;
		private final long mask1;
		
		/**
		 * The constructor.
		 * 
		 * @param  store The store, not allowed to be {@code null}.
		 * @param  ci0 The column information object of the column to be modified
		 *         before modification, not allowed to be {@code null} and {@code
		 *         ci0.nullBitMask} must be zero.
		 * 
		 * @throws ImplementationRestrictionException If the bitmap is too large
		 *         for being expanded.
		 */
		NiE(WRStore store, WRColInfo ci0) throws
														ImplementationRestrictionException {
			
			// Compute the length of the Null-info and the null bit mask of the
			// column with the highest index less than the index of the column
			// ci0. Note that ci0.nullBitMask == 0 by assumption.
			long l = -1;
			boolean stop = false;
			int niLen = 0;
			for (WRColInfo colInfo : store.colInfoArr) {
				if (colInfo.nullBitMask != 0) {
					niLen++;
				}
				stop = stop || ci0 == colInfo;
				if (!stop) {
					final long nbm = colInfo.nullBitMask;
					if (nbm != 0) {
						l = nbm;
					}
				}
			}
			
			if (niLen > 62) {
				throw new ImplementationRestrictionException(store.table,
									"Table has too many columns needing a separate " +
									"null information.");
			}

			// The bitmap starts with the row gap marker bit followed by the
			// Null-info. Since the adjusted Null-info will include one bit more,
			// the size of the adjusted bitmap will be one bit more.
			nBM = Utils.bmLength(niLen + 2);
			
			nBM0 = store.nBM;
			
			// Compute the null bit mask of the modified column.
			nullBitMask1 = l == -1 ? 1L : l << 1;
			
			mask0 = nullBitMask1 - 1;
			mask1 = ~mask0 << 1;
		}
		
		@Override
		public final int nBM() {
			return nBM;
		}
		
		@Override
		public final long bm(byte[] flDataBlock, int offBM, boolean isNull) {
			final long bm0 = Utils.unsFromBytes(flDataBlock, offBM, nBM0);
			final long bm = ((bm0 << 1) & mask1) | (bm0 & mask0);
			return isNull ? bm | nullBitMask1 : bm;
		}
	}
	
	
	/**
	 * The base class of all {@linkplain Presenter presenters}.
	 * 
	 * @author Beat Hoermann
	 */
	private static abstract class P implements Presenter {
		/**
		 * The byte array housing the current FL data block, never {@code null}.
		 * <p>
		 * This value should be considered as read-only.
		 */
		byte[] flDataBlock;
		/**
		 * The position within the byte array where the current FL data block
		 * starts.
		 * Since the FL data block starts with the bitmap, this is also the
		 * position where the bitmap starts.
		 * <p>
		 * This value should be considered as read-only.
		 */
		protected int offBM;
		/**
		 * The position within the byte array where the current FL column data
		 * starts.
		 * <p>
		 * This value should be considered as read-only.
		 */
		int offCD;
		/**
		 * Indicates whether the current value is {@code null}.
		 * The value is {@code true} if and only if the current value is {@code
		 * null}.
		 * <p>
		 * This value should be considered as read-only.
		 */
		boolean isNull;
		
		/**
		 * The position where the FL column data starts within the FL data block.
		 */
		private final int colOff;
		
		/**
		 * The constructor.
		 * 
		 * @param ci0 The column information object of the column to be modified
		 *        before modification, not allowed to be {@code null}.
		 */
		P(WRColInfo ci0) {
			colOff = ci0.offset;
		}
		
		/**
		 * Returns the information whether the current value is {@code null}.
		 * <p>
		 * Implementers <em>may</em> throw a {@code NullPointerException} if the
		 * current value is {@code null} but the target column type does not
		 * allow the {@code null} value.
		 * 
		 * @return The boolean value {@code true} if and only if the current
		 *         value is {@code null}.
		 *         
		 * @throws NullPointerException If the current value is {@code null} but
		 *         the target column type does not allow the {@code null} value.
		 * @throws FileIOException If an I/O error occurs.
		 */
		protected abstract boolean isNull() throws NullPointerException,
																					FileIOException;
		@Override
		public final void rowData(byte[] flDataBlock, int offset) throws
																					FileIOException {
			this.flDataBlock = flDataBlock;
			offBM = offset;
			offCD = offset + colOff;
			isNull = isNull();
		}
	}
	
	/**
	 * The presenter implementing the {@code isNull} method without converting
	 * the byte representation of the stored value to an object.
	 * <p>
	 * The three characters {@code SNO} stands for "scheme or nullable only".
	 * 
	 * @author Beat Hoermann
	 */
	private static class SNO_P extends P {
		/**
		 * The size of the bitmap in bytes, &gt; 0 and &le; 8.
		 */
		private final int nBM0;
		
		private final WRStore store;
		private final WRColInfo ci0;
		
		private final int len;
		private final long nullBitMask;
		private final boolean col0IsNonNullST;
		private final boolean col1IsNonNullST;
		
		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci0 The column information object of the column to be modified
		 *        before modification, not allowed to be {@code null}.
		 * @param t1 The target type of the column to be modified, not allowed to
		 *        be {@code null}.
		 */
		SNO_P(WRStore store, WRColInfo ci0, Type t1) {
			super(ci0);
			
			this.store = store;
			this.ci0 = ci0;
			nBM0 = store.nBM;
			len = ci0.len;
			nullBitMask = ci0.nullBitMask;
			
			final Type t0 = ci0.col.type();
			col0IsNonNullST = (t0 instanceof SimpleType) &&
															!((SimpleType<?>) t0).nullable();
			col1IsNonNullST = (t1 instanceof SimpleType) &&
															!((SimpleType<?>) t1).nullable();
		}
		
		@Override
		protected final boolean isNull() throws NullPointerException {
			final boolean isNull;
			if (col0IsNonNullST)
				// We assume that the integrity of the database is not violated.
				isNull = false;
			else {
				isNull = nullBitMask > 0 ? (nullBitMask &
									Utils.unsFromBytes(flDataBlock, offBM, nBM0)) != 0 :
														Utils.isZero(flDataBlock, offCD, len);
				if (isNull && col1IsNonNullST) {
					throw new NullPointerException(ACDPException.prefix(store.table,
								ci0.col) + "The value is null but the type of the " +
								"column to be modified does not allow the null value.");
				}
			}
			return isNull;
		}
	}
	
	/**
	 * Expands or contracts the Null-info by one bit, computes the modified
	 * bitmap and {@linkplain BMProvider provides} a bitmap {@linkplain Updater
	 * updater} with the ajusted bitmap.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class Ni_SNO_P extends SNO_P implements BMProvider {
		private final NiAdjuster niAdjuster;
		
		/**
		 * The constructor.
		 * 
		 * @param niAdjuster The Null-info modifier, not allowed to be {@code
		 *        null}.
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci0 The column information object of the column to be modified
		 *        before modification, not allowed to be {@code null}.
		 * @param t1 The target type of the column to be modified, not allowed to
		 *        be {@code null}.
		 */
		Ni_SNO_P(NiAdjuster niAdjuster, WRStore store, WRColInfo ci0, Type t1) {
			super(store, ci0, t1);
			this.niAdjuster = niAdjuster;
		}
		
		@Override
		public final int nBM() {
			return niAdjuster.nBM();
		}
		
		
		@Override
		public final long bm() {
			return niAdjuster.bm(flDataBlock, offBM, isNull);
		}
	}
	
	/**
	 * The presenter implementing the {@code isNull} method by converting the
	 * byte representation of the current value to an object.
	 * <p>
	 * Subclasses may access the current value {@code v0} which remains
	 * unconverted since it is assumed that the {@code valueChanger} argument of
	 * the {@link ColModify#run} method is {@code null}.
	 * Other clients may access {@code v0} via the {@link VProvider} interface.
	 * 
	 * @author Beat Hoermann
	 */
	private static class V0_P extends P implements VProvider {
		/**
		 * The current value, may be {@code null}.
		 * <p>
		 * This value should be considered as read-only.
		 */
		protected Object v0;
		
		private final WRStore store;
		private final WRColInfo ci0;
		private final int nBM0;
		private final IBytesToObject b2o;
		private final Bag bag;

		private final boolean col1IsNonNullST;
		
		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci0 The column information object of the column to be modified
		 *        before modification, not allowed to be {@code null}.
		 * @param t1 The target type of the column to be modified, not allowed to
		 *        be {@code null}.
		 */ 
		V0_P(WRStore store, WRColInfo ci0, Type t1) {
			super(ci0);
			this.store = store;
			this.ci0 = ci0;
			nBM0 = store.nBM;
			b2o = ci0.b2o;
			bag = new Bag();
			col1IsNonNullST = (t1 instanceof SimpleType) &&
															!((SimpleType<?>) t1).nullable();
		}

		@Override
		public final Object v() {
			return v0;
		}

		@Override
		protected final boolean isNull() throws NullPointerException,
															CryptoException, FileIOException {
			bag.bytes = flDataBlock;
			bag.offset = offCD;
			v0 = b2o.convert(Utils.unsFromBytes(flDataBlock, offBM, nBM0), bag);
			
			final boolean isNull = v0 == null;
			if (isNull && col1IsNonNullST) {
				throw new NullPointerException(ACDPException.prefix(store.table,
							ci0.col) + "The value is null but the type of the " +
							"column to be modified does not allow the null value.");
			}
			
			return isNull;
		}
	}
	
	/**
	 * Expands or contracts the Null-info by one bit, computes the modified
	 * bitmap and {@linkplain BMProvider provides} a bitmap {@linkplain Updater
	 * updater} with the adjusted bitmap.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class Ni_V0_P extends V0_P implements BMProvider {
		private final NiAdjuster niAdjuster;
		
		/**
		 * The constructor.
		 * 
		 * @param niAdjuster The Null-info modifier, not allowed to be {@code
		 *        null}.
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci0 The column information object of the column to be modified
		 *        before modification, not allowed to be {@code null}.
		 * @param t1 The target type of the column to be modified, not allowed to
		 *        be {@code null}.
		 */
		Ni_V0_P(NiAdjuster niAdjuster, WRStore store, WRColInfo ci0, Type t1) {
			super(store, ci0, t1);
			this.niAdjuster = niAdjuster;
		}
		
		@Override
		public final int nBM() {
			return niAdjuster.nBM();
		}
		
		
		@Override
		public final long bm() {
			return niAdjuster.bm(flDataBlock, offBM, isNull);
		}
	}
	
	/**
	 * The presenter implementing the {@code isNull} method by converting the
	 * byte representation of the current value to an object and converting the
	 * object again with the value changer given by the {@code valueChanger}
	 * argument of the constructor.
	 * <p>
	 * Subclasses may access the stored current value {@code v0} as well as the
	 * converted current value {@code v1}.
	 * Other clients may access {@code v1} via the {@link VProvider} interface.
	 * 
	 * @author Beat Hoermann
	 */
	private static class V1_P extends P implements VProvider {
		/**
		 * The stored current value, may be {@code null}.
		 * <p>
		 * This value should be considered as read-only.
		 */
		protected Object v0;
		/**
		 * The converted current value, may be {@code null}.
		 * <p>
		 * This value should be considered as read-only.
		 */
		protected Object v1;
		
		private final ValueChanger<?> valueChanger;
		private final int nBM0;
		private final IBytesToObject b2o;
		private final Bag bag;
		
		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci0 The column information object of the column to be modified
		 *        before modification, not allowed to be {@code null}.
		 * @param valueChanger The value changer, not allowed to be {@code null}.
		 */ 
		V1_P(WRStore store, WRColInfo ci0, ValueChanger<?> valueChanger) {
			super(ci0);
			this.valueChanger = valueChanger;
			nBM0 = store.nBM;
			b2o = ci0.b2o;
			bag = new Bag();
		}

		@Override
		public final Object v() {
			return v1;
		}

		@Override
		protected final boolean isNull() throws CryptoException, FileIOException {
			bag.bytes = flDataBlock;
			bag.offset = offCD;
			v0 = b2o.convert(Utils.unsFromBytes(flDataBlock, offBM, nBM0), bag);
			v1 = valueChanger.changeValue(v0);
			// We do not check whether the column type allows v1 being null.
			
			return v1 == null;
		}
	}
	
	/**
	 * Expands or contracts the Null-info by one bit, computes the modified
	 * bitmap and {@linkplain BMProvider provides} a bitmap {@linkplain Updater
	 * updater} with the adjusted bitmap.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class Ni_V1_P extends V1_P implements BMProvider {
		private final NiAdjuster niAdjuster;
		
		/**
		 * The constructor.
		 * 
		 * @param niAdjuster The Null-info modifier, not allowed to be {@code
		 *        null}.
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci0 The column information object of the column to be modified
		 *        before modification, not allowed to be {@code null}.
		 * @param valueChanger The value changer, not allowed to be {@code null}.
		 */
		Ni_V1_P(NiAdjuster niAdjuster, WRStore store, WRColInfo ci0,
																ValueChanger<?> valueChanger) {
			super(store, ci0, valueChanger);
			this.niAdjuster = niAdjuster;
		}
		
		@Override
		public final int nBM() {
			return niAdjuster.nBM();
		}
		
		
		@Override
		public final long bm() {
			return niAdjuster.bm(flDataBlock, offBM, isNull);
		}
	}
	
	/**
	 * Sets the Null-info depending on whether the converted current value is
	 * {@code null} or is not {@code null} and {@linkplain BMProvider provides}
	 * a bitmap {@linkplain Updater updater} with the adjusted bitmap.
	 * <p>
	 * It is assumed the original and the modified column have a Null-info.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class NiSet_V1_P extends V1_P implements BMProvider {
		private final int nBM0;
		private final long nullBitMask;
		
		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci0 The column information object of the column to be modified
		 *        before modification, not allowed to be {@code null}.
		 * @param valueChanger The value changer, not allowed to be {@code null}.
		 */
		NiSet_V1_P(WRStore store, WRColInfo ci0, ValueChanger<?> valueChanger) {
			super(store, ci0, valueChanger);
			nBM0 = store.nBM;
			nullBitMask = ci0.nullBitMask;
			// Assumption nullBitMask != 0
		}
		
		@Override
		public final int nBM() {
			return nBM0;
		}
		
		@Override
		public final long bm() {
			final long bm0 = Utils.unsFromBytes(flDataBlock, offBM, nBM0);
			if (v0 == null && !isNull)
				return bm0 & ~nullBitMask;
			else if (v0 != null && isNull)
				return bm0 | nullBitMask;
			else {
				// v0 == null && v1 == null || v0 != null && v1 != null;
				return bm0;
			}
		}
	}
	
	/**
	 * Provides the size of the modified bitmap and the modified bitmap of the
	 * current row itself.
	 * 
	 * @author Beat Hoermann
	 */
	private interface BMProvider {
		/**
		 * Returns the size of the modified bitmap in bytes, &gt; 0 and &le; 8.
		 *
		 * @return The size of the modified bitmap of the current row.
		 */
		int nBM();

		/**
		 * Returns the modified bitmap of the current row.
		 * 
		 * @return The modified bitmap of the current row.
		 */
		long bm();
	}
	
	/**
	 * The bitmap updater updates the bitmap.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class BM_U extends Updater {
		private final BMProvider bmp;
		
		/**
		 * The constructor.
		 * 
		 * @param bmp The bitmap provider, not allowed to be {@code null}.
		 */
		BM_U(BMProvider bmp) {
			super(bmp.nBM());
			this.bmp = bmp;
		}

		@Override
		public final void newData(byte[] bytes, int offset) {
			Utils.unsToBytes(bmp.bm(), len, bytes, offset);
		}
	}
	
	/**
	 * The deallocation column data updater reads the byte representation of the
	 * non-null current value from the VL data file, copies it into the current
	 * column data and deallocates the memory block from the VL file space.
	 * <p>
	 * Does nothing if the current value is {@code null}.
	 * <p>
	 * The targeted column is an OUTROW ST, OUTROW A[INROW ST] or OUTROW A[RT]
	 * column that turns into an INROW ST, INROW A[INROW ST] or INROW A[RT] 
	 * column, respectively.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class Dealloc_U extends Updater {
		private final Table_ table;
		private final String colName;
		private final VLFileSpace vlFileSpace;
		private final FileIO vlDataFile;
		private final int lengthLen;
		private final int nobsOutrowPtr;
		private final P p;
		
		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci0 The column information object of the column to be modified,
		 *        not allowed to be {@code null}.
		 *        The column is assumed to be a VL column.
		 * @param ci1 The new column information object, not allowed to be {@code
		 *        null}.
		 *        The column is assumed not to be a VL column.
		 * @param p The presenter, not allowed to be {@code null}.
		 */
		Dealloc_U(WRStore store, WRColInfo ci0, WRColInfo ci1, P p) {
			super(ci1.len);
			table = store.table;
			colName = ci0.col.name();
			vlFileSpace = store.vlFileSpace;
			vlDataFile = store.vlDataFile;
			lengthLen = ci0.lengthLen;
			nobsOutrowPtr = store.nobsOutrowPtr;
			this.p = p;
		}
		
		@Override
		public final void newData(byte[] bytes, int offset) throws
									ImplementationRestrictionException, FileIOException {
			// p.isNull: This is coded in the Null-info, no need to care about the
			//           column data.
			if (!p.isNull) {
				final byte[] cd0 = p.flDataBlock;
				final int off0 = p.offCD;
				
				// Get length of outrow data.
				final long l = Utils.unsFromBytes(cd0, off0, lengthLen);
				if (l > len) {
					// Column is OUTROW ST column, !ST.variable.
					new ImplementationRestrictionException(ACDPException.prefix(
										table, colName) + "Outrow data is too large (" +
										l + ") to fit into inrow column section (" +
										len + ").");
				}
				
				if (l == 0)
					// Empty array. Copy zero array size.
					System.arraycopy(Utils_.zeros[8], 0, bytes, offset, 8);
				else {
					final int length = (int) l;
					// Initialize byte buffer. Note that length <= len.
					final ByteBuffer buf = ByteBuffer.wrap(bytes);
					buf.position(offset);
					buf.limit(offset + length);
					
					// Read byte representation and update column data.
					vlDataFile.read_(buf, Utils.unsFromBytes(cd0, off0 + lengthLen,
																					nobsOutrowPtr));
					// Deallocate memory block.
					vlFileSpace.deallocate(length, null);
				}
			}
		}
	}
	
	/**
	 * The allocation column data updater allocates a new memory block in the VL
	 * file space, writes the byte representation of the non-null current value
	 * to that memory block and copies the length and the pointer of the memory
	 * block into the current column data.
	 * <p>
	 * If the current value is {@code null} then this method just fills the
	 * current column data with zeros.
	 * <p>
	 * The targeted column is an INROW ST, INROW A[INROW ST] or INROW A[RT]
	 * column that turns into an OUTROW ST, OUTROW A[INROW ST] or OUTROW A[RT] 
	 * column, respectively.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class Alloc_U extends Updater {
		private final VLFileSpace vlFileSpace;
		private final FileIO vlDataFile;
		private final int sizeLen;
		private final int etLen;
		private final boolean etNullable;
		private final int lengthLen;
		private final int nobsOutrowPtr;
		private final byte[] zeros;
		private final P p;
		
		/**
		 * The length of the byte representation of the current value.
		 */
		private int length;
		
		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci0 The column information object of column to be modified, not
		 *        allowed to be {@code null}.
		 *        The column is assumed not to be a VL column.
		 * @param ci1 The new column information object, not allowed to be {@code
		 *        null}.
		 *        The column is assumed to be a VL column.
		 * @param p The presenter, not allowed to be {@code null}.
		 */
		Alloc_U(WRStore store, WRColInfo ci0, WRColInfo ci1, P p) {
			super(ci1.len);
			vlFileSpace = store.vlFileSpace;
			vlDataFile = store.vlDataFile;
			
			final Type type = ci0.col.type();
			if (type instanceof SimpleType) {
				sizeLen = 0;
				etLen = 0;
				etNullable = false;
			}
			else {
				sizeLen = ci0.sizeLen;
				if (type instanceof ArrayType_) {
					final SimpleType<?> et = ((ArrayType_) type).elementType();
					etLen = et.length();
					etNullable = et.nullable();
				}
				else {
					etLen = ci0.refdStore.nobsRowRef;
					etNullable = false;
				}
			}
			
			lengthLen = ci1.lengthLen;
			nobsOutrowPtr = store.nobsOutrowPtr;
			zeros = new byte[len];
			Arrays.fill(zeros, (byte) 0);
			this.p = p;
			length = ci0.len;
		}
		
		@Override
		public final void newData(byte[] bytes, int offset) throws
																					FileIOException {
			if (p.isNull)
				System.arraycopy(zeros, 0, bytes, offset, len);
			else {
				final byte[] cd0 = p.flDataBlock;
				final int off0 = p.offCD;
				
				if (sizeLen > 0 && Utils.isZero(cd0, off0, sizeLen)) {
					// Column is an array column and size of array is zero. No need
					// for writing to the VL file space. Just update column data.
					System.arraycopy(Utils_.zeros[lengthLen], 0, bytes, offset,
																							lengthLen);
					Utils.unsToBytes(1, nobsOutrowPtr, bytes, offset + lengthLen);
				}
				else {
					// Initialize length. Note that length is already initialized if
					// column is INROW ST column. Note also that if column is INROW
					// ST column then !ST.variable.
					if (sizeLen > 0) {
						// Column is either an INROW A[INROW ST] or an INROW A[RT]
						// column.
						final int size = (int) Utils.unsFromBytes(cd0, off0, sizeLen);
						length = sizeLen + size * etLen;
						if (etNullable) {
							length += Utils.bmLength(size);
						}
					}
					
					// Allocate memory block.
					final long ptr = vlFileSpace.allocate(length, null);
					
					// Initialize byte buffer.
					final ByteBuffer buf = ByteBuffer.wrap(cd0);
					buf.position(off0);
					buf.limit(off0 + length);
					
					// Write byte representation.
					vlDataFile.write(buf, ptr);
					
					// Update column data
					Utils.unsToBytes(length, lengthLen, bytes, offset);
					Utils.unsToBytes(ptr, nobsOutrowPtr, bytes, offset + lengthLen);
				}
			}
		}
	}
	
	/**
	 * Provides the (converted) current value.
	 * 
	 * @author Beat Hoermann
	 */
	private interface VProvider {
		/**
		 * Returns the (converted) current value.
		 * 
		 * @return The (converted) current value, may be {@code null}.
		 */
		Object v();
	}
	
	/**
	 * The base class of an updater that assumes the current value to be an
	 * {@code Object} rather than an array of byes.
	 * 
	 * @author Beat Hoermann
	 */
	private static abstract class V_U extends Updater {
		private final VProvider vProvider;
		private final IObjectToBytes o2b;
		private final Bag bag;

		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci1 The new column information object, not allowed to be {@code
		 *        null}.
		 * @param vProvider The value provider, not allowed to be {@code null}.
		 */
		V_U(WRStore store, WRColInfo ci1, VProvider vProvider) {
			super(ci1.len);
			this.vProvider = vProvider;
			o2b = new ObjectToBytes(store).create(ci1);
			bag = new Bag();
		}
		
		/**
		 * Converts the {@linkplain VProvider#v current value} to its column data
		 * and updates the current column data.
		 * 
		 * @param  bag0 The bag containing the current column data.
		 * @param  bytes The byte array containing the current column data, never
		 *         {@code null}.
		 * @param  offset The position where the current column data starts in
		 *         the byte array.
		 * 
		 * @throws NullPointerException If the current value is an array value
		 *         and at least one element is equal to {@code null} but the
		 *         type of the elements forbids the {@code null} value.
		 * @throws IllegalArgumentException If the length of the byte
		 *         representation of the current value (or one of the elements if
		 *         the value is an array value) exceeds the maximum length allowed
		 *         by the simple column type.
		 *         This exception also happens if the value is an array value and
		 *         the size of the array exceeds the maximum length allowed by
		 *         the array column type.
		 * @throws MaximumException If a new memory block in the VL file space
		 *         must be allocated and its file position exceeds the maximum
		 *         allowed position.
		 * @throws CryptoException If encryption fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption or if the column type is an A[RT].
		 * @throws FileIOException If an I/O error occurs.
		 *         This exception can happen only if the column type has an
		 *         outrow storage scheme or is an INROW A[OUTROW ST].
		 */
		protected final void update(Bag bag0, byte[] bytes, int offset) throws
									NullPointerException, IllegalArgumentException,
									MaximumException, CryptoException, FileIOException {
			bag.bytes = bytes;
			bag.offset = offset;
			// bag0 == null requires bag.bytes filled with zeros, see description
			// of o2b.convert method.
			if (bag0 == null) {
				Arrays.fill(bytes, offset, offset + len, (byte) 0);
			}
			// Do the conversion and update column data. Remember that the
			// o2b.convert method assumes an Insert or Update operation if
			// bag0 == null or bag0 != null, respectively.
			o2b.convert(vProvider.v(), 0L, bag0, null, bag);
		}
	}
	
	/**
	 * This column data updater {@linkplain #update updates the current column
	 * data} by trying to reuse any VL file space.
	 * <p>
	 * Does nothing if the current value is {@code null}.
	 * <p>
	 * The targeted column is a VL column that remains a VL column.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class V0VV_U extends V_U {
		private final V0_P p;
		private final Bag bag0;
		
		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci1 The new column information object, not allowed to be {@code
		 *        null}.
		 *        The column is assumed to be a VL column.
		 * @param p The presenter, not allowed to be {@code null}.
		 */
		V0VV_U(WRStore store, WRColInfo ci1, V0_P p) {
			super(store, ci1, p);
			this.p = p;
			bag0 = new Bag();
		}
		
		@Override
		final void newData(byte[] bytes, int offset) throws
									NullPointerException, IllegalArgumentException,
									MaximumException, CryptoException, FileIOException {
			if (!p.isNull) {
				bag0.bytes = p.flDataBlock;
				bag0.offset = p.offCD;
				
				update(bag0, bytes, offset);
			}
		}
	}
	
	/**
	 * Before {@linkplain #update updating the current column data} this column
	 * data updater deallocates the memory block occupied by the byte
	 * representation of a non-null current value from the VL file space.
	 * <p>
	 * Does nothing if the current value is {@code null}.
	 * <p>
	 * The targeted column is a VL column that turns into an FL column.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class V0VF_U extends V_U {
		private final V0_P p;
		private final VLFileSpace vlFileSpace;
		private final int lengthLen;

		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci0 The column information object of the column to be modified,
		 *        not allowed to be {@code null}.
		 *        The column is assumed to be a VL column.
		 * @param ci1 The new column information object, not allowed to be {@code
		 *        null}.
		 *        The column is assumed not to be a VL column.
		 * @param p The presenter, not allowed to be {@code null}.
		 */
		V0VF_U(WRStore store, WRColInfo ci0, WRColInfo ci1, V0_P p) {
			super(store, ci1, p);
			this.p = p;
			vlFileSpace = store.vlFileSpace;
			lengthLen = ci0.lengthLen;
		}
		
		@Override
		final void newData(byte[] bytes, int offset) throws
									NullPointerException, IllegalArgumentException,
									MaximumException, CryptoException, FileIOException {
			if (!p.isNull) {
				vlFileSpace.deallocate(Utils.unsFromBytes(p.flDataBlock, p.offCD,
																				lengthLen), null);
				update(null, bytes, offset);
			}
		}
	}
	
	/**
	 * This column data updater {@linkplain #update updates the current column
	 * data}.
	 * <p>
	 * If the current value is {@code null} then it just filles the current
	 * column data with zeros.
	 * <p>
	 * The targeted column is an FL column that turns into a VL column.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class V0FV_U extends V_U {
		private final V0_P p;
		private final byte[] zeros;
		
		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci1 The new column information object, not allowed to be {@code
		 *        null}.
		 *        The column is assumed to be a VL column.
		 * @param p The presenter, not allowed to be {@code null}.
		 */
		V0FV_U(WRStore store, WRColInfo ci1, V0_P p) {
			super(store, ci1, p);
			this.p = p;
			zeros = new byte[len];
			Arrays.fill(zeros, (byte) 0);
		}
		
		@Override
		final void newData(byte[] bytes, int offset) throws
									NullPointerException, IllegalArgumentException,
									MaximumException, CryptoException, FileIOException {
			if (p.isNull)
				System.arraycopy(zeros, 0, bytes, offset, len);
			else {
				update(null, bytes, offset);
			}
		}
	}
	
	/**
	 * This column data updater {@linkplain #update updates the current column
	 * data}.
	 * <p>
	 * Does nothing if the current value is {@code null}.
	 * <p>
	 * The targeted column is an FL column that remains an FL column.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class V0FF_U extends V_U {
		private final V0_P p;
		
		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci1 The new column information object, not allowed to be {@code
		 *        null}.
		 *        The column is assumed to be not a VL column.
		 * @param p The presenter, not allowed to be {@code null}.
		 */
		V0FF_U(WRStore store, WRColInfo ci1, V0_P p) {
			super(store, ci1, p);
			this.p = p;
		}
		
		@Override
		final void newData(byte[] bytes, int offset) throws
									NullPointerException, IllegalArgumentException,
									MaximumException, CryptoException, FileIOException {
			if (!p.isNull) {
				update(null, bytes, offset);
			}
		}
	}
	
	/**
	 * This column data updater {@linkplain #update updates the current column
	 * data} by trying to reuse any VL file space.
	 * <p>
	 * The targeted column is a VL column that remains a VL column.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class V1VV_U extends V_U {
		private final V1_P p;
		private final Bag bag0;
		
		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci1 The new column information object, not allowed to be {@code
		 *        null}.
		 *        The column is assumed to be a VL column.
		 * @param p The presenter, not allowed to be {@code null}.
		 */
		V1VV_U(WRStore store, WRColInfo ci1, V1_P p) {
			super(store, ci1, p);
			this.p = p;
			bag0 = new Bag();
		}
		
		@Override
		final void newData(byte[] bytes, int offset) throws
									NullPointerException, IllegalArgumentException,
									MaximumException, CryptoException, FileIOException {
			if (p.v0 == null)
				update(null, bytes, offset);
			else {
				bag0.bytes = p.flDataBlock;
				bag0.offset = p.offCD;
				
				update(bag0, bytes, offset);
			}
		}
	}
	
	/**
	 * Before {@linkplain #update updating the current column data} this column
	 * data updater deallocates the memory block occupied by the byte
	 * representation of a non-null current value from the VL file space.
	 * <p>
	 * The targeted column is a VL column that turns into an FL column.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class V1VF_U extends V_U {
		private final V1_P p;
		private final VLFileSpace vlFileSpace;
		private final int lengthLen;

		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci0 The column information object of the column to be modified,
		 *        not allowed to be {@code null}.
		 *        The column is assumed to be a VL column.
		 * @param ci1 The new column information object, not allowed to be {@code
		 *        null}.
		 *        The column is assumed not to be a VL column.
		 * @param p The presenter, not allowed to be {@code null}.
		 */
		V1VF_U(WRStore store, WRColInfo ci0, WRColInfo ci1, V1_P p) {
			super(store, ci1, p);
			this.p = p;
			vlFileSpace = store.vlFileSpace;
			lengthLen = ci0.lengthLen;
		}
		
		@Override
		final void newData(byte[] bytes, int offset) throws
									NullPointerException, IllegalArgumentException,
									MaximumException, CryptoException, FileIOException {
			if (p.v0 != null) {
				vlFileSpace.deallocate(Utils.unsFromBytes(p.flDataBlock, p.offCD,
																				lengthLen), null);
			}
			
			// p.v1 == null for an FL column -> update method checks if the null
			//                                  value is allowed.
			update(null, bytes, offset);
		}
	}
	
	/**
	 * This column data updater {@linkplain #update updates the current column
	 * data}.
	 * <p>
	 * The targeted column is an FL column that turns into a VL column or both
	 * columns are FL columns.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class V1FVFF_U extends V_U {
		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci1 The new column information object, not allowed to be {@code
		 *        null}.
		 *        The column is assumed to be a VL column.
		 * @param p The presenter, not allowed to be {@code null}.
		 */
		V1FVFF_U(WRStore store, WRColInfo ci1, V1_P p) {
			super(store, ci1, p);
		}
		
		@Override
		final void newData(byte[] bytes, int offset) throws
									NullPointerException, IllegalArgumentException,
									MaximumException, CryptoException, FileIOException {
			// p.v1 == null for an FL column -> update method checks if the null
			//                                  value is allowed.
			update(null, bytes, offset);
		}
	}
	
	/**
	 * Indicates whether only scheme and nullable are different or other
	 * properties as well.
	 * This method assumes that the type descriptors of both types are different
	 * and that
	 * 
	 * <ol>
	 *    <li>Both types are ST and both value types are identical, or</li>
	 *    <li>Both types are array types.</li>
	 * </ol>
	 * 
	 * @param   t0 The type of the column to be modified before modification, not
	 *          allowed to be {@code null}.
	 * @param   t1 The target type of the column to be modified, not allowed to
	 *          be {@code null}.
	 *         
	 * @return The boolean value {@code true} if and only if both types are
	 *         ST and only scheme or nullable or both are different or both
	 *         types are array types and only scheme has a different value.
	 */
	private final boolean schemeNullableOnly(Type t0, Type t1) {
		// Precondition: both types are A[RT] OR
		//                     (both types are A[ST] OR both types are ST) AND
		//                     both value types are identical
		if (t0 instanceof SimpleType) {
			// Both types are ST with same value type.
			final SimpleType<?> st0 = (SimpleType<?>) t0;
			final SimpleType<?> st1 = (SimpleType<?>) t1;
			return st0.length() == st1.length() && st0.variable() ==st1.variable();
		}
		else {
			// Both types are array types
			final boolean sameMaxSize = ((AbstractArrayType) t0).maxSize() ==
														((AbstractArrayType) t1).maxSize();
			if (t0 instanceof ArrayType_) {
				// Both types are A[ST]
				return sameMaxSize && ((ArrayType_) t0).elementType().
							typeDesc() == ((ArrayType_) t1).elementType().typeDesc();
			}
			else {
				// Both types are A[RT]
				return sameMaxSize;
			}
		}
	}
	
	/**
	 * Accommodates the FL data file around the specified column to be modified.
	 * <p>
	 * Treats the special case where both column types are INROW ST AND only
	 * nullable changes.
	 * <p>
	 * This method assumes that the {@code valueChanger} argument of the {@link
	 * ColModify#run} method is {@code null}.
	 * 
	 * @param  store The store, not allowed to be {@code null}.
	 * @param  ci0 The column information object of the column to be modified,
	 *         not allowed to be {@code null}.
	 * @param  col1 The column replacing the column to be modified, not allowed
	 *         to be {@code null}.
	 * 
	 * @throws NullPointerException If a value is set equal to {@code null} but
	 *         the column type forbids the {@code null} value.
	 * @throws ImplementationRestrictionException If the Null-info must be
	 *         expanded by a single bit but the bitmap is too large for being
	 *         expanded.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void accommodate1(WRStore store, WRColInfo ci0,
																			Column_<?> col1) throws
							NullPointerException, ImplementationRestrictionException,
																				FileIOException {
		// Build presenter.
		final NiAdjuster niAjuster;
		// t0 denotes type of col0, t1 denotes type of col1.
		if (ci0.nullBitMask != 0)
			// t0 is INROW, nullable ST AND t1 is INROW, Non-NULL ST
			// Contract Null-info.
			niAjuster = new NiC(store, ci0);
		else {
			// t0 is INROW, Non-NULL ST AND t1 is INROW, nullable ST
			// Expand Null-info.
			niAjuster = new NiE(store, ci0);
		}
		// Build presenter.
		final P p = new Ni_SNO_P(niAjuster, store, ci0, col1.type());
		// Build bitmap updater.
		final BM_U bmu = new BM_U((BMProvider) p);
		// Accommodate FL data file.
		FLFileAccommodate.spot(0, bmu.len - store.nBM, bmu).run(p, store);
	}
	
	/**
	 * Creates a particular specification from the specified parameters.
	 * If the bitmap updater is equal to {@code null} then the returned
	 * specification has a single spot.
	 * Otherwise, the returend specification has two spots.
	 * 
	 * @param  store The store, not allowed to be {@code null}.
	 * @param  ci0 The column information object of the column to be modified.
	 * @param  ci1 The column information object of the column replacing the
	 *         column to be modified, not allowed to be {@code null}.
	 * @param  bmu The bitmap updater, may be {@code null}.
	 * @param  cdu The column data updater, not allowed to be {@code null}.
	 * 
	 * @return The created specification, never {@code null}.
	 */
	private final Spec spec(WRStore store, WRColInfo ci0, WRColInfo ci1,
																	Updater bmu, Updater cdu) {
		final Spec spec = FLFileAccommodate.newSpec();
		if (bmu != null) {
			spec.spot(0, bmu.len - store.nBM, bmu);
		}
		return spec.spot(ci0.offset, ci1.len - ci0.len, cdu);
	}
	
	/**
	 * Accommodates the FL data file around the specified column to be modified.
	 * <p>
	 * Treats one of the following special cases:
	 * 
	 * <ul>
	 *	   <li>Both types are ST AND scheme changes.
	 *        The nullable property may change too but nothing else.
	 *        The variable property is {@code false}.</li>
	 *    <li>Both types are A[RT] AND only scheme changes.</li>
	 *    <li>Both types are A[INROW ST] AND only scheme changes.</li>
	 * </ul>
	 * <p>
	 * This method assumes that the {@code valueChanger} argument of the {@link
	 * ColModify#run} method is {@code null}.
	 * 
	 * @param  store The store, not allowed to be {@code null}.
	 * @param  ci0 The column information object of the column to be modified,
	 *         not allowed to be {@code null}.
	 * @param  col1 The column replacing the column to be modified, not allowed
	 *         to be {@code null}.
	 * 
	 * @throws NullPointerException If a value is set equal to {@code null} but
	 *         the column type forbids the {@code null} value.
	 * @throws MaximumException If a new memory block in the VL file space
	 *         must be allocated and its file position exceeds the maximum
	 *         allowed position.
	 * @throws ImplementationRestrictionException If the Null-info must be
	 *         expanded by a single bit but the bitmap is too large for being
	 *         expanded <em>or</em> if the byte representation stored in the VL
	 *         file space is too large to fit into the inrow column section.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void accommodate2(WRStore store, WRColInfo ci0,
																			Column_<?> col1) throws
									NullPointerException, MaximumException,
									ImplementationRestrictionException, FileIOException {
		final Type t1 = col1.type();
		final boolean t1inrow = t1.scheme() == Scheme.INROW;
		final Boolean t0hasNI = ci0.nullBitMask != 0;
		final boolean t1hasNI = t1inrow && (t1 instanceof AbstractArrayType ||
															((SimpleType<?>) t1).nullable());
		// Build presenter.
		final P p;
		if (t0hasNI && !t1hasNI)
			// Contract Null-info.
			p = new Ni_SNO_P(new NiC(store, ci0), store, ci0, t1);
		else if (!t0hasNI && t1hasNI)
			// Expand Null-info.
			p = new Ni_SNO_P(new NiE(store, ci0), store, ci0, t1);
		else {
			// Both types have Null-info OR none of the types has
			// Null-info. Actually, this case happens only if both
			// columns are Non-NULL ST columns.
			p = new SNO_P(store, ci0, t1);
		}
		// Build bitmap updater.
		final BM_U bmu = p instanceof BMProvider ?
													new BM_U((BMProvider) p) : null;
													
		final WRColInfo ci1 = store.createCi(col1);
		
		// Build column data updater.
		final Updater cdu = t1inrow ? new Dealloc_U(store, ci0, ci1, p) :
												new Alloc_U(store, ci0, ci1, p);
		// Accommodate FL data file.
		spec(store, ci0, ci1, bmu, cdu).run(p, store);
	}
	
	/**
	 * Accommodates the FL data file around the specified column to be modified.
	 * <p>
	 * Treats one of the following special cases:
	 * 
	 * <ul>
	 *	   <li>Both types are ST having same VT AND at least length or variable
	 *        change.</li>
	 *    <li>Both types are A[RT] AND at least maxSize changes.</li>
	 *    <li>Both types are A[ST] AND at least maxSize or some properties of ST
	 *        change, although both element types have same VT.</li>
	 * </ul>
	 * <p>
	 * This method assumes that the {@code valueChanger} argument of the {@link
	 * ColModify#run} method is {@code null}.
	 * It follows that if the stored value is {@code null} or not {@code null}
	 * then the new value will be {@code null} or not {@code null} as well,
	 * respectively.
	 * 
	 * @param  store The store, not allowed to be {@code null}.
	 * @param  ci0 The column information object of the column to be modified,
	 *         not allowed to be {@code null}.
	 * @param  col1 The column replacing the column to be modified, not allowed
	 *         to be {@code null}.
	 * 
	 * @throws NullPointerException If a value is set equal to {@code null} but
	 *         the column type forbids the {@code null} value.
	 * @throws MaximumException If a new memory block in the VL file space
	 *         must be allocated and its file position exceeds the maximum
	 *         allowed position.
	 * @throws CryptoException If encryption or decryption fails.
	 *         This exception never happens if the WR database does not apply
	 *         encryption or if the column type is an A[RT].
	 * @throws ImplementationRestrictionException If the Null-info must be
	 *         expanded by a single bit but the bitmap is too large for being
	 *         expanded <em>or</em> if the length of the column data is too
	 *         small to fit the byte representation stored in the VL file space.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void accommodate3(WRStore store, WRColInfo ci0,
																			Column_<?> col1) throws
								NullPointerException, MaximumException, CryptoException,
								ImplementationRestrictionException, FileIOException {
		final Type t0 = ci0.col.type();
		final Type t1 = col1.type();
		final boolean t1inrow = t1.scheme() == Scheme.INROW;
		final Boolean t0hasNI = ci0.nullBitMask != 0;
		final boolean t1hasNI = t1inrow && (
						t1 instanceof SimpleType && ((SimpleType<?>) t1).nullable() ||
						t1 instanceof ArrayOfRefType_ || t1 instanceof ArrayType_ &&
						((ArrayType_) t1).elementType().scheme() == Scheme.INROW);
		
		// Build presenter.
		final V0_P p;
		if (t0hasNI && !t1hasNI)
			// Contract Null-info.
			p = new Ni_V0_P(new NiC(store, ci0), store, ci0, t1);
		else if (!t0hasNI && t1hasNI)
			// Expand Null-info.
			p = new Ni_V0_P(new NiE(store, ci0), store, ci0, t1);
		else {
			// Both types have Null-info OR none of the types has
			// Null-info.
			p = new V0_P(store, ci0, t1);
		}
		// Build bitmap updater.
		final BM_U bmu = p instanceof BMProvider ?
														new BM_U((BMProvider) p) : null;

		final boolean isVL0 = !t0hasNI && !(t0 instanceof SimpleType<?> &&
															t0.scheme() == Scheme.INROW);
		final boolean isVL1 = !t1hasNI && !(t1 instanceof SimpleType<?> &&
																					t1inrow);
		final WRColInfo ci1 = store.createCi(col1);
		
		// Build column data updater.
		final Updater cdu;
		if (isVL0 && isVL1)
			cdu = new V0VV_U(store, ci1, p);
		else if (isVL0 && !isVL1)
			cdu = new V0VF_U(store, ci0, ci1, p);
		else if (!isVL0 && isVL1)
			cdu = new V0FV_U(store, ci1, p);
		else { // !isVL0 && !isVL1
			cdu = new V0FF_U(store, ci1, p);
		}
		
		// Accommodate FL data file.
		spec(store, ci0, ci1, bmu, cdu).run(p, store);
	}
	
	/**
	 * Accommodates the FL data file around the specified column to be modified.
	 * <p>
	 * It is assumed hat the {@code valueChanger} argument of the {@link
	 * ColModify#run} method is not {@code null}.
	 * This implies that none of the types is A[RT].
	 * <p>
	 * Note that we have to distinguish between the stored current value and the
	 * converted current value.
	 * Both can be {@code null}.
	 * The check if the converted current value is allowed to be {@code null}
	 * does not take place within the presenter but in the column data updater
	 * when invoking the IObjectToBytes-Converter method.
	 * <p>
	 * The value resulting from applying the specified value changer must be
	 * {@linkplain Type#isCompatible compatible} with the type of the specified
	 * column replacing the column to be modified.
	 * This method does not explicitly check this precondition in all situations.
	 * In any case, if this precondition is not satisfied then this method
	 * throws an exception, however, this may be an exception of a type not
	 * listed below.
	 * 
	 * @param  <T> The type of the new column's values.
	 * 
	 * @param  store The store, not allowed to be {@code null}.
	 * @param  ci0 The column information object of the column to be modified,
	 *         not allowed to be {@code null}.
	 * @param  col1 The column replacing the column to be modified, not allowed
	 *         to be {@code null}.
	 * @param  valueChanger The value changer, not allowed to be {@code null}.
	 * 
	 * @throws NullPointerException If a value of a simple column type is set
	 *         equal to {@code null} but the column type forbids the {@code
	 *         null} value or if the value is an array value and this condition
	 *         is satisfied for at least one element contained in the array.
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
	 *         expanded <em>or</em> if the length of the column data is too
	 *         small to fit the byte representation stored in the VL file space
	 *         <em>or</em> if the number of bytes needed to persist a value of
	 *         an array type exceeds Java's maximum array size.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final <T> void accommodate4(WRStore store, WRColInfo ci0,
									Column_<T> col1, ValueChanger<T> valueChanger) throws
									NullPointerException, IllegalArgumentException,
									MaximumException, CryptoException,
									ImplementationRestrictionException, FileIOException {
		final Type t0 = ci0.col.type();
		final Type t1 = col1.type();
		final boolean t1inrow = t1.scheme() == Scheme.INROW;
		final Boolean t0hasNI = ci0.nullBitMask != 0;
		final boolean t1hasNI = t1inrow && (
						t1 instanceof SimpleType && ((SimpleType<?>) t1).nullable() ||
						t1 instanceof ArrayType_ && ((ArrayType_) t1).elementType().
																	scheme() == Scheme.INROW);
		
		// Build presenter.
		final V1_P p;
		if (t0hasNI && !t1hasNI)
			// Contract Null-info.
			p = new Ni_V1_P(new NiC(store, ci0), store, ci0, valueChanger);
		else if (!t0hasNI && t1hasNI)
			// Expand Null-info.
			p = new Ni_V1_P(new NiE(store, ci0), store, ci0, valueChanger);
		else if (t0hasNI && t1hasNI)
			p = new NiSet_V1_P(store, ci0, valueChanger);
		else {
			// None of the types has Null-info.
			p = new V1_P(store, ci0, valueChanger);
		}
		// Build bitmap updater.
		final BM_U bmu = p instanceof BMProvider ?
															new BM_U((BMProvider) p) : null;

		final boolean isVL0 = !t0hasNI && !(t0 instanceof SimpleType<?> &&
															t0.scheme() == Scheme.INROW);
		final boolean isVL1 = !t1hasNI && !(t1 instanceof SimpleType<?> &&
																					t1inrow);
		final WRColInfo ci1 = store.createCi(col1);
		
		// Build column data updater.
		final Updater cdu;
		if (isVL0 && isVL1)
			cdu = new V1VV_U(store, ci1, p);
		else if (isVL0 && !isVL1)
			cdu = new V1VF_U(store, ci0, ci1, p);
		else {
			// !isVL0 && isVL1 || !isVL0 && !isVL1
			cdu = new V1FVFF_U(store, ci1, p);
		}
		
		// Accommodate FL data file.
		spec(store, ci0, ci1, bmu, cdu).run(p, store);
	}
	
	/**
	 * Accommodates the FL data file of the specified store with respect to the
	 * specified column to be modified.
	 * <p>
	 * This method makes the following assumptions:
	 * 
	 * <ol>
	 *    <li>The table is not empty.</li>
	 *    <li>The type descriptors of the specified columns are different.</li>
	 *    <li>The value changer is not {@code null} or if it is {@code null}
	 *        then the modification is not a trivial one (see below).</li>
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
	 * @param  <T> The type of the new column's values.
	 * 
	 * @param  store The WR store, not allowed to be {@code null}.
	 * @param  col0 The column to be modified, not allowed to be {@code null}.
	 * @param  col1 The column replacing the column to be modified, not allowed
	 *         to be {@code null}.
	 * @param  valueChanger The value changer, may be {@code null}.
	 * 
	 * @throws NullPointerException If one of the arguments not allowed to be
	 *         {@code null} is {@code null} or if a value of a simple column type
	 *         is set equal to {@code null} but the column type forbids the
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
	final <T> void run(WRStore store, Column_<?> col0, Column_<T> col1,
														ValueChanger<T> valueChanger) throws
									NullPointerException, IllegalArgumentException,
									MaximumException, CryptoException,
									ImplementationRestrictionException, FileIOException {
		// Assumptions:
		// 1. Table not empty
		// 2. Type descriptors of column types are different.
		// 3. valueChanger != null OR non-trivial modification
		// 4. valueChanger != null IMPLIES none of the column types is A[RT]
		// 5. valueChanger == null IMPLIES both types are A[RT] OR
		//                     (both types are A[ST] OR both types are ST) AND
		//                     both value types are identical
		final WRColInfo ci0 = store.colInfoMap.get(col0);
		final Type t0 = col0.type();
		final Type t1 = col1.type();
		if (valueChanger == null) {
			// valueChanger == null IMPLIES both types are A[RT] OR
			//                     (both types are A[ST] OR both types are ST) AND
			//                     both value types are identical
			// Note that if the current value is null or not null then current
			// value of the modified column will be null or not null, respectively.
			if (schemeNullableOnly(t0, t1)) {
				// Byte representation remains unchanged.
				// If both types are ST then scheme or nullable or both change.
				// Otherwise, both types are array types and only scheme changes.
				if (t0.scheme() == t1.scheme())
					// Both types are INROW ST AND only nullable changes. They can't
					// be OUTROW ST because this would be a trivial modification
					// which is impossible due to the assumption.
					accommodate1(store, ci0, col1);
				else {
					// We have one of the following situations:
					// 1. Both types are ST AND scheme changes. Nullable may change
					//    too but nothing else. Since scheme changes but not
					//    variable, variable is false. Changing the scheme of an ST
					//    column without changing length or variable is probably not
					//    a typical use-case.
					// 2. Both types are A[RT] AND only scheme changes.
					// 3. Both types are A[INROW ST] AND only scheme changes. ST is
					//    indeed INROW because otherwise the modification would be a
					//    trivial modificiation which is impossible due to the
					//    assumption.
					accommodate2(store, ci0, col1);
				}
			}
			else {
				// Byte represenation changes.
				// We have one of the following situations:
				// 1. Both types are ST having same VT AND at least length or
				//    variable change.
				// 2. Both types are A[RT] AND at least maxSize changes.
				// 3. Both types are A[ST] AND at least maxSize or some properties
				//    of ST change, although both element types have same VT.
				// Furthermore, if both types are array types and only maxSize
				// changes then lor(maxSize0) != lor(maxSize1) (non-trivial
				// modification).
				// Moreover, remember that if the current value is null or not null
				// then current value of the modified column will be null or not
				// null, respectively.
				accommodate3(store, ci0, col1);
			}
		}
		else {
			// valueChanger != null
			// valueChanger != null IMPLIES none of the types is A[RT]
			accommodate4(store, ci0, col1, valueChanger);
		}
	}
}