/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import acdp.design.SimpleType;
import acdp.exceptions.CryptoException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.MaximumException;
import acdp.internal.Column_;
import acdp.internal.FileIOException;
import acdp.internal.store.Bag;
import acdp.internal.store.wr.FLFileAccommodate.Presenter;
import acdp.internal.store.wr.FLFileAccommodate.Spec;
import acdp.internal.store.wr.FLFileAccommodate.Updater;
import acdp.internal.store.wr.ObjectToBytes.IObjectToBytes;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.internal.types.ArrayOfRefType_;
import acdp.internal.types.ArrayType_;
import acdp.internal.types.Type_;
import acdp.misc.Utils;
import acdp.types.Type.Scheme;

/**
 * Provides the {@link #run} method which {@linkplain FLFileAccommodate
 * accommodates} the FL data file around a column to be inserted.
 *
 * @author Beat Hoermann
 */
final class ColInsert {
	/**
	 * The Ni presenter adjusts the Null-info of the bitmap.
	 * <p>
	 * The targeted column is either a nullable INROW ST, an INROW A[INROW ST]
	 * or an INROW A[RT] column, hence, a column with a Null-info.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class NiPresenter implements Presenter {
		/**
		 * The information whether the initial value is equal to {@code null} or
		 * not.
		 */
		private final boolean isNull;
		/**
		 * The size of the bitmap in bytes, &gt; 0 and &le; 8.
		 */
		private final int nBM;
		/**
		 * The new size of the bitmap in bytes, &ge; {@code nBM} and &le; 8.
		 */
		final int newNBM;
		/**
		 * The new bitmap of the current row.
		 */
		long newBitmap;
		/**
		 * The null bit mask of the column to be inserted.
		 */
		private final long nullBitMask;
		
		private final long mask0;
		private final long mask1;
		
		/**
		 * The constructor.
		 * 
		 * @param  store The store, not allowed to be {@code null}.
		 * @param  index The index within the table definition where the column
		 *         is to be inserted.
		 * @param  isNull The information whether the initial value is equal to
		 *         {@code null} or not.
		 *         
		 * @throws ImplementationRestrictionException If the bitmap is too large
		 *         for being expanded.
		 */
		NiPresenter(WRStore store, int index, boolean isNull) throws
														ImplementationRestrictionException {
			this.isNull = isNull;
			nBM = store.nBM;
			
			// Compute the length of the Null-info and remember the null bit mask
			// of the column with the highest index less than the index argument.
			int niLen = 0;
			long l = -1;
			int colIndex = 0;
			for (WRColInfo colInfo : store.colInfoArr) {
				final long nbm = colInfo.nullBitMask;
				if (nbm != 0) {
					if (colIndex < index) {
						l = nbm;
					}
					niLen++;
				}
				colIndex++;
			}
			
			if (niLen > 62) {
				throw new ImplementationRestrictionException(store.table,
										"Table has too many columns needing a separate " +
										"null information.");
			}
			
			// Compute the null bit mask of the new column.
			nullBitMask = l == -1 ? 1L : l << 1;
			
			mask0 = nullBitMask - 1;
			mask1 = ~mask0 << 1;
			
			
			// nBM == Utils.bmLength(niLen + 1)
			// If we insert a column with a Null-info then newNiLen == niLen + 1
			// so that newBM == Utils.bmLength(niLen + 2)
			newNBM = Utils.bmLength(niLen + 2);
		}
		
		/**
		 * Adjusts the specified bitmap by expanding the Null-info contained in
		 * the specified bitmap and setting the correct bit depending on whether
		 * the initial value is equal to {@code null} or not.
		 * 
		 * @param  bm0 The bitmap containing the Null-info.
		 * 
		 * @return The adjusted bitmap.
		 */
		private final long adjustBitmap(long bm0) {
			final long bm1 = bm0 & mask0;
			bm0 <<= 1;
			bm0 &= mask1;
			bm0 |= bm1;
			return isNull ? bm0 | nullBitMask : bm0;
		}

		@Override
		public final void rowData(byte[] flDataBlock, int offset) throws
																					FileIOException {
			newBitmap = adjustBitmap(Utils.unsFromBytes(flDataBlock, offset, nBM));
		}
	}
	
	/**
	 * The bitmap updater updates the bitmap.
	 * <p>
	 * The targeted column is a column with a Null-info.
	 * 
	 * @author Beat Hoermann Hörmann
	 */
	private static final class BitmapUpdater extends Updater {
		private final NiPresenter niPresenter;
		
		/**
		 * The constructor.
		 * 
		 * @param  niPresenter The Ni presenter, not allowed to be {@code null}.
		 */
		BitmapUpdater(NiPresenter niPresenter) {
			super(niPresenter.newNBM);
			this.niPresenter = niPresenter;
		}

		@Override
		public final void newData(byte[] bytes, int offset) {
			Utils.unsToBytes(niPresenter.newBitmap, len, bytes, offset);
		}
	}
	
	/**
	 * The simple column updater updates the column data with the data contained
	 * in the byte array given to this class via its constructor.
	 * <p>
	 * The targeted column is an INROW ST or INROW A[INROW ST].
	 * 
	 * @author Beat Hoermann Hörmann
	 */
	private static final class SimpleColUpdater extends Updater {
		private final byte[] byteArr;
		
		/**
		 * The constructor.
		 * 
		 * @param bytes The data used to update the column data.
		 */
		SimpleColUpdater(byte[] bytes) {
			super(bytes.length);
			byteArr = bytes;
		}
		
		@Override
		public final void newData(byte[] bytes, int offset) {
			System.arraycopy(byteArr, 0, bytes, offset, len);
		}
	}
	
	/**
	 * The allocation column updater updates the column data of a pointer column
	 * with the value given to this class via its constructor.
	 * <p>
	 * Note that this method writes, as a side effect, the byte representation
	 * of the value to the VL data file of the store.
	 * <p>
	 * The targeted column is an OUTROW ST, INROW A[OUTROW ST] or OUTROW A[ST]
	 * column.
	 * 
	 * @author Beat Hoermann Hörmann
	 */
	private static final class AllocColUpdater extends Updater {
		private final IObjectToBytes o2b;
		private final Object initialValue;
		private final Bag bag;
		
		/**
		 * The constructor.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 * @param ci The column information object of the VL column to be
		 *        inserted, not allowed to be {@code null}.
		 * @param initialValue The initial value, typicall not {@code null}
		 *        and assumed to be {@linkplain acdp.types.Type#isCompatible
		 *        compatible} with the type of the column.
		 */
		AllocColUpdater(WRStore store, WRColInfo ci, Object initialValue) {
			super(ci.len);
			this.o2b = new ObjectToBytes(store).create(ci);
			this.initialValue = initialValue;
			this.bag = new Bag();
		}
		
		/**   
		 * @throws IllegalArgumentException If the length of the byte
		 *         representation of the value (or one of the elements if the
		 *         value is an array value) exceeds the maximum length allowed by
		 *         the simple column type.
		 * @throws MaximumException If the file position of the allocated memory
		 *         block in the VL file space exceeds the maximum allowed
		 *         position.
		 * @throws CryptoException If encryption fails.
		 *         This exception never happens if the database does not apply
		 *         encryption.
		 * @throws FileIOException If an I/O error occurs.
		 */
		@Override
		public final void newData(byte[] bytes, int offset) throws
											IllegalArgumentException, MaximumException,
															CryptoException, FileIOException {
			bag.bytes = bytes;
			bag.offset = offset;
			o2b.convert(initialValue, 0, null, null, bag);
		}
	}
	
	/**
	 * Computes the offset within the FL data block where the column data of the
	 * new column having the specified index starts.
	 * 
	 * @param  store The store, not allowed to be {@code null}.
	 * @param  index The index of the new column.
	 * 
	 * @return The offset of the column data of the column to be inserted.
	 */
	private final int computeOffset(WRStore store, int index) {
		WRColInfo[] ciArray = store.colInfoArr;
		if (index < ciArray.length)
			return ciArray[index].offset;
		else {
			// index == ciArray.length
			WRColInfo lastCi = ciArray[index - 1];
			return lastCi.offset + lastCi.len;
		}
	}
	
	/**
	 * Accommodates the FL data file of the specified store with respect to the
	 * specified column to be inserted.
	 * This method has no effect if the FL data file contains no FL data blocks.
	 * <p>
	 * To run properly the FL data file is not allowed to be already open at the
	 * time this method is invoked.
	 * <p>
	 * Note that the FL file space and hence the whole database won't work
	 * properly anymore after this method has been executed.
	 * The database should be closed as soon as possible.
	 * <p>
	 * Note also that the format of the FL data file may be corrupted if this
	 * method throws an exception different from the listed {@code
	 * NullPointerException}.
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
	 * @param  nobsRefCount If this value is greater than zero then room is
	 *         made in the FL data block for the reference counter.
	 *         This value must be greater than zero if and only if the FL data
	 *         blocks do not yet contain a reference counter and the reference
	 *         counter must be installed.
	 * 
	 * @throws NullPointerException If {@code store} or {@code col} is {@code
	 *         null} or if {@code col} is not a column of the store's table.
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
	final void run(WRStore store, Column_<?> col, int index, Object initialValue,
																		int nobsRefCount) throws
									NullPointerException, IllegalArgumentException,
									MaximumException, CryptoException, 
									ImplementationRestrictionException, FileIOException {
		final Type_ type = col.type();
		final WRColInfo ci = store.createCi(col);
		final boolean isNull = initialValue == null;

		ci.offset = computeOffset(store, index);
		
		// Precondition:
		// 1. nobsRefCount > 0 implies column is RT or INROW A[RT] or OUTROW A[RT]
		// 2. RT or INROW A[RT] or OUTROW A[RT] implies isNull
		
		if (type.scheme() == Scheme.INROW && (type instanceof SimpleType ||
									type instanceof ArrayType_ && ((ArrayType_) type).
									elementType().scheme() == Scheme.INROW) ||
															type instanceof ArrayOfRefType_) {
			// INROW ST or INROW A[INROW ST] or INROW A[RT]
			
			// Create a bag and fill it with the converted initial value.
			final Bag bag;
			if (isNull)
				bag = null;
			else {
				bag = new Bag(ci.len);
				new ObjectToBytes(store).create(ci).convert(initialValue, 0, null,
																							null, bag);
			}
			
			if (type instanceof SimpleType && !((SimpleType<?>) type).nullable()) {
				// Non-NULL INROW ST
				// Implies !isNull
				if (Utils.isZero(bag.bytes, 0, bag.bytes.length))
					FLFileAccommodate.spot(ci.offset, ci.len).run(store);
				else {
					FLFileAccommodate.spot(ci.offset, ci.len, new SimpleColUpdater(
																			bag.bytes)).run(store);
				}
			}
			else {
				// The column has a Null-info.
				final NiPresenter niP = new NiPresenter(store, index, isNull);
				final Updater bmu = new BitmapUpdater(niP);
				final Spec spec = FLFileAccommodate.spot(0, bmu.len-store.nBM, bmu);
				if (nobsRefCount > 0) {
					spec.spot(store.nBM, nobsRefCount);
				}
				
				if (isNull || Utils.isZero(bag.bytes, 0, bag.bytes.length))
					// Nullable INROW ST or INROW A[INROW ST] or INROW A[RT]
					spec.spot(ci.offset, ci.len).run(niP, store);
				else {
					// Nullable INROW ST or INROW A[INROW ST]
					spec.spot(ci.offset, ci.len, new SimpleColUpdater(bag.bytes))
																					.run(niP, store);
				}
			}
		}
		else if (isNull) {
			// OUTROW ST or RT or INROW A[OUTROW ST] or OUTROW A[ST/RT]
			if (nobsRefCount > 0)
				FLFileAccommodate.spot(store.nBM, nobsRefCount).
															spot(ci.offset, ci.len).run(store);
			else {
				FLFileAccommodate.spot(ci.offset, ci.len).run(store);
			}
		}
		else {
			// initialValue != null and
			// OUTROW ST or INROW A[OUTROW ST] or OUTROW A[ST]
			FLFileAccommodate.spot(ci.offset, ci.len, new AllocColUpdater(store,
																ci, initialValue)).run(store);
		}
	}
}