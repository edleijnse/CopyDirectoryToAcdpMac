/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import acdp.design.SimpleType;
import acdp.internal.Column_;
import acdp.internal.FileIOException;
import acdp.internal.store.Bag;
import acdp.internal.store.wr.FLFileAccommodate.Presenter;
import acdp.internal.store.wr.FLFileAccommodate.Spec;
import acdp.internal.store.wr.FLFileAccommodate.Updater;
import acdp.internal.store.wr.ObjectToBytes.IObjectToBytes;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.internal.types.ArrayType_;
import acdp.internal.types.Type_;
import acdp.misc.Utils;
import acdp.types.ArrayOfRefType;
import acdp.types.RefType;
import acdp.types.Type.Scheme;

/**
 * Provides the {@link #run} method which {@linkplain FLFileAccommodate
 * accommodates} the FL data file around a column to be removed.
 *
 * @author Beat Hoermann
 */
final class ColRemove {
	/**
	 * Provided that the given reference is not a null-reference, this method
	 * decrements the reference counter of the referenced row by one.
	 * 
	 * @param  bytes The byte array containing the reference.
	 * @param  off The offset within the byte array where the reference starts.
	 * @param  refLen The length of the reference.
	 * @param  refdStore The store of the referenced row.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	private static final void decRef(byte[] bytes, int off, int refLen,
												WRStore refdStore) throws FileIOException {
		final long ri = Utils.unsFromBytes(bytes, off, refLen);
		if (ri != 0) {
			GenericWriteOp.inc(refdStore, ri, -1, null);
		}
	}
	
	/**
	 * The Dec presenter decrements the reference counter of the referenced
	 * row.
	 * <p>
	 * The targeted column is an RT column.
	 * 
	 * @author Beat Hoermann Hörmann
	 */
	private static final class DecPresenter implements Presenter {
		private final int colOff;
		private final WRStore refdStore;
		private final int refLen;
		
		/**
		 * The constructor.
		 * 
		 * @param  ci The column information object of the RT column, not allowed
		 *         to be {@code null}.
		 *         The column must be a column of the store's table.
		 */
		DecPresenter(WRColInfo ci) {
			colOff = ci.offset;
			refdStore = ci.refdStore;
			refLen = refdStore.nobsRowRef;
		}

		@Override
		public final void rowData(byte[] flDataBlock, int offset) throws
																					FileIOException {
			// Column is an RT column.
			decRef(flDataBlock, offset + colOff, refLen, refdStore);
		}
	}
	
	/**
	 * The Ni presenter contracts the Null-info of the bitmap.
	 * <p>
	 * The targeted column is either a nullable INROW ST or an INROW A[INROW ST]
	 * column.
	 * 
	 * @author Beat Hoermann Hörmann
	 */
	private static class NiPresenter implements Presenter {
		/**
		 * The size of the bitmap in bytes, &gt; 0 and &le; 8.
		 */
		private final int nBM;
		/**
		 * The new size of the bitmap in bytes, &gt; 0 and &le; {@code nBM}.
		 */
		final int newNBM;
		
		/**
		 * The mask applied to the bitmap.
		 * This value is equal to 2<sup>{@code m}</sup>, where 0 &le; {@code m}
		 * &le; 62.
		 */
		protected final long nullBitMask;
		
		/**
		 * The bitmap of the current row.
		 */
		protected long bitmap;
		/**
		 * The new bitmap of the current row.
		 */
		long newBitmap;
		
		private final long mask0;
		private final long mask1;
		
		/**
		 * The constructor.
		 * 
		 * @param  store The store, not allowed to be {@code null}.
		 * @param  ci The column information object of the column having a
		 *         Null-info, not allowed to be {@code null}.
		 *         The column must be a column of the store's table.
		 */
		NiPresenter(WRStore store, WRColInfo ci) {
			nBM = store.nBM;
			nullBitMask = ci.nullBitMask;
			// nullBitMask != 0;
			mask0 = nullBitMask - 1;
			mask1 = ~mask0;
			
			// Compute the length of the Null-info.
			int niLen = 0;
			for (WRColInfo colInfo : store.colInfoArr) {
				if (colInfo.nullBitMask != 0) {
					niLen++;
				}
			}
			// nBM == Utils.bmLength(niLen + 1)
			// If we remove a column with a Null-info then newNiLen == niLen - 1
			// so that newBM == Utils.bmLength(niLen)
			newNBM = Utils.bmLength(niLen);
		}
		
		/**
		 * Adjusts the specified bitmap by contracting the Null-info contained in
		 * the specified bitmap.
		 * 
		 * @param  bm0 The bitmap containing the Null-info.
		 * 
		 * @return The adjusted bitmap.
		 */
		private final long contract(long bm0) {
			final long bm1 = bm0 & mask0;
			bm0 >>>= 1;
			bm0 &= mask1;
			return bm0 | bm1;
		}

		@Override
		public void rowData(byte[] flDataBlock, int offset) throws
																					FileIOException {
			bitmap = Utils.unsFromBytes(flDataBlock, offset, nBM);
			newBitmap = contract(bitmap);
		}
	}
	
	/**
	 * The NiDec presenter decrements the reference counters of the referenced
	 * rows and contracts the Null-info of the bitmap.
	 * <p>
	 * The targeted column is an INROW A[RT] column.
	 * 
	 * @author Beat Hoermann Hörmann
	 */
	private static final class NiDecPresenter extends NiPresenter {
		private final int colOff;
		private final int sizeLen;
		private final WRStore refdStore;
		private final int refLen;
		
		/**
		 * The constructor.
		 * 
		 * @param  store The store, not allowed to be {@code null}.
		 * @param  ci The column information object of the INROW A[RT] column,
		 *         not allowed to be {@code null}.
		 *         The column must be a column of the store's table.
		 */
		NiDecPresenter(WRStore store, WRColInfo ci) {
			super(store, ci);
			colOff = ci.offset;
			sizeLen = ci.sizeLen;
			refdStore = ci.refdStore;
			refLen = refdStore.nobsRowRef;
		}
		
		@Override
		public final void rowData(byte[] flDataBlock, int offset) throws
																					FileIOException {
			// Column is an INROW A[RT].
			super.rowData(flDataBlock, offset);
			if ((bitmap & nullBitMask) == 0) {
				// Stored value not null.
				int off = offset + colOff;
				final int size = (int) Utils.unsFromBytes(flDataBlock, off,sizeLen);
				off += sizeLen;
				final int end = off + size * refLen;
				while (off < end) {
					decRef(flDataBlock, off, refLen, refdStore);
					off += refLen;
				}
			}
		}
	}
	
	/**
	 * The Deref presenter decrements the reference counters of the referenced
	 * rows and deallocates any column data stored in the VL data file.
	 * <p>
	 * The targeted column is an OUTROW A[RT] column.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB2},
	 * {@linkplain WRStore.GlobalBuffer GB3}.
	 * 
	 * @author Beat Hoermann Hörmann
	 */
	private static final class DerefPresenter implements Presenter {
		private final int colOff;
		private final int len;
		private final IObjectToBytes o2b;
		private final Bag bag0;
		private final Bag bag;
		
		/**
		 * The constructor.
		 * 
		 * @param  ci The column information object of the OUTROW A[RT] column,
		 *         not allowed to be {@code null}.
		 *         The column must be a column of the store's table.
		 */
		DerefPresenter(WRColInfo ci) {
			colOff = ci.offset;
			len = ci.len;
			o2b = ci.o2b;
			bag0 = new Bag();
			bag = new Bag(len);
		}

		@Override
		public final void rowData(byte[] flDataBlock, int offset) throws
																					FileIOException {
			// Column is an OUTROW A[RT] column.
			final int off = offset + colOff;
			if (!Utils.isZero(flDataBlock, off, len)) {
				// Stored value not null.
				bag0.bytes = flDataBlock;
				bag0.offset = off;
				
				// We are interested in the side effects of the
				// IObjectToBytes.convert-method only.
				o2b.convert(null, 0, bag0, null, bag);
			}
		}
	}
	
	/**
	 * The Deallocate presenter deallocates any column data stored in the VL
	 * data file.
	 * <p>
	 * The targeted column is a VL column different from an OUTROW A[RT] column,
	 * hence, one of the columns OUTROW ST, INROW A[OUTROW ST] and OUTROW A[ST].
	 * 
	 * @author Beat Hoermann
	 */
	private static final class DeallocatePresenter implements Presenter {
		private final VLFileSpace vlFileSpace;
		private final int colOff;
		private final int len;
		private final int lengthLen;
		
		/**
		 * The constructor.
		 * 
		 * @param  store The store, not allowed to be {@code null}.
		 * @param  ci The column information object of the VL column, not allowed
		 *         to be {@code null}.
		 *         The column must be a column of the store's table.
		 */
		DeallocatePresenter(WRStore store, WRColInfo ci) {
			vlFileSpace = store.vlFileSpace;
			colOff = ci.offset;
			len = ci.len;
			lengthLen = ci.lengthLen;
		}

		@Override
		public final void rowData(byte[] flDataBlock, int offset) throws
																					FileIOException {
			// Column is a VL column.
			final int off = offset + colOff;
			if (!Utils.isZero(flDataBlock, off, len)) {
				// Stored value not null.
				final long len0 = Utils.unsFromBytes(flDataBlock, off, lengthLen);
				if (len0 > 0) {
					// Deallocate outrow data.
					vlFileSpace.deallocate(len0, null);
				}
			}
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
	 * Returns a specification for contracting the FL data blocks at the column
	 * to be removed and, depending on the value of the {@code removeRefCount}
	 * parameter, at the reference counter.
	 *
	 * @param  ci The column information object of the column to be removed, not
	 *         allowed to be {@code null}.
	 * @param  removeRefCount Indicates whether the space for the reference
	 *         counter in the FL data block must be removed or not.
	 * @param  store The store, not allowed to be {@code null}.
	 * 
	 * @return The specification, never {@code null}.
	 */
	private final Spec spec(WRColInfo ci, boolean removeRefCount,
																					WRStore store) {
		final Spec spec = FLFileAccommodate.newSpec();
		if (removeRefCount) {
			spec.spot(store.nBM, -store.nobsRefCount);
		}
		return spec.spot(ci.offset, -ci.len);
	}
	
	/**
	 * Returns a specification for updating the FL data blocks at the bitmap and,
	 * optionally, for contracting the FL data blocks at the bitmap and at the
	 * reference counter, respectively, and for contracting the FL data blocks
	 * at the column to be removed.
	 * 
	 * @param  bmu The bitmap updater, not allowed to be {@code null}.
	 * @param  ci The column info object, not allowed to be {@code null}.
	 * @param  removeRefCount Indicates whether the space for the reference
	 *         counter in the FL data block must be removed or not.
	 * @param  store The store, not allowed to be {@code null}.
	 * 
	 * @return The specification, never {@code null}.
	 */
	private final Spec spec(BitmapUpdater bmu, WRColInfo ci,
													boolean removeRefCount, WRStore store) {
		final Spec spec;
		if (store.nBM > bmu.len)
			spec = FLFileAccommodate.spot(0, -1 - (removeRefCount ?
																store.nobsRefCount : 0), bmu);
		else {
			spec = FLFileAccommodate.spot(0, bmu);
			if (removeRefCount) {
				spec.spot(store.nBM, -store.nobsRefCount);
			}
		}
		return spec.spot(ci.offset, -ci.len);
	}
	
	/**
	 * Accommodates the FL data file of the specified store with respect to the
	 * specified column to be removed.
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
	 * method throws a {@code FileIOException}.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1},
	 * {@linkplain WRStore.GlobalBuffer GB2},
	 * {@linkplain WRStore.GlobalBuffer GB3}.
	 * 
	 * @param  store The WR store, not allowed to be {@code null}.
	 * @param  col The column to be removed, not allowed to be {@code null}.
	 *         The column must be a column of the store's table.
	 * @param  removeRefCount Indicates whether the reference counter must be
	 *         removed or not.
	 *         The value must be equal to {@code true} if and only if the FL data
	 *         blocks contain a reference counter and the reference counter must
	 *         be removed.
	 * 
	 * @throws NullPointerException If one of the arguments is {@code null} or
	 *         if {@code col} is not a column of the store's table.
	 * @throws FileIOException If an I/O error occurs.
	 */
	final void run(WRStore store, Column_<?> col, boolean removeRefCount) throws
													NullPointerException, FileIOException {
		final Type_ type = col.type();
		final WRColInfo ci = store.colInfoMap.get(col);
		
		if (ci.refdStore == store) {
			// The column is a reference column, hence, a column of RT or A[RT]
			// that references its own table! Removing a reference column requires
			// a presenter to decrement the reference counters of the referenced
			// rows. However, contracting the FL data file with a presenter cannot
			// be done "in situ" but must be done by the help of a temporary file
			// which finally replaces the original FL data file. Since the
			// presenter operates on the FL data file that is just in the process
			// of being contracted it can happen that the presenter decrements the
			// reference counter of a row that is already contracted and copied
			// into the temporary file. To avoid this, we set all references to
			// null thus decrementing the references counters prior to contracting
			// the FL data file.
			store.updateAllSupplyValues(col, () -> null);
		}
		
		// Precondition:
		// removeRefCount implies column is RT or INROW A[RT] or OUTROW A[RT]
		
		if (type.scheme() == Scheme.INROW && !(type instanceof ArrayType_ &&
					((ArrayType_) type).elementType().scheme() == Scheme.OUTROW)) {
			// INROW type but not INROW A[OUTROW ST]
			if (type instanceof SimpleType<?> && !((SimpleType<?>)type).nullable())
				// Non-NULL INROW ST
				FLFileAccommodate.spot(ci.offset, -ci.len).run(store);
			else if (type instanceof RefType)
				// RT
				spec(ci, removeRefCount, store).run(new DecPresenter(ci), store);
			else {
				// Nullable INROW ST or INROW A[INROW ST] or INROW A[RT]
				final NiPresenter niP = type instanceof ArrayOfRefType ?
							new NiDecPresenter(store, ci) : new NiPresenter(store, ci);
				spec(new BitmapUpdater(niP), ci, removeRefCount, store).run(niP,
																								store);
			}
		}
		else if (type instanceof ArrayOfRefType)
			// OUTROW A[RT]
			spec(ci, removeRefCount, store).run(new DerefPresenter(ci), store);
		else {
			// OUTROW ST or INROW A[OUTROW ST] or OUTROW A[ST]
			FLFileAccommodate.spot(ci.offset, -ci.len).
											run(new DeallocatePresenter(store, ci), store);
		}
	}
}