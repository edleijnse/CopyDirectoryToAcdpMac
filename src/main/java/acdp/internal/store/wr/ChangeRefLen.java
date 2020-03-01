/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.nio.ByteBuffer;

import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.MaximumException;
import acdp.internal.Buffer;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.Table_;
import acdp.internal.misc.Utils_;
import acdp.internal.store.Bag;
import acdp.internal.store.wr.FLFileAccommodate.Presenter;
import acdp.internal.store.wr.FLFileAccommodate.Spec;
import acdp.internal.store.wr.FLFileAccommodate.Updater;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.internal.types.AbstractArrayType;
import acdp.misc.Utils;
import acdp.types.RefType;
import acdp.types.Type;
import acdp.types.Type.Scheme;

/**
 *
 *
 * @author Beat Hoermann
 */
/**
 * @author Beat Hoermann Hörmann
 *
 */
final class ChangeRefLen {
	/**
	 * The presenter.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class P implements Presenter {
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
		int offBM;
		/**
		 * The position within the byte array where the current FL column data
		 * starts.
		 * <p>
		 * This value should be considered as read-only.
		 */
		int offCD;
		
		/**
		 * The position where the FL column data starts within the FL data block.
		 */
		private final int colOff;
		
		/**
		 * The constructor.
		 * 
		 * @param ci The column information object of the column to be modified
		 *        before modification, not allowed to be {@code null}.
		 */
		P(WRColInfo ci) {
			colOff = ci.offset;
		}
		
		@Override
		public final void rowData(byte[] flDataBlock, int offset) throws
																					FileIOException {
			this.flDataBlock = flDataBlock;
			offBM = offset;
			offCD = offset + colOff;
		}
	}
	
	/**
	 * A reference updater reads the byte representation of a reference from
	 * a byte array, contracts or expands the byte representation and copies
	 * the contracted or expanded byte representation into a target byte array.
	 * 
	 * @author Beat Hoermann
	 */
	private interface RefUpdater {
		/**
		 * Updates the byte representation of a reference.
		 * 
		 * @param bytes0 The byte array containing the byte representation of the
		 *        reference.
		 * @param off0 The position within {@code bytes0} where the byte
		 *        representation of the reference starts.
		 * @param bytes1 The target byte array.
		 * @param off1 The position within the target byte array where the
		 *        changed byte representation of the reference must be copied
		 *        to.
		 */
		void update(byte[] bytes0, int off0, byte[] bytes1, int off1);
	}
	
	/**
	 * A reference updater that contracts the byte representation of a
	 * reference.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class RefC implements RefUpdater {
		final int d;
		final int refLen1;
		
		/**
		 * The constructor.
		 * <p>
		 * It is assumed that {@code refLen0} is greater than {@code refLen1}.
		 * 
		 * @param refLen0 The original length of the byte representation of a
		 *        reference, must be greater than 0 and less than or equal
		 *        to 8.
		 * @param refLen1 The new length of the byte representation of a
		 *        reference, must be greater than 0 and less than or equal
		 *        to 8.
		 */
		RefC(int refLen0, int refLen1) {
			// refLen0 > refLen1
			d = refLen0 - refLen1;
			this.refLen1 = refLen1;
		}
		
		@Override
		public final void update(byte[] bytes0, int off0, byte[] bytes1,
																						int off1) {
			System.arraycopy(bytes0, off0 + d, bytes1, off1, refLen1);
		}
	}
	
	/**
	 * A reference updater that expands the byte representation of a
	 * reference.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class RefE implements RefUpdater {
		final int d;
		final int refLen0;
		
		/**
		 * The constructor.
		 * <p>
		 * It is assumed that {@code refLen0} is less than {@code refLen1}.
		 * 
		 * @param refLen0 The original length of the byte representation of a
		 *        reference, must be greater than 0 and less than or equal
		 *        to 8.
		 * @param refLen1 The new length of the byte representation of a
		 *        reference, must be greater than 0 and less than or equal
		 *        to 8.
		 */
		RefE(int refLen0, int refLen1) {
			// refLen0 < refLen1
			d = refLen1 - refLen0;
			this.refLen0 = refLen0;
		}
		
		@Override
		public final void update(byte[] bytes0, int off0, byte[] bytes1,
																						int off1) {
			final int end = off1 + d;
			while (off1 < end) {
				bytes1[off1++] = 0;
			}
			
			System.arraycopy(bytes0, off0, bytes1, off1, refLen0);
		}
	}
	
	/**
	 * This column data updater updates the current column data of an INROW
	 * A[RT] column.
	 * <p>
	 * It is assumed that the length of the references and thus the length of
	 * the entire column data changes.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class InrowUpdater extends Updater {
		private final int nBM;
		private final long nullBitMask;
		private final int sizeLen;
		private final int refLen0;
		private final int refLen1;
		private final RefUpdater ru;
		private final P p;

		/**
		 * The constructor.
		 * 
		 * @param store The WR store, not allowed to be {@code null}.
		 * @param ci The column information object, not allowed to be {@code
		 *        null}.
		 *        The column must be an INROW A[RT] column.
		 * @param newLen The new length of the column data.
		 * @param newRefLen The new length of the byte representation of a
		 *        reference.
		 * @param p The presenter, not allowed to be {@code null}.
		 */
		InrowUpdater(WRStore store, WRColInfo ci, int newLen, int newRefLen,
																								P p) {
			super(newLen);
			nBM = store.nBM;
			nullBitMask = ci.nullBitMask;
			sizeLen = ci.sizeLen;
			refLen0 = ci.refdStore.nobsRowRef;
			refLen1 = newRefLen;
			// refLen0 != refLen1 by assumption.
			ru = refLen0 > refLen1 ? new RefC(refLen0, refLen1) :
																	new RefE(refLen0, refLen1);
			this.p = p;
		}

		@Override
		final void newData(byte[] bytes, int offset) {
			final byte[] flDataBlock = p.flDataBlock;
			final int offCD = p.offCD;
			
			if ((Utils.unsFromBytes(flDataBlock, p.offBM, nBM) &
																			nullBitMask) == 0) {
				// Array value is not null.
				// Update array size.
				System.arraycopy(flDataBlock, offCD, bytes, offset, sizeLen);
				// Compute array size.
				final int size = (int) Utils.unsFromBytes(flDataBlock, offCD,
																							sizeLen);
				if (size > 0) {
					int off1 = offset + sizeLen;
					final int start = offCD + sizeLen;
					final int end = start + size * refLen0;
					for (int off0 = start; off0 < end; off0 += refLen0) {
						ru.update(flDataBlock, off0, bytes, off1);
						off1 += refLen1;
					}
				}
			}
		}
	}
	
	/**
	 * This column data updater updates the current column data of an OUTROW
	 * A[RT] column.
	 * <p>
	 * It is assumed that the length of the references changes.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB2},
	 * {@linkplain WRStore.GlobalBuffer GB3}.
	 * 
	 * @author Beat Hoermann
	 */
	private static abstract class OutrowUpdater extends Updater {
		private final Buffer gb2;
		private final Buffer gb3;
		protected final FileIO vlDataFile;
		protected final int nobsOutrowPtr;
		private final int lengthLen0;
		private final int lengthLen1;
		private final int sizeLen;
		private final int refLen0;
		private final int refLen1;
		private final byte[] ref1;
		private final Bag bag;
		private final RefUpdater ru;
		private final P p;
		
		/**
		 * The constructor.
		 * 
		 * @param store The WR store, not allowed to be {@code null}.
		 * @param ci The column information object, not allowed to be {@code
		 *        null}.
		 *        The column must be an OUTROW A[RT] column.
		 * @param newLengthLen The new length of the length field of the column
		 *        data.
		 * @param newRefLen The new length of the byte representation of a
		 *        reference.
		 * @param ru The reference updater, not allowed to be {@code null}.
		 * @param p The presenter, not allowed to be {@code null}.
		 */
		protected OutrowUpdater(WRStore store, WRColInfo ci, int newLengthLen,
														int newRefLen, RefUpdater ru, P p) {
			super(newLengthLen + store.nobsOutrowPtr);
			gb2 = store.gb2;
			gb3 = store.gb3;
			vlDataFile = store.vlDataFile;
			nobsOutrowPtr = store.nobsOutrowPtr;
			lengthLen0 = ci.lengthLen;
			lengthLen1 = newLengthLen;
			sizeLen = ci.sizeLen;
			refLen0 = ci.refdStore.nobsRowRef;
			refLen1 = newRefLen;
			// refLen0 != refLen1 by assumption.
			ref1 = new byte[refLen1];
			bag = new Bag();
			this.ru = ru;
			this.p = p;
		}
		
		/**
		 * Creates a writer with the specified buffer.
		 * 
		 * @param  buffer The buffer, not allowed to be {@code null}.
		 * @param  ptr0 The pointer to the stored byte representation of the
		 *         array value.
		 * 
		 * @return The created writer, never {@code null}.
		 * 
		 * @throws FileIOException If an I/O error occurs.
		 */
		protected abstract AbstractWriter newWriter(Buffer buffer, long ptr0)
																			throws FileIOException;
		
		/**
		 * Finalizing comprises the deallocation of unused outrow data as well as
		 * updating the FL column data by the length of and the pointer to the
		 * new outrow data.
		 * 
		 * @param length0 The length of the original byte representation of the
		 *        array value.
		 * @param length1 The new length of the byte representation of the array
		 *        value.
		 * @param lengthLen1 The new length of the length field of the column
		 *        data.
		 * @param bytes The byte array to change, never {@code null}.
		 * @param offset The position where changing the contents of the byte
		 *        array should take place.
		 */
		protected abstract void finalize(long length0, long length1,
												int lengthLen1, byte[] bytes, int offset);

		@Override
		final void newData(byte[] bytes, int offset) throws FileIOException {
			final byte[] flDataBlock = p.flDataBlock;
			final int offCD = p.offCD;
			
			// Copy Pointer. Note that for an empty array length0 will be zero but
			// the pointer will be greater than zero. Note also that the pointer
			// remains unchanged if the length of the references is reduced.
			System.arraycopy(flDataBlock, offCD + lengthLen0, bytes, offset +
																	lengthLen1, nobsOutrowPtr);
			// Extract length.
			final long length0 = Utils.unsFromBytes(flDataBlock, offCD,lengthLen0);
			if (length0 > 0) {
				// Array value is not null and has at least one element.
				final long ptr0 = Utils.unsFromBytes(flDataBlock, offCD +
																	lengthLen0, nobsOutrowPtr);
				// Create the streamer.
				final IStreamer sr = new FileStreamer(vlDataFile, ptr0, length0,
																						gb2, false);
				// Create the writer.
				final AbstractWriter wr = newWriter(gb3, ptr0);
				// Get the size of the stored array value. The size is greater than
				// zero.
				sr.pull(sizeLen, bag);
				// Write size.
				wr.write(bag.bytes, bag.offset, sizeLen);
				// Compute size.
				final int size = (int) Utils.unsFromBytes(bag.bytes, bag.offset,
																							sizeLen);
				long length1 = sizeLen;
				for (int i = 0; i < size; i++) {
					sr.pull(refLen0, bag);
					ru.update(bag.bytes, bag.offset, ref1, 0);
					wr.write(ref1, 0, refLen1);
					length1 += refLen1;
				}
				wr.flush();
				
				finalize(length0, length1, lengthLen1, bytes, offset);
			}
		}
	}
	
	/**
	 * This outrow updater updates the current column data of an OUTROW A[RT]
	 * column.
	 * <p>
	 * It is assumed that the new length of a reference is smaller than the
	 * original length.
	 * This means that the references need to be contracted (see the "c" in the
	 * class name).
	 * Furthermore, the VL memory space of the stored byte represenations of
	 * the array values can be reused.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB2},
	 * {@linkplain WRStore.GlobalBuffer GB3}.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class OUc extends OutrowUpdater {
		private final VLFileSpace vlFileSpace;
		
		/**
		 * The constructor.
		 * 
		 * @param store The WR store, not allowed to be {@code null}.
		 * @param ci The column information object, not allowed to be {@code
		 *        null}.
		 *        The column must be an OUTROW A[RT] column.
		 * @param newLengthLen The new length of the length field of the column
		 *        data.
		 * @param newRefLen The new length of the byte representation of a
		 *        reference.
		 * @param p The presenter, not allowed to be {@code null}.
		 */
		OUc(WRStore store, WRColInfo ci, int newLengthLen, int newRefLen, P p) {
			super(store, ci, newLengthLen, newRefLen,
										new RefC(ci.refdStore.nobsRowRef, newRefLen), p);
			vlFileSpace = store.vlFileSpace;
		}

		/**
		 * Writes to the VL data file starting at the position given by the
		 * {@code pos} argument of the constructor.
		 * 
		 * @author Beat Hoermann
		 */
		private final class Writer extends AbstractWriter {
			/**
			 * The constructor.
			 * 
			 * @param  buffer The buffer to apply, not allowed to be {@code null}.
			 * @param  pos The position where to start writing within the VL
			 *         data file.
			 *
			 * @throws FileIOException If an I/O error occurs.
			 */
			Writer(Buffer buffer, long pos) throws FileIOException {
				super(40, buffer);
				vlDataFile.position(pos);
			}
			
			/**
			 * Writes the data contained in the specified buffer to the VL data
			 * file at the files current position.
			 * 
			 * @param  buf The buffer.
			 * 
			 * @throws FileIOException If an I/O error occurs.
			 */
			@Override
			protected final void save(ByteBuffer buf) throws FileIOException {
				vlDataFile.write(buf);
			}
		}
		
		@Override
		protected final AbstractWriter newWriter(Buffer buffer, long ptr0) throws
																					FileIOException {
			return new Writer(buffer, ptr0);
		}

		@Override
		protected final void finalize(long length0, long length1, int lengthLen1,
																	byte[] bytes, int offset) {
			// length0 > length1
			vlFileSpace.deallocate(length0 - length1, null);
			Utils.unsToBytes(length1, lengthLen1, bytes, offset);
			// The pointer remains unchanged. It is already contained in bytes.
		}
	}
	
	/**
	 * This outrow updater updates the current column data of an OUTROW A[RT]
	 * column.
	 * <p>
	 * It is assumed that the new length of a reference is greater than the
	 * original length.
	 * This means that the references need to be expanded (see the "e" in the
	 * class name).
	 * Note that the VL memory space of the stored byte represenations of the
	 * array can't be reused.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB2},
	 * {@linkplain WRStore.GlobalBuffer GB3}.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class OUe extends OutrowUpdater {
		private final Table_ table;
		private final VLFileSpace vlFileSpace;
		private Writer wr;
		
		/**
		 * The constructor.
		 * 
		 * @param store The WR store, not allowed to be {@code null}.
		 * @param ci The column information object, not allowed to be {@code
		 *        null}.
		 *        The column must be an OUTROW A[RT] column.
		 * @param newLengthLen The new length of the length field of the column
		 *        data.
		 * @param newRefLen The new length of the byte representation of a
		 *        reference.
		 * @param p The presenter, not allowed to be {@code null}.
		 */
		OUe(WRStore store, WRColInfo ci, int newLengthLen, int newRefLen, P p) {
			super(store, ci, newLengthLen, newRefLen,
										new RefE(ci.refdStore.nobsRowRef, newRefLen), p);
			table = store.table;
			vlFileSpace = store.vlFileSpace;
			wr = null;
		}
		
		/**
		 * Writes to the VL data file starting at a position which is given by
		 * a call to the {@code VLFileSpace.allocate} method.
		 * 
		 * @author Beat Hoermann
		 */
		private final class Writer extends AbstractWriter {
			private long ptr;
			private boolean firstSave;
			
			Writer(Buffer buffer) {
				super(40, buffer);
				ptr = 0;
				firstSave = true;
			}
			
			/**
			 * Writes the data contained in the specified buffer to the VL data
			 * file starting at the position returned by the first call to the
			 * {@code VLFileSpace.allocate} method.
			 * <p>
			 * Note that the {@code VLFileSpace.allocate} method returns memory
			 * blocks that subsequently follow each other without a gap.
			 * 
			 * @param  buf The buffer.
			 * 
			 * @throws MaximumException If this writer wants to save the buffer
			 *         beyond the maximum allowed position.
			 * @throws FileIOException If an I/O error occurs.
			 */
			@Override
			protected final void save(ByteBuffer buf) throws MaximumException,
																					FileIOException {
				if (firstSave) {
					ptr = vlFileSpace.allocate(buf.limit(), null);
					vlDataFile.position(ptr);
					if (ptr > Utils_.bnd8[nobsOutrowPtr]) {
						String str = "Maximum size of VL data file exceeded! " +
												"Compact the VL file space of the table!";
						if (nobsOutrowPtr < 8) {
							str += " You may want to refactor the table with a " +
														"higher value of \"nobsOutrowPtr\".";
						}
						throw new MaximumException(table, str);
					}
					firstSave = false;
				}
				else {
					vlFileSpace.allocate(buf.limit(), null);
				}
				vlDataFile.write(buf);
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
		
		@Override
		protected final AbstractWriter newWriter(Buffer buffer, long ptr0) {
			wr = new Writer(buffer);
			return wr;
		}

		@Override
		protected final void finalize(long length0, long length1, int lengthLen1,
																	byte[] bytes, int offset) {
			// length0 < length1
			vlFileSpace.deallocate(length0, null);
			Utils.unsToBytes(length1, lengthLen1, bytes, offset);
			Utils.unsToBytes(wr.ptr(), nobsOutrowPtr, bytes, offset + lengthLen1);
		}
		
	}
	
	/**
	 * Accommodates the FL data file of the store specified by the {@code store}
	 * argument with respect to all columns referencing the store specified by
	 * the {@code refdStore} argument and the specified changed value of the
	 * {@code nobsRowRef} parameter of {@code refdStore}.
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
	 * @param  store The WR store, not allowed to be {@code null}.
	 * @param  refdStore The store referenced by at least one of the columns
	 *         of {@code store}, not allowed to be {@code null}.
	 * @param  newNobsRowRef The new value of the {@code nobsRowRef} parameter
	 *         of {@code refdStore}, must be greater than 0 and less than or
	 *         equal to 8.
	 *         Furthermore, {@code newNobsRowRef} is not allowed to be equal
	 *         to {@code refdStore.nobsRowRef}.
	 * 
	 * @throws NullPointerException If one of the arguments not allowed to be
	 *         {@code null} is {@code null}.
	 * @throws MaximumException If a new memory block in the VL file space
	 *         must be allocated and its file position exceeds the maximum
	 *         allowed position.
	 * @throws ImplementationRestrictionException If the number of bytes needed
	 *         to persist an array of references in an inrow column exceeds
	 *         Java's maximum array size.
	 * @throws FileIOException If an I/O error occurs.
	 */
	final void run(WRStore store, WRStore refdStore, int newNobsRowRef) throws
									NullPointerException, MaximumException,
									ImplementationRestrictionException, FileIOException {
		final Spec spec = FLFileAccommodate.newSpec();
		P p = null;
		for (WRColInfo ci : store.colInfoArr) {
			if (ci.refdStore == refdStore) {
				// Column references refdStore.
				final Type t = ci.col.type();
				if (t instanceof RefType)
					// RT
					// Add the contraction-only spot.
					spec.spot(ci.offset, newNobsRowRef - refdStore.nobsRowRef);
				else {
					// Create the presenter.
					p = new P(ci);
					// A[RT]
					// Compute new maximum length of the byte representation.
					final long newLen = (long) ci.sizeLen + newNobsRowRef *
															((AbstractArrayType) t).maxSize();
					final int cLen;
					final Updater u;
					if (t.scheme() == Scheme.INROW) {
						// INROW A[RT]
						if (newLen > Integer.MAX_VALUE) {
							throw new ImplementationRestrictionException(store.table,
									ci.col.name(), "The number of bytes needed to " +
									"persist a value in this column being of an inrow " +
									"array of references type exceeds Java\'s maximum " +
									"array size: " + newLen + ".");
						}
						final int nl = (int) newLen;
						// Set cLen and u.
						cLen = nl - ci.len;
						u = new InrowUpdater(store, ci, nl, newNobsRowRef, p);
					}
					else {
						// OUTROW A[RT]
						final int newLengthLen = Utils.lor(newLen);

						// Set cLen and u.
						cLen = newLengthLen - ci.lengthLen;
						u = newNobsRowRef < refdStore.nobsRowRef ?
									new OUc(store, ci, newLengthLen, newNobsRowRef, p) :
									new OUe(store, ci, newLengthLen, newNobsRowRef, p);
					}
					// Add the update spot which, at the same time, may be a
					// contraction spot.
					spec.spot(ci.offset, cLen, u);
				}
			}
		}
		
		spec.run(p, store);
	}
}
