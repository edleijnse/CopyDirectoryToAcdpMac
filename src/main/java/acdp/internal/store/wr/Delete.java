/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import acdp.Ref;
import acdp.exceptions.ACDPException;
import acdp.exceptions.DeleteConstraintException;
import acdp.exceptions.IllegalReferenceException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.FileIOException;
import acdp.internal.IUnit;
import acdp.internal.Ref_;
import acdp.internal.store.Bag;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.internal.types.ArrayType_;
import acdp.internal.types.RefType_;
import acdp.internal.types.Type_;
import acdp.misc.Utils;
import acdp.types.Type.Scheme;

/**
 * The delete operation deletes an existing row from a given store.
 * Note that deleting a non-existing row or a row which is referenced by at
 * least one foreign row raises an exception.
 *
 * @author Beat Hoermann
 */
final class Delete extends GenericWriteOp {
	/**
	 * The constructor.
	 * 
	 * @param store The store from which the row must be deleted.
	 */
	Delete(WRStore store) {
		super(store);
	}
	
	/**
	 * Reads the references from the specified streamer and decrements the
	 * reference counters of the referenced rows.
	 * <p>
	 * Note that a reference may occur more than once in the array of references
	 * read by the specified streamer.
	 * 
	 * @param  sr The streamer delivering the byte representation of the stored
	 *         array of references, not allowed to be {@code null}.
	 * @param  ci The column info object, not allowed to be {@code null}.
	 *         The column must be an A[RT] column.
	 * @param  unit The unit, may be {@code null}.
	 *         
	 * @throws UnitBrokenException If recording before data fails.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private static final void decRCsOfArray(IStreamer sr, WRColInfo ci,
																				IUnit unit) throws
														UnitBrokenException, FileIOException {
		final WRStore refdStore = ci.refdStore;
		
		// Map: rowIndex -> increment.
		final Map<Long, Integer> decMap = new HashMap<>();
		
		// Get references and put them into the decMap.
		final Bag bag = new Bag();
		sr.pull(ci.sizeLen, bag);
		final int size = (int) Utils.unsFromBytes(bag.bytes, bag.offset,
																						ci.sizeLen);
		final int len = refdStore.nobsRowRef;
		for (int k = 0; k < size; k++) {
			sr.pull(len, bag);
			final long rowIndex = Utils.unsFromBytes(bag.bytes, bag.offset, len);
			if (rowIndex > 0) {
				// Put row index into decMap.
				Integer incVal = decMap.get(rowIndex);
				if (incVal == null)
					decMap.put(rowIndex, -1);
				else {
					decMap.put(rowIndex, incVal - 1);
				}
			}
		}
		
		// Updater reference counters of referenced rows.
		for (Entry<Long, Integer> entry : decMap.entrySet()) {
			GenericWriteOp.inc(refdStore, entry.getKey(), entry.getValue(), unit);
		}
	}
	
	/**
	 * Decrements the reference(s) stored in the specified column of the
	 * specified row.
	 * <p>
	 * The column is assumed to be an RT or an A[RT] column.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB2}.
	 * 
	 * @param  flData The FL data, not allowed to be {@code null}.
	 * @param  type The type of the column.
	 * @param  ci The column info object, not allowed to be {@code null}.
	 *         The column must be an RT or an A[RT] column.
	 * @param  store The store, not allowed to be {@code null}.
	 * @param  unit The unit, may be {@code null}.
	 * 
	 * @throws UnitBrokenException If recording before data fails.
	 * @throws FileIOException If an I/O error occurs.
	 */
	static final void decRCs(byte[] flData, Type_ type, WRColInfo ci,
															WRStore store, IUnit unit) throws
														UnitBrokenException, FileIOException {
		if (type instanceof RefType_) {
			final WRStore refdStore = ci.refdStore;
			long rowIndex = Utils.unsFromBytes(flData, ci.offset,
																			refdStore.nobsRowRef);
			if (rowIndex > 0) {
				GenericWriteOp.inc(refdStore, rowIndex, -1, unit);
			}
		}
		else {
			// AofRT
			if (type.scheme() == Scheme.INROW)
				decRCsOfArray(new ArrayStreamer(flData, ci.offset), ci, unit);
			else {
				final long length = Utils.unsFromBytes(flData, ci.offset,
																						ci.lengthLen);
				if (length > 0) {
					decRCsOfArray(new FileStreamer(store.vlDataFile,
							Utils.unsFromBytes(flData, ci.offset + ci.lengthLen,
							store.nobsOutrowPtr), length, store.gb2, true), ci, unit);
				}
			}
		}
	}
	
	/**
	 * Decrements the reference counters of all foreign rows referenced by this
	 * row and deallocates all outrow data held by this row.
	 * <p>
	 * This method assumes that the database is not corrupted.
	 * However, if the database is corrupted then this method may throw an
	 * exception that is not listed below.
	 * <p>
	 * Having a picture in front of you depicting the different storage layouts,
	 * definitely helps understanding the source code of this method.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB2}.
	 * 
	 * @param  flData The FL data, not allowed to be {@code null}.
	 * 
	 * @throws UnitBrokenException If recording before data fails.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void release(byte[] flData) throws UnitBrokenException,
																					FileIOException {
		for (WRColInfo ci : store.colInfoArr) {
			// Nothing to do if type of column is INROW ST or INROW AT<INROW ST>.
			// Note that a column of type OUTROW AofRT requires both: decrementing
			// the reference counters and deallocating the variable length array
			// data.
			
			final Type_ type = ci.col.type();
			
			// Decrement reference counters if necessary.
			if (ci.refdStore != null) {
				// RT || AofRT
				decRCs(flData, type, ci, store, unit);
			}
			
			// Deallocate outrow data if necessary.
			if (type.scheme() == Scheme.OUTROW || type instanceof ArrayType_ &&
						((ArrayType_) type).elementType().scheme() == Scheme.OUTROW) {
				// OUTROW ST || OUTROW AAT || INROW AT<OUTROW ST>
				final long length = Utils.unsFromBytes(flData, ci.offset,
																						ci.lengthLen);
				if (length > 0) {
					store.vlFileSpace.deallocate(length, unit);
				}
			}
		}
	}
	
	/**
	 * Deletes an existing row from the table.
	 * Note that deleting a non-existing row or a row which is referenced by at
	 * least one foreign row raises an exception.
	 * <p>
	 * This method assumes that the database is not corrupted.
	 * However, if the database is corrupted then this method may throw an
	 * exception that is not listed below.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB2}.
	 * 
	 * @param  ref The reference to the row to delete from the table.
	 * 
	 * @return The {@code null} value.
	 * 
	 * @throws NullPointerException If {@code ref} is {@code null}.
	 * @throws DeleteConstraintException If the row to delete is referenced by
	 *         at least one foreign row.
	 * @throws IllegalReferenceException If the reference points to a row that
	 *         does not exist within the table or if the reference points to a
	 *         row gap.
	 *         This exception never occurs if the reference is a {@linkplain
	 *         Ref valid} reference.
	 * @throws UnitBrokenException If recording before data fails.
	 * @throws FileIOException If an I/O error occurs.
	 */
	@Override
	protected final Object body(Object ref) throws NullPointerException,
									IllegalArgumentException, IllegalReferenceException,
														UnitBrokenException, FileIOException {
		final Ref_ rowRef = (Ref_) Objects.requireNonNull(ref,
									ACDPException.prefix(table) + "Reference is null.");
		// Convert reference to row position.
		final long pos = store.riToPos(store.refToRi(rowRef));
		
		// Read row.
		final ByteBuffer buf = ByteBuffer.allocate(store.n);
		flDataFile.read(buf, pos);
		final byte[] flData = buf.array();
		if (flData[0] < 0) {
			// Row Gap!
			throw new IllegalReferenceException(table, rowRef, true);
		}

		// Check if row is referenced by another row.
		if (store.nobsRefCount > 0) {
			final long rc = Utils.unsFromBytes(flData, store.nBM,
																				store.nobsRefCount);
			if (rc > 0) {
				throw new DeleteConstraintException(table, "Row " + rowRef +
										" can't be deleted because it is referenced by " +
										rc + " foreign row(s).");
			}
		}
		
		// Deallocate outrow data and handle references to foreign rows.
		release(flData);
		
		// Record before data.
		if (unit != null) {
			unit.record(flDataFile, pos, flData);
		}
		
		// Deallocate row.
		store.flFileSpace.deallocate(pos);
		
		return null;
	}
}