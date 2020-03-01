/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.nio.ByteBuffer;

import acdp.exceptions.ACDPException;
import acdp.exceptions.MaximumException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.IUnit;
import acdp.internal.WriteOp;
import acdp.internal.misc.Utils_;
import acdp.misc.Utils;

/**
 * The super class of the {@code Delete} and the {@code ChangeOp} operations.
 * <p>
 * This class provides some methods not directly used in this class but in its
 * subclasses.
 *
 * @author Beat Hoermann
 */
abstract class GenericWriteOp extends WriteOp {
	/**
	 * The store.
	 */
	protected final WRStore store;
	
	/**
	 * The FL data file.
	 * Set at the beginning of the {@code body} method.
	 */
	protected final FileIO flDataFile;
	/**
	 * The VL data file.
	 * Set at the beginning of the {@code body} method.
	 */
	protected final FileIO vlDataFile;
	
	/**
	 * The constructor.
	 * 
	 * @param  store The target store of this write operation.
	 * 
	 * @throws NullPointerException If the store is {@code null}.
	 */
	protected GenericWriteOp(WRStore store) throws NullPointerException {
		super(store.table);
		this.store = store;
		flDataFile = store.flDataFile;
		vlDataFile = store.vlDataFile;
	}
	
	/**
	 * Increments the reference counter of the row with the specified row index
	 * in the specified table by the specified value.
	 * <p>
	 * Note that the current position of {@code refdStore.flDataFile} changes
	 * wich may be a problem if the store referencing {@code refdStore} is
	 * identical to {@code refdStore}.
	 * 
	 * 
	 * @param  refdStore The store of the table containing the row whose
	 *         reference counter must be incremented.
	 * @param  ri The index of the row in the referenced table.
	 * @param  n The increment, may be negative.
	 * @param  unit The unit, may be {@code null}.
	 *         
	 * @throws IllegalArgumentException if the reference behind the row index
	 *         points to a row that does not exist within the referenced table
	 *         or if the reference points to a row gap or if the stored value of
	 *         the reference counter of the referenced row is negative or if the
	 *         reference counter is negative after it has been incremented.
	 *         This execption may be due to an insert operation trying to insert
	 *         an illegal reference or due to an update operation trying to
	 *         replace an existing reference with an illegal reference.
	 *         In any other case it is very likely that the database is
	 *         corrupted due to any reason.
	 * @throws MaximumException If the maximum value of the reference counter
	 *         is exceeded.
	 * @throws UnitBrokenException If recording before data fails.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public static final void inc(WRStore refdStore, long ri, int n,
					IUnit unit) throws IllegalArgumentException, MaximumException,
														UnitBrokenException, FileIOException {
		if (ri < 1) {
			throw new IllegalArgumentException(ACDPException.prefix(
					refdStore.table) + "Row with index " + ri + " does not exist.");
		}
		final long pos = refdStore.riToPos(ri);
		
		final FileIO file = refdStore.flDataFile;
		file.open(); // file.position() == 0
		try {
			final ByteBuffer bufH = ByteBuffer.allocate(refdStore.nH);
			
			// Read header bytes.
			try {
				file.read(bufH, pos);
			} catch (FileIOException e) {
				if (e.eof()) {
					throw new IllegalArgumentException(ACDPException.prefix(
												refdStore.table) + "Row with index " + ri +
												" does not exist.");
				}
			}
			final byte[] arrH = bufH.array();
			if (arrH[0] < 0) {
				// Row Gap!
				throw new IllegalArgumentException(ACDPException.prefix(
						refdStore.table) + "Row with index " + ri + " is a row gap.");
			}
			
			// Extract value of reference counter.
			final int indexRC = refdStore.nBM;
			long rc = Utils.unsFromBytes(arrH, indexRC, refdStore.nobsRefCount);
			
			// Check stored reference counter.
			if (rc < 0) {
				// Database is corrupted.
				new IllegalArgumentException(ACDPException.prefix(
								refdStore.table) + "Reference counter of the row at " +
								"position " + pos + " is less than zero: " + rc + ".");
			}
			
			// Increment reference counter. Note that n may be negative.
			rc += n;
			
			// Check incremented reference counter.
			if (rc < 0)
				// A negativ value may result from an overflow situation due to
				// rc + n > Long.MAX_VALUE.
				new IllegalArgumentException(ACDPException.prefix(
										refdStore.table) + "Incrementing the reference " +
										"counter of row " + ri + " by " + n +
										" results in a negative value: " + rc + ".");
			else if (rc > Utils_.bnd8[refdStore.nobsRefCount]) {
				String str = "Maximum number of references for row " + ri +
																						" exceeded.";
				if (refdStore.nobsRefCount < 8) {
					str += " You may want to refactor the table with a higher " +
																"value of \"nobsRefCount\".";
				}
				throw new MaximumException(refdStore.table, str);
			}
			
			// Record before data.
			if (unit != null) {
				unit.record(file, pos + indexRC, arrH, indexRC,
																		refdStore.nobsRefCount);
			}
		
			// Write rc.
			Utils.unsToBytes(rc, refdStore.nobsRefCount, arrH, indexRC);
			bufH.position(indexRC);
			file.write(bufH, pos + indexRC);
		} finally {
			file.close();
		}
	}
	
	/**
	 * The body of this write operation, depending on the type of the write
	 * operation.
	 * Besides the listed {@code FileIOException} other exceptions may occur
	 * depending on the concrete implementation of this method.
	 * 
	 * @param  params The input parameters of this write operation.
	 * 
	 * @return The return value of this write operation.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	protected abstract Object body(Object params) throws FileIOException;
	
	@Override
	protected final Object body(Object params, IUnit unit) throws
																					FileIOException {
		// !db.isBroken() && db.isWritable() && !db.isTempReadOnly() &&
		// params != null
		this.unit = unit;
		// Since a write operation is a synchronized database operation,
		// opening a FileIO instance won't raise a ShutdownException.
		flDataFile.open();
		try {
			vlDataFile.open();
			try {
				return body(params);
			} finally {
				vlDataFile.close();
			}
		} finally {
			flDataFile.close();
		}
	}
}