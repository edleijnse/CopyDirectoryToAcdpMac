/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import acdp.exceptions.ACDPException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.ShutdownException;
import acdp.exceptions.UnitBrokenException;

/**
 * The super class of all regular write operations, hence, all regular database
 * operations that modify the content of an ACDP database.
 * <p>
 * Although, a regular write operation is invoked from a particular table,
 * updates to reference counters of rows of other tables may be part of the
 * regular write operation.
 * <p>
 * A regular write operation is launched by invoking the {@link #execute}
 * method.
 * This method ensures that regular write operations are safe for use by
 * multiple concurrent threads.
 * Read the description of the {@link SyncManager} class for more details.
 *
 * @author Beat Hoermann
 */
public abstract class WriteOp {
	protected final Database_ db;
	protected final Table_ table;
	private final FileSpaceStateTracker fssTracker;
	private final SyncManager syncManager;
	
	/**
	 * The surrounding unit.
	 * Set at the beginning of the {@code execute} method.
	 * The value is equal to <code>null</code> if and only if this write
	 * operation is a Kamikaze write.
	 */
	protected IUnit unit;
	
	/**
	 * The constructor.
	 * 
	 * @param table The target table of this write operation.
	 */
	protected WriteOp(Table_ table) {
		this.db = table.db();
		this.table = table;
		this.fssTracker = db.fssTracker();
		this.syncManager = db.syncManager();
	}
	
	/**
	 * The body of this write operation.
	 * Besides the listed {@code FileIOException} other exceptions may occur
	 * depending on the concrete implementation of this method.
	 * 
	 * @param  params The input parameters of this write operation.
	 * @param  unit The unit, may be <code>null</code>.
	 * 
	 * @return The return value of this write operation.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	protected abstract Object body(Object params, IUnit unit) throws
																					FileIOException;
	/**
	 * Executes this write operation.
	 * Besides the listed exceptions other exceptions may occur depending on the
	 * concrete implementation of the {@code body} method.
	 * 
	 * @param  params The input parameters of this write operation.
	 * 
	 * @return The return value of this write operation.
	 * 
	 * @throws NullPointerException If the database is read-only and this write
	 *         operation is a Kamikaze write.
	 * @throws ShutdownException If this write operation is a Kamikaze write
	 *         and the synchronization manager is shut down.
	 * @throws ACDPException If this write operation is called within a read
	 *         zone or an ACDP zone.
	 *	@throws UnitBrokenException If this write operation is executed within a
	 *         unit and the unit is broken.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	public final Object execute(Object params) throws NullPointerException,
													ShutdownException, ACDPException,
													UnitBrokenException, IOFailureException {
		// 1. If DB is read-only then it follows that this write operation is a
		//    Kamikaze write and the following method throws a
		//    NullPointerException. Note, however, that the execution of a write
		//    operation throws almost for sure an UnsupportedOperationException
		//    far before this point.
		// 2. If DB is writable and if this write operation is executed within a
		//    unit then it follows that the ACDP zone is closed. Note that read
		//    zones are allowed within units.
		// 3. If DB is writable and if this write operation is a Kamikaze write
		//    then the ACDP zone may be open.
		Unit_ unit = syncManager.getUnit();
		// db.isWritable()
		final long id = Thread.currentThread().getId();
		// Write operations are not allowed in read zones, even if the read zone
		// is within a unit.
		if (syncManager.readZoneOpenedInThread(id)) {
			throw new ACDPException(table, "Write operation invoked within a " +
																						"read zone.");
		}
		if (unit == null) {
			// This write operation is executed outside a unit and is therefore a
			// Kamikaze write. If the ACDP zone is open and it was opened in the
			// current thread then invoking syncManager.block() would result in a
			// deadlock. Throw an exception instead.
			if (syncManager.acdpZoneOpenedInThread(id)) {
				throw new ACDPException(table, "Kamikaze write invoked " +
																		"within the ACDP zone.");
			}
			// There may exist one or more open read zones in threads different
			// from the current thread or the ACDP zone may be open in a thread
			// different from the current thread or the unit may be issued in a
			// thread different from the current thread or there is currently
			// running another Kamikaze write. Wait until the synchroniziation
			// manager is again unblocked.
			syncManager.block();
			// !syncManager.shutdown() && db.isWritable() && no open read zone &&
			// ACDP zone closed && unit closed && no other Kamikaze write is
			// running
		}
		else {
			// This write operation is executed within a unit. It is not impossible
			// but quite unusual that the unit is broken at that time. Note that
			// this write operation will be executed even if the synchronization
			// manager is shut down.
			unit.ensureNotBroken();
		}
		// (unit != null implies unit is not broken) && db.isWritable() &&
		// no open read zone && ACDP zone closed && no other Kamikaze write is
		// running
		try {
			final Object out = body(params, unit);
			if (unit == null) {
				// Kamikaze write: "commit".
				fssTracker.writeStates();
				fssTracker.clearPristineStates();
			}
			return out;
		} catch (FileIOException e) {
			throw new IOFailureException(table, e);
		}
		finally {
			if (unit == null) {
				// Kamikaze write
				syncManager.unblock();
			}
		}
	}
}