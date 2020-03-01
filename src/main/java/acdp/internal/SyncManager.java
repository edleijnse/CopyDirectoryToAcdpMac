/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import java.io.OutputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import acdp.ReadZone;
import acdp.Unit;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CreationException;
import acdp.exceptions.ShutdownException;
import acdp.exceptions.UnitBrokenException;

/**
 * The synchronization manager of the database.
 * The synchronization manager provides methods which force serial execution
 * of potentially conflicting database operations executed in different threads.
 * 
 * <h1>Blocking</h1>
 * The synchronization manager is either <em>blocked</em> or <em>unblocked</em>.
 * If it is blocked then there exists exactly one thread, the current thread,
 * that can execute a number of (single-threaded) <em>synchronized database
 * operations</em>.
 * (Synchronized database operations are defined below.)
 * Other synchronized database operations in threads different from the current
 * thread must wait until the current thread unblocks the synchronization
 * manager.
 * There are four different ways to block or unblock the synchronization
 * manager:
 * 
 * <ol>
 *    <li>Invoke the {@link #block} or {@link #unblock} method.</li>
 *    <li>Let the synchronization manager issue the {@linkplain Unit_ unit}.
 *        Unblock the sychronization manager by closing the unit.</li>
 *    <li>Open or close a <em>read zone</em>.</li>
 *    <li>Open or close the <em>ACDP zone</em>.</li>
 * </ol>
 * 
 * <h1>The Unit</h1>
 * If the database is writable then the synchronization manager internally
 * houses a single instance of the {@link Unit_} class.
 * A client requests the unit invoking the {@link #issueUnit} method and
 * "returns" it invoking the {@link Unit_#close} method.
 * As long as the unit is issued to a client the synchronization manager is
 * blocked so that threads requesting the unit or threads going to open a read
 * zone (see below) or the ACDP zone (see below) or threads going to execute a
 * Kamikaze write (see below), all waiting on the synchronization manager being
 * unblocked, are delayed until the client returns the unit.
 * The unit can only be issued if the snchronization manager is unblocked
 * unless a client requests the unit from within the already issued unit.
 * In such a case the synchronization manager immediately reissues the unit,
 * despite the fact that the synchronization manager is already blocked.
 * Statements executed within such a unit are said to be executed within a
 * <em>nested</em> unit.
 * <p>
 * It follows that any two database operations within two separate units are
 * serially executed.
 * To preserve data integrity from the malicious effects of concurrent writes,
 * write operations <em>must</em> test if they are invoked within a unit and if
 * not <em>must</em> block the synchronization manager thus forcing serial
 * execution of <em>all</em> write operations.
 * We call a write operation executed outside a unit a <em>Kamikaze write</em>.
 * (Kamikaze writes outperform write operations executed within a unit but
 * since they cannot be rolled back, they pose a threat to the integrity of the
 * persisted data.)
 * Requesting the unit inside a read zone or inside the ACDP zone raises an
 * exception.
 * 
 * <h1>Read Zones</h1>
 * Read-only operations can not harm the integrity of the data persisted in the
 * database, but in the presence of concurrent writes they still may suffer
 * from inconsistent views and therefore may result in <em>dirty reads</em>.
 * To completely prevent inconsistent views, even across transaction
 * boundaries (!), invoke read-only operations within a <em>read zone</em>.
 * (Of course, a read zone must be "wide enough" to cope with all kinds of view
 * inconsistencies.
 * Explaining all kinds of view inconsistencies is beyond the scope of this
 * class description.)
 * Invoking the {@link #openReadZone} and {@link #closeReadZone} methods opens
 * and closes a read zone, respectively.
 * As long as at least one read zone is open (several read zones may be
 * open in several threads at the same time) the synchronization manager
 * is blocked, so that threads going to open the ACDP zone (see below) or
 * threads requesting the unit or threads going to execute a Kamikaze write,
 * all waiting on the synchronization manager being unblocked, are delayed
 * until all read zones are closed.
 * A read zone can only be opened if another read zone is already open or if
 * the synchronization manager is unblocked.
 * <em>Nested</em> read zones are allowed.
 * (Nested read zones are read zones that are opened and closed within a read
 * zone.)
 * Opening a read zone inside a unit or inside the ACDP zone (see below) is
 * allowed, although opening a read zone within the ACDP zone is recommended
 * only if the integrity of the persisted data can be taken for granted at the
 * time the read zone is entered.
 * Write operations <em>must</em> throw an exception if they are invoked within
 * a read zone.
 * <p>
 * Although read-only operations executed within read zones of different threads
 * are executed concurrently, read zones prevent any write operation from being
 * executed in parallel.
 * If you think that read zones take out too much potential of a performance
 * gain due to concurrency, you may wish to avoid read zones.
 * In such a case, apply your own strategy to cope with inconsistent views on
 * a layer above ACDP.
 * 
 * <h1>ACDP zone</h1>
 * The ACDP zone enables some database service operations to be executed within
 * a running session.
 * Thus, a service operation, as for example, compacting the persisted data
 * of a table can be executed at any time during a session:
 * There is no need for closing the database and reopening it in order to
 * produce a session which is exclusively reserved for a particular service
 * operation.
 * Invoking the {@link #openACDPZone} and {@link #closeACDPZone} methods opens
 * and closes the ACDP zone, respectively.
 * As long as the ACDP zone is open the synchronization manager is blocked,
 * so that threads going to open a read zone or the ACDP zone or threads
 * requesting the unit or threads going to execute a Kamikaze write, all waiting
 * on the synchronization manager being unblocked, are delayed until the ACDP
 * zone is closed.
 * The ACDP zone can only be opened if the synchronization manager is unblocked.
 * <em>Nesting</em> by opening and closing the ACDP zone within the ACDP zone
 * is allowed to any depth.
 * Opening the ACDP zone within a read zone or a unit raises an exception.
 * Kamikaze writes <em>must</em> throw an exception if they are invoked within
 * the ACDP zone.
 * A read-only operation is not required to throw an exception if it is invoked
 * within the ACPD zone, although, invoking a read-only operation within the
 * ACDP zone is unusual.
 * <p>
 * After all it follows that there won't be any problems with conflicts arising
 * from concurrency in a writable database if for each database operation the
 * following holds:
 * 
 * <ul>
 * 	<li>The database operation is a Kamikaze write or</li>
 * 	<li>The database operation is a read or write operation executed within
 *        a unit or</li>
 * 	<li>The database operation is a read-only operation executed within a
 *        read zone or</li>
 * 	<li>The database operation is a read-only operation or a maintenance
 *        operation executed within the ACDP zone.</li>
 * </ul>
 * <p>
 * We call a database operation satisfying one of the four conditions from
 * above a <em>synchronized database operation</em> in contrast to a database
 * operation that satisfies none of these conditions.
 * We call the latter one an <em>unsynchronized database operation</em>.
 * In a writable database read-only operations executed outside a read zone are
 * the only unsynchronized database operations.
 * In a read-only database all database operations are unsynchronized as per
 * definition.
 *
 * @author Beat Hoermann
 */
final class SyncManager {
	/**
	 * The database to which this synchronization manager belongs.
	 */
	private final Database_ db;
	
	/**
	 * The one and only one instance of a {@code Unit_}.
	 * This value is {@code null} if and only if the database is read-only.
	 */
	private final Unit_ unit;
	
	/**
	 * The identifiers of the threads currently running within an open read
	 * zone.
	 * Maps the identifier of a particular thread to the number of open nested
	 * read zones in this thread.
	 * This value is {@code null} if and only if the database is read-only.
	 */
	private final Map<Long, Integer> readZones;
	
	/**
	 * The identifier of the thread currently running within the ACDP zone or
	 * -1 if the ACDP zone is currently closed.
	 */
	private long acdpZoneThreadId;
	
	/**
	 * The number of open nested ACDP zones.
	 */
	private int acdpZoneNested;
	
	/**
	 * Indicates if the synchronization manager is blocked.
	 */
	private boolean blocked;
	
	/**
	 * Indicates if the synchronization manager ceases or has ceased operation.
	 */
	private boolean shutdown;
	
	/**
	 * Constructs the synchronization manager.
	 * The synchronization manager is unblocked.
	 * 
	 * @param  recFilePath The absolute path of the recorder file (file name
	 *         included), may be {@code null} if the database is read-only.
	 * @param  db The database for which this synchronization manager is to be
	 *         constructed.
	 *         
	 * @throws CreationException If the synchronization manager can't be created
	 *         due to the unit which can't be created.
	 */
	SyncManager(Path recFilePath, Database_ db) throws CreationException {
		this.db = db;
		this.unit = db.isWritable() ? new Unit_(recFilePath, db, this) : null;
		readZones = db.isWritable() ? new HashMap<Long,Integer>() : null;
		acdpZoneThreadId = -1;
		acdpZoneNested = 0;
		blocked = false;
		shutdown = false;
	}
	
	/**
	 * Copies the content of the locked recorder file to the specified output
	 * stream.
	 * <p>
	 * The output stream is not closed.
	 * 
	 * @param  os The output stream, not allowed to be {@code null}.
	 * @param  buffer The buffer to be used, not allowed to be {@code null}.
	 * 
	 * @throws NullPointerException If one of the arguments is {@code null}.
	 * @throws FileIOException if an I/O error occurs.
	 */
	final void copyRecFile(OutputStream os, Buffer buffer) throws
													NullPointerException, FileIOException {
		unit.copyRecFile(os, buffer);
	}
	
	/**
	 * If the database is writable and if changes to the database are not forced
	 * being materialized as part of committing a sequence of write operations
	 * then this method forces any changes being materialized.
	 * 
	 * @throws FileIOException if an I/O error occurs.
	 */
	final void forceWrite() throws FileIOException {
		if (db.isWritable() && !db.forceWriteCommit()) {
			unit.forceWrite();
		}
	}
	
	/**
	 * Ensures that the synchronization manager is not shut down.
	 * 
	 * @throws ShutdownException If the synchronization manager is shut down.
	 */
	private final void ensureNotShutdown() throws ShutdownException {
		if (shutdown) {
			throw new ShutdownException(db, "Synchronization manager is shut " +
																							"down.");
		}
		// !shutdown
	}
	
	/**
	 * Blocks the synchronization manager.
	 * If the synchronization manager is blocked this method waits until the
	 * synchronization manager is no longer blocked and immediately blocks it
	 * again.
	 * Each invocation of this method must be compensated by an invocation of
	 * the {@link #unblock} method.
	 * 
	 * @throws NullPointerException If the database is read-only.
	 * @throws ShutdownException If the synchronization manager is shut down.
	 *         This exception never happens if the database is read-only.
	 */
	final synchronized void block() throws NullPointerException,
																				ShutdownException {
		while (blocked && !shutdown) {
			try {
				wait();
			} catch (InterruptedException e) {}
		}
		ensureNotShutdown();
		blocked = true;
		// !shutdown && blocked
	}
	
	/**
	 * Unblocks the synchronization manager, giving any waiting threads a
	 * chance to proceed.
	 */
	final synchronized void unblock() {
		blocked = false;
		notifyAll();
		// !blocked
	}
	
	/**
	 * Initializes and returns the unit.
	 * Subsequent database operations are executed within this unit.
	 * <p>
	 * If the unit is currently issued and it was issued in the current thread
	 * then the client obviously requests a <em>nested</em> unit.
	 * Otherwise the unit is currently not issued or it was issued in a thread
	 * different from the current thread.
	 * If the unit is currently not issued and the synchronization manager is
	 * blocked then this method waits until the synchronization manager is no
	 * longer blocked, issues the unit in the current thread and blocks the
	 * synchronization manager again.
	 * If the unit is currently not issued and the synchronization manager is
	 * unblocked then this method immediately issues the unit in the current
	 * thread and blocks the synchronization manager.
	 * If the unit is currently issued and it was issued in a thread different
	 * from the current thread then the synchronization manager is blocked and
	 * this method waits until the synchronization manager is no longer blocked
	 * (which implies that the unit is closed), issues the unit in the current
	 * thread and blocks the synchronization manager.
	 * <p>
	 * In any case, after this method has finished execution, the synchronization
	 * manager is blocked which has the effect of preventing another unit, an
	 * ACDP zone, a Kamikaze write or a read zone from being concurrently
	 * executed.
	 * The synchronization manager gets unblocked again when the top-level unit
	 * is closed.
	 * (Closing a nested unit leaves the synchronization manager blocked.)
	 * It is therefore vital that each call to this method is compensated by
	 * a call to the {@link Unit_#close} method.
	 * 
	 * @return The unit, never <code>null</code>.
	 * 
	 * @throws UnitBrokenException If the unit is broken.
	 * @throws ShutdownException If the synchronization manager is shut down.
	 * @throws ACDPException If the database is read-only or if this method is
	 *         invoked within a read zone or within the ACDP zone.
	 */
	final synchronized Unit issueUnit() throws UnitBrokenException,
															ShutdownException, ACDPException {
		if (!db.isWritable()) {
			throw new ACDPException(db, "Database is read-only.");
		}
		long threadId = Thread.currentThread().getId();
		if (readZones.containsKey(threadId)) {
			// There exists an open read zone that was opened in the current
			// thread.
			throw new ACDPException(db, "Can't issue unit within a read zone.");
		}
		if (acdpZoneThreadId == threadId) {
			// The ACDP zone is open and it was opened in the current thread.
			throw new ACDPException(db, "Can't issue unit within the ACDP " +
																							"zone.");
		}
		unit.ensureNotBroken();
		if (unit.openedInThread(threadId)) {
			// Unit issued and it was issued in the current thread.
			// Client requests nested unit.
			// Implies blocked.
			unit.nest();
		}
		else {
			// Unit not issued or it was issued in a thread different from the
			// current thread.
			block();
			unit.open(threadId);
		}
		// !db.isBroken() && db.isWritable() && no open read zone &&  unit not
		// issued in a thread different from the current thread && ACDP zone
		// closed.
		return unit;
	}
	
	/**
	 * Returns the unit if it is issued and if it was issued in the current
	 * thread.
	 * This method returns {@code null} if the unit is closed or if it was
	 * issued in a thread different from the current thread.
	 * <p>
	 * By invoking this method read/write operations can find out if they are
	 * invoked within a unit.
	 * 
	 * @return The issued unit.
	 *         This value is {@code null} if the unit is closed or if it was
	 *         issued in a thread different from the current thread.
	 * 
	 * @throws NullPointerException If the database is a read-only database.
	 */
	final synchronized Unit_ getUnit() throws NullPointerException {
		// unit open implies blocked.
		return (unit.openedInThread(Thread.currentThread().getId())) ? unit :null;
	}
	
	/**
	 * The sole purpose of this class is to enable the use of the
	 * try-with-resources statement for opening and auto closing a read zone.
	 * 
	 * @author Beat Hoermann
	 */
	private final class ReadZone_ implements ReadZone {
		@Override
		public final void close() throws ACDPException {
			closeReadZone();
		}
	}
	
	/**
	 * The only instance of the {@code ReadZone_} class.
	 */
	private final ReadZone_ readZone = new ReadZone_();
	
	/**
	 * Opens a read zone.
	 * Subsequent read-only operations are executed in this read zone.
	 * <p>
	 * Note that, apart from returning the one and only one instance of the
	 * {@code ReadZone_} class, this method has no effect if the database is a
	 * read-only database.
	 * In the following we therefore assume that the database is a writable
	 * database.
	 * We distinguish two cases:
	 * 
	 * <ol>
	 *    <li> The unit is currently issued and it was issued in the current
	 *    thread <em>or</em> the ACDP zone is open and it was opened in the
	 *    current thread <em>or</em> at least one read zone is currently open
	 *    (either in the current thread or in a thread different from the
	 *    current thread).
	 *    </li>
	 *    <li> The unit is currently not issued or it is issued and it was
	 *    issued in a thread different from the current thread <em>and</em> the
	 *    ACDP zone is currently closed or it is open and it was opened in a
	 *    thread different from the current thread <em>and</em> there exists no
	 *    open read zone (neither in the current thread nor in a thread
	 *    different from the current thread).</li>
	 * </ol>
	 * <p>
	 * As to the first case, it follows that the synchronization manager is
	 * currently blocked.
	 * The read zone will be opened within the unit <em>or</em> within the ACDP
	 * zone <em>or</em> within an open read zone (resulting in a <em>nested</em>
	 * read zone) <em>or</em> the read zone will be opened stand-alone (that is,
	 * not surrounded by a unit or an ACDP zone or an open read zone) and coexist
	 * with at least another open read zone that was opened in a thread different
	 * from the current thread.
	 * In this regard note that read zones can't contain units or ACDP zones but
	 * read zones only.
	 * <p>
	 * As to the second case, the read zone will be opened stand-alone and there
	 * exists no other open read zone.
	 * This method blocks the synchronization manager if it is unblocked or
	 * waits until the synchronization manager is no longer blocked and
	 * immediately blocks the synchronization manager again.
	 * (The synchronization manager may be blocked either due to an issued unit
	 * or due to an open ACDP zone or due to a Kamikaze write executed in a
	 * thread different from the current thread.)
	 * <p>
	 * In any case, after this method has finished execution the synchronization
	 * manager is blocked which has the effect of preventing a unit, an ACDP zone
	 * or a Kamikaze write from being concurrently executed.
	 * <p>
	 * Each invocation of this method must be compensated by an invocation of
	 * the {@link #closeReadZone} method.
	 * <p>
	 * A Kamikaze write is not allowed to invoke this method.
	 * (This would result in a deadlock.)
	 * 
	 * @return The read zone, never {@code null}.
	 * 
	 * @throws ShutdownException If the synchronization manager is shut down.
	 *         This exception never happens if the database is read-only.
	 */
	final synchronized ReadZone openReadZone() throws ShutdownException {
		if (db.isWritable()) {
			long threadId = Thread.currentThread().getId();
			final boolean opensInUnitOrACDPZone = unit.openedInThread(threadId) ||
																	acdpZoneThreadId == threadId;
			if (opensInUnitOrACDPZone || !readZones.isEmpty()) {
				// Read zone opens within the unit OR the ACDP zone OR there exists
				// at least one open read zone.
				if (!opensInUnitOrACDPZone && !readZoneOpenedInThread(threadId)) {
					// Top level read zone outside unit and outside ACDP zone.
					ensureNotShutdown();
				}
				// !db.isBroken()
			}
			else {
				// The unit is either not issued or it is issued and it was issued
				// in a thread different from the current thread AND The ACDP zone
				// is either closed or it is open and it was opened in a thread
				// different from the current thread AND there exists no open read
				// zone
				block();
				// !shutdown && blocked
			}
			Integer n = readZones.get(threadId);
			readZones.put(threadId, n == null ? 1 : n + 1);
			// Either this method was invoked within a unit or an ACDP zone OR
			// the unit is not issued and the ACDP zone is closed.
		}
		return readZone;
	}
	
	/**
	 * Closes the read zone previously opened by a call to the {@code
	 * openReadZone} method.
	 * Unblocks the synchronization manager provided that there are no other
	 * open read zones.
	 * <p>
	 * Invoking this method has no effect if the database is read-only.
	 * 
	 * @throws ACDPException If there is no open read zone in the current thread.
	 *         This exception never happens if the database is read-only.
	 */
	private final synchronized void closeReadZone() throws ACDPException {
		if (db.isWritable()) {
			long threadId = Thread.currentThread().getId();
			Integer n = readZones.get(threadId);
			if (n == null) {
				throw new ACDPException(db, "No open read zone in the current " +
																							"thread.");
			}
			if (n > 1)
				// Nested read zone.
				readZones.put(threadId, n - 1);
			else {
				// n == 1
				readZones.remove(threadId);
				if (readZones.isEmpty() && !unit.openedInThread(threadId) &&
																acdpZoneThreadId != threadId) {
					// There exists no open read zone and this method is not invoked
					// within a unit or an ACDP zone.
					unblock();
				}
			}
		}
	}

	/**
	 * Tests if there exists an open read zone that was opened in the specified
	 * thread.
	 * <p>
	 * By invoking this method Kamikaze writes can find out if they are
	 * invoked within a read zone, and if so they must throw an exception.
	 * 
	 * @param  threadId The identifier of the thread.
	 * 
	 * @return The boolean value {@code true} if there exists an open read
	 *         zone that was opened in the specified thread.
	 */
	final synchronized boolean readZoneOpenedInThread(long threadId) {
		return readZones.containsKey(threadId);
	}
	
	/**
	 * Opens the ACDP zone.
	 * Subsequently invoked methods are executed within this ACDP zone.
	 * <p>
	 * If the ACDP zone is currently opened and it was opened in the current
	 * thread then the client obviously opens a <em>nested</em> ACDP zone.
	 * Otherwise the ACDP zone is currently closed or it was opened in a thread
	 * different from the current thread.
	 * If the ACDP zone is currently closed and the synchronization manager is
	 * blocked then this method waits until the synchronization manager is no
	 * longer blocked, opens the ACDP zone in the current thread and blocks the
	 * synchronization manager again.
	 * If the ACDP zone is currently closed and the synchronization manager is
	 * unblocked then this method immediately opens the ACDP zone in the
	 * current thread and blocks the synchronization manager.
	 * If the ACDP zone is currently open and it was opened in a thread different
	 * from the current thread then the synchronization manager is blocked and
	 * this method waits until the synchronization manager is no longer blocked
	 * (which implies that the ACDP zone is closed), opens the ACDP zone in the
	 * current thread and blocks the synchronization manager.
	 * <p>
	 * In any case, after this method has finished execution, the synchronization
	 * manager is blocked which has the effect of preventing a unit, another
	 * ACDP zone, a Kamikaze write or a read zone from being concurrently
	 * executed.
	 * The synchronization manager gets unblocked again when the top-level ACDP
	 * zone is closed.
	 * (Closing a nested ACDP zone leaves the synchronization manager blocked.)
	 * It is therefore vital that each call to this method is compensated by
	 * a call to the {@link #closeACDPZone} method.
	 * <p>
	 * A Kamikaze write is not allowed to invoke this method.
	 * (This would result in a deadlock.)
	 * 
	 * @throws ShutdownException If the synchronization manager is shut down.
	 * @throws ACDPException If the database is read-only or if this method is
	 *         invoked within a read zone or a unit.
	 */
	final synchronized void openACDPZone() throws ShutdownException,
																					ACDPException {
		if (!db.isWritable()) {
			throw new ACDPException(db, "Database is read-only.");
		}
		// Database is writable. This guarantees that no other client has
		// currently opened the same database.
		long threadId = Thread.currentThread().getId();
		if (readZones.containsKey(threadId)) {
			// There exists an open read zone that was opened in the current
			// thread.
			throw new ACDPException(db, "Can't open ACDP zone within a read " +
																							"zone.");
		}
		if (unit.openedInThread(threadId)) {
			// The unit is issued and it was issued in the current thread.
			throw new ACDPException(db, "Can't open ACDP zone within a unit.");
		}
		// The unit is either not issued or it is issued and it was issued in a
		// thread different from the current thread. Furthermore, there exists
		// either no open read zone or there exist one or more open read zones
		// but all were opened in threads different from the current thread.
		if (acdpZoneThreadId != threadId) {
			// The ACDP zone is either closed or it is open and it was opened in
			// a thread different from the current thread.
			block();
			acdpZoneThreadId = threadId;
		}
		// else: The ACDP zone is open and it was opened in the current thread.
		//       Client requests nested ACDP zone. Implies blocked.
		acdpZoneNested++;
		// db.isWritable() && no open read zone && unit not issued && ACDP zone
		// not open in a thread different from the current thread.
	}
	
	/**
	 * Closes the ACDP zone previously opened by a call to the {@code
	 * openACDPZone} method.
	 * Unblocks the synchronization manager provided that the ACDP zone is a
	 * top-level ACDP zone.
	 * 
	 * @throws ACDPException If the ACDP zone was never opened or it was opened
	 *         in a thread different from the current thread or if the ACDP zone
	 *         contains an open read zone.
	 */
	final synchronized void closeACDPZone() throws ACDPException {
		long threadId = Thread.currentThread().getId();
		if (acdpZoneThreadId != threadId) {
			throw new ACDPException(db, "The ACDP zone was never opened or it " +
						"was opened in a thread different from the current thread.");
		}
		if (readZoneOpenedInThread(threadId)) {
			throw new ACDPException(db, "The ACDP zone contains an open " +
																					"read zone.");
		}
		acdpZoneNested--;
		if (acdpZoneNested == 0) {
			acdpZoneThreadId = -1;
			unblock();
		}
	}
	
	/**
	 * Tests if the ACDP zone is open and if it was opened in the specified
	 * thread.
	 * <p>
	 * By invoking this method Kamikaze writes can find out if they are
	 * invoked within an ACDP zone, and if so they must throw an exception.
	 * 
	 * @param  threadId The identifier of the thread.
	 * 
	 * @return The boolean value {@code true} if the ACDP zone is open and if
	 *         it was opened in the specified thread.
	 */
	final synchronized boolean acdpZoneOpenedInThread(long threadId) {
		return acdpZoneThreadId == threadId;
	}
	
	/**
	 * Ceases operation of the synchronization manager.
	 * <p>
	 * If the synchronization manager is blocked then this method informs all
	 * threads waiting for the synchronization manager being unblocked that the
	 * synchronization manager shuts down.
	 * Upon receiving this information these threads immediately stop waiting.
	 * Then this method waits until the synchronization manager is no longer
	 * blocked.
	 * Once the synchronization manager is unblocked this method releases any
	 * system resources associated with the unit.
	 * <p>
	 * Note that all methods waiting for the synchronization manager being
	 * unblocked, that are the {@code block}, {@code issueUnit}, {@code
	 * openReadZone} and {@code openACDPZone} methods, throw an exception if
	 * this method is invoked.
	 * Therefore, strictly speaking, it is an error to invoke this method if
	 * any thread is waiting for the synchronization manager being unblocked.
	 * <p>
	 * This method waits for the thread that has blocked the synchronization
	 * manager until that thread unblocks it again thus giving the currently
	 * running <em>synchronized database operations</em>, hence, the read/write
	 * operations currently running within a unit or within a read zone or a
	 * currently running Kamikaze write or a service operation currently running
	 * within an ACDP zone a chance to terminate gracefully, in particular,
	 * without corrupting the database.
	 * (See the class description to learn more about synchronized and
	 * unsynchronized database operations.)
	 * <p>
	 * To conclude, after this method has finished execution, all units, read
	 * zones, Kamikaze writes and ACDP zones waiting for being executed have
	 * "received" an exception and the unit or read zone(s) or Kamikaze write or
	 * ACDP zone executed at the time this method was invoked has now finished
	 * execution.
	 * Therefore, the <em>unsynchronized read-only operations</em> are the only
	 * database operations which remain uninformed about the shutdown of the
	 * synchronization manager.
	 * They still may be running after this method has finished execution.
	 * <p>
	 * Of course, any unit, read zone, Kamikaze write and ACDP zone immediately
	 * receives an exception if it is invoked after this method has finished.
	 * <p>
	 * If the database is read-only or if the synchronization manager is already
	 * shut down then this method has no effect.
	 *
	 * @throws FileIOException If an I/O error occurs.
	 */
	final synchronized void shutdown() throws FileIOException {
		if (db.isWritable() && !shutdown) {
			shutdown = true;
			if (blocked) {
				// Inform any threads waiting for the synchronization manager being
				// unblocked that the synchronization manager shuts down. These
				// threads immediately stop waiting.
				notifyAll();
				// Wait until the synchronization manager gets unblocked. No other
				// thread waits for the synchronization manager being unblocked.
				while (blocked) {
					try {
						wait();
					} catch (InterruptedException e) {}
				}
			}
			// unit is closed.
			unit.shutdown();
			// !blocked && shutdown
		}
	}
}