/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp;

import acdp.exceptions.ACDPException;
import acdp.exceptions.UnitBrokenException;

/**
 * Confines an atomically executed sequence of read/write database operations
 * that has to be committed or otherwise undone.
 * <p>
 * Users are strongly encouraged to apply the following pattern:
 * 
 * <pre>
 * // db of type Database or CustomDatabase, not null.
 * // Precondition: db.info().isWritable()
 * try (Unit u = db.getUnit()) {
 *    ...
 *    u.commit();
 * }</pre>
 * 
 * where the ellipsis ('&hellip;') stands for any program code, including code
 * that contains calls to {@code u.commit()} and code that repeats this pattern
 * to create one or more <em>nested</em> units.
 * <p>
 * The effects of so called <em>unconfirmed</em> write operations (see below)
 * onto the data persisted in the database are reverted at the time a unit is
 * closed.
 * Those unconfirmed write operations are said to be <em>rolled back</em>.
 * <p>
 * To explain what an "unconfirmed write operation" is, we need to introduce
 * some additional concepts.
 * <p>
 * A write operation is said to be a <em>member</em> of a unit {@code u}, or
 * short "of {@code u}", if it is executed within {@code u} but not within a
 * nested unit in {@code u}.
 * A write operation {@code w} of {@code u} is <em>committed</em> in {@code u}
 * as soon as {@code u.commit()} is successfully executed for the first time
 * since the termination of {@code w}.
 * As long as {@code w} is not committed it is said to be <em>uncommitted</em>.
 * Likewise, a nested unit in {@code u} is <em>committed</em> in {@code u} as
 * soon as {@code u.commit()} is successfully executed for the first time since
 * that nested unit was closed.
 * As long as the nested unit is not committed it is said to be
 * <em>uncommitted</em>.
 * Note that the nested units in {@code u} include <em>deeply nested units</em>,
 * for example, a unit {@code u''} that is in a unit {@code u'} thas is in
 * {@code u}.
 * <p>
 * Now, an <em>unconfirmed write operation</em> is either an uncommitted write
 * operation of {@code u} <em>or</em> a committed write operation of an
 * uncommitted nested unit in {@code u}.
 * <p>
 * In the following examples {@code w} denotes a write operation and {@code c}
 * denotes the successful execution of the {@link #commit} method.
 * Unconfirmed write operations are marked with an asterisk ('{@code *}').
 * Nested units in {@code u} are indented and labeled with {@code u'} and {@code
 * u''}.
 * 
 * <pre>
 * Example 1          Example 2           Example 3
 * 
 * u  u' u''          u  u' u''           u  u' u''
 * 
 * w1                 w1*                 w1
 * w2                    w1.1*               w1.1
 *    w1.1                  w1.1.1              w1.1.1
 *    c                  c                      c
 * w3                                           w1.1.2
 * c                                      c
 *    w2.1*
 *       w1.1.1*
 *       c
 *    w2.2*
 *    c
 * w4*</pre>
 * 
 * In the first example the write operations {@code w2.1}, {@code w1.1.1},
 * {@code w2.2} and {@code w4} are unconfirmed because the first three ones are
 * committed write operations of nested units that are uncommitted in {@code u}
 * and the last is an uncommitted write operation of {@code u}.
 * In the second example the write operations {@code w1} and {@code w1.1} are
 * unconfirmed.
 * (The write operation {@code w1.1.1} was rolled back at the time {@code u''}
 * was closed because it was an uncommitted write operation of {@code u''}.)
 * In the third example there is no unconfirmed write operation:
 * The write operations {@code w1.1}, {@code w1.1.1} and {@code w1.1.2} were
 * rolled back before and {@code w1} is a committed write operation of {@code
 * u}.
 * Note that the write operation {@code w1.1.1} was rolled back at the time
 * {@code u'} was closed because {@code u''} remained uncommitted in {@code u'}.
 * Write operation {@code w1.1.1} was not rolled back at the time {@code u''}
 * was closed because {@code w1.1.1} was committed in {@code u''}.
 * However, write operation {@code w1.1.2} was rolled back at the time {@code
 * u''} was closed because it remained uncommitted in {@code u''}.
 * <p>
 * Units only make sense if the database is {@linkplain
 * acdp.Information.DatabaseInfo#isWritable() writable}.
 * 
 * <h1>Broken Unit</h1>
 * A unit <em>breaks</em> if one of the methods specified in this interface
 * breaks the unit or if a write operation invoked within the unit fails
 * recording its before data.
 * <p>
 * A unit that is about to break throws a {@code UnitBrokenException}.
 * In case of a disaster, for instance a power failure or a crash of the
 * storage device, a unit may not have a chance to throw a {@code
 * UnitBrokenException}.
 * <p>
 * If a unit breaks, ACDP tries to rollback any unconfirmed write operations.
 * However, even if the unconfirmed write operations can successfully be rolled
 * back, the unit remains broken and units can no longer be executed.
 * <p>
 * <em>The only way to recover from a broken unit is to close the database
 * and open the database again.</em>
 * <p>
 * Do the following if a unit is broken:
 * 
 * <ol>
 *    <li>Close the database.</li>
 *    <li>Check the integrity of the persisted data by calling the {@link
 *    acdp.misc.ACDP#verify} method.</li>
 *    <li>Find out if recent changes made to the database have been
 *    materialized, in particular, check if {@code forceWriteCommit} (see
 *    section "Durability" in the description of the {@link Database}
 *    interface) was enabled and analyze the <em>recorder file</em>.
 *    (The recorder file is the place where ACDP saves any before data of
 *    unconfirmed write operations.
 *    The path to the recorder file is saved in the {@code recFile} entry of
 *    the database layout.)
 *    <li>Repair the database if necessary.</li>
 *    <li>Reopen the database.</li>
 * </ol>
 * <p>
 * Reopening the database may rollback any unconfirmed write operations and thus
 * healing a broken unit, however, rolling back fails if the recorder file is
 * corrupted.
 * If this is the case then repair the recorder file and try to open the
 * database again.
 * <p>
 * Note that ACDP never "automatically" rolls back a unit with the exception of
 * the situation described above.
 * (The database has to be writable.
 * If the database is a write protected WR database then ACDP still detects an
 * earlier produced broken unit or a corrupted recorder file but ACDP does not
 * execute a rollback.)
 * <p>
 * ACDP makes an effort to avoid rollbacks based on a recorder file that is, in
 * reality, corrupted but which ACDP naively could believe being non-corrupted.
 * <p>
 * There should be no need for clients to implement this interface.
 *
 * @author Beat Hoermann
 */
public interface Unit extends AutoCloseable {
	
	/**
	 * Commits any yet uncommitted write operations of this unit as well as any
	 * yet uncommitted nested units in this unit.
	 * 
	 * @throws ACDPException If this unit was originally opened in a thread
	 *         different from the current thread.
	 *         This exception never happens if you consequently apply the usage
	 *         pattern described in the class description.
	 * @throws UnitBrokenException If committing fails, including the case that
	 *         this unit was already broken before this method was able to start
	 *         doing its job.
	 */
	void commit() throws ACDPException, UnitBrokenException;
	
	/**
	 * Rolls back any uncommitted write operations of this unit as well as any
	 * write operations that were executed and committed in any uncommitted
	 * nested units and closes this unit.
	 * 
	 * @throws ACDPException If this unit was originally opened in a thread
	 *         different from the current thread or if this unit contains an
	 *         open read zone.
	 *         This exception never happens if you consequently apply the usage
	 *         pattern described in the description of this interface and if you
	 *         consequently close inside a unit any read zone that was opened
	 *         within that unit.
	 * @throws UnitBrokenException If this unit was already broken before this
	 *         method was invoked or if rolling back any uncommitted write
	 *         operations failed.
	 */
	@Override
	void close() throws ACDPException, UnitBrokenException;
}
