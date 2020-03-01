/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp;

import acdp.exceptions.ACDPException;

/**
 * Confines a sequence of read-only operations protected against the effects of
 * concurrent write operations.
 * <p>
 * Read-only operations can not harm the integrity of the data persisted in the
 * database, but in the presence of concurrent writes they still may suffer
 * from inconsistent views and therefore may result in <em>dirty reads</em>.
 * To completely prevent inconsistent views, even across classic transaction
 * boundaries (!), invoke read-only database operations within a <em>read
 * zone</em>.
 * (Of course, a read zone must be "wide enough" to cope with all kinds of view
 * inconsistencies.
 * Explaining all kinds of view inconsistencies is beyond the scope of this
 * description.)
 * <p>
 * Users are encouraged to use the try-with-resources statement to open and
 * auto close a read zone:
 * 
 * <pre>
 * // db of type Database or CustomDatabase, not null.
 * try (ReadZone rz = db.openReadZone()) {
 *    ...
 * }</pre>
 * 
 * where the ellipsis stands for any program code, including code that repeats
 * this pattern to create one or more <em>nested</em> read zones.
 * <p>
 * As long as at least one read zone is open (several read zones may be
 * open in several threads at the same time) the database is blocked for any
 * write database operation.
 * Write database operations requesting the unit or threads wanting to execute
 * a Kamikaze write are delayed until all read zones are closed.
 * <p>
 * Write database operations throw an exception if they are invoked within a
 * read zone.
 * Likewise, requesting the unit inside a read zone throws an exception.
 * However, opening a read zone inside a unit is allowed.
 *
 * @author Beat Hoermann
 */
public interface ReadZone extends AutoCloseable {
	/**
	 * Closes this read zone.
	 * <p>
	 * Invoking this method has no effect if the database is read-only.
	 * 
	 * @throws ACDPException If there is no open read zone in the current thread.
	 *         This exception never happens if the database is read-only.
	 */
	@Override
	void close() throws ACDPException;
}
