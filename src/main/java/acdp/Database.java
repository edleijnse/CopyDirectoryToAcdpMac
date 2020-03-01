/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp;

import java.lang.AutoCloseable;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

import acdp.Information.DatabaseInfo;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CreationException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.ShutdownException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.Database_;

/**
 * Defines the database and provides the {@link #open} method.
 * <p>
 * A database comprises a collection of {@linkplain Table tables}.
 * The configuration of a database is contained in the <em>database
 * {@linkplain acdp.misc.Layout layout}</em>.
 * <p>
 * An instance of this class can be obtained by opening the database as a
 * weakly typed database (see below) and applying the following usage pattern:
 * 
 * <pre>
 * try (Database db = Database.open(...) {
 *    Table myTable = db.getTable("MyTable");
 *    <em>do something with</em> myTable
 * }</pre>
 * 
 * <h1>Weakly and Strongly Typed</h1>
 * A database can be {@linkplain #open opened as a weakly typed} or {@linkplain
 * acdp.design.CustomDatabase strongly typed database}.
 * An instance of this class is called a <em>weakly typed database</em> because
 * the first access to a specific table of the database can only be done by the
 * name of the table or by its position within the list of table layouts
 * contained in the database layout, see the {@link #getTable(String)} and
 * {@link #getTables()} methods.
 * Changing the name of the table or changing the position of the table in the
 * list of table layouts, without adapting the code on the client side, breaks
 * the client code.
 * Even worse, the compiler has no chance to detect such inconsistencies.
 * Another drawback is that the tables of a weakly typed database are
 * themselves weakly typed.
 * (See section "Weakly and Strongly Typed" in the {@code Table} interface
 * description to learn about weakly and strongly typed tables.)
 * <p>
 * In contrast to this, a <em>strongly typed database</em> is an instance of a
 * class extending the {@link acdp.design.CustomDatabase} class.
 * An elaborated custom database class can provide tailor-made methods for
 * dealing with all kinds of aspects of cross-entity-specific persistence
 * management.
 * Opening a database as a strongly typed database creates its tables as
 * strongly typed tables.
 * Doing something with a particular table of a strongly typed database is
 * simply doing it with the appropriate table instance which is an
 * instance of a class extending the {@link acdp.design.CustomTable} class.
 * <p>
 * Opening a database as a weakly typed database can be done from the
 * information saved in the database layout only.
 * Therefore, ACDP does not need to know of a custom database class or of any
 * custom table classes in order to open a database as a weakly typed database.
 * Typically, a database is opened as a weakly typed database if there is no
 * need for making any reference to the concrete entities the tables represent,
 * for example, consider a tool which displays the persisted data of an
 * arbitrary database without making any reference to a concrete entity.
 * 
 * <h1>WR and RO Database</h1>
 * A <em>WR database</em> (<em>RO database</em>) is a database where all of its
 * tables are associated with a <em>WR store</em> (<em>RO store</em>),
 * respectively.
 * See section "Store" in the description of the {@code Table} interface to
 * learn more about these two types of store.
 * In a WR database that is not write protected, the database's tables support
 * insert, delete and update operations.
 * In an RO database or a write protected WR database such operations are not
 * supported.
 * Furthermore, a WR database consists of several files, including a separate
 * file containing the layout of the database, whereas an RO database is backed
 * by just one database file.
 * Invoking the {@link #convert} method converts a WR database to an RO
 * database.
 * 
 * <h1>Units</h1>
 * An ACDP unit can be compared to the well-known database transaction.
 * The main differences are:
 * 
 * <ul>
 * 	<li>Transactions running in different threads may interleave.
 *        Units, on the other hand, act as a monitor: Units in different threads
 *        are executed serially.</li>
 * 	<li>Nested units work as expected: If a sequence of write operations needs
 *        to be rolled back and this sequence of write operations contains
 *        other sequences of write operations in nested units then these
 *        sequences of committed write operations are rolled back as well.
 *        On the other hand, no write operation in any outer unit is affected
 *        if a sequence of write operations of a nested unit needs to be rolled
 *        back.</li>
 *    <li>Reasonably, a unit contains at least one write operation.
 *        Units generally don't make sense if the database is read-only.
 *        To cope with <em>view inconsistencies</em> ACDP provides a different
 *        concept, the so called <em>read zones</em> (see below) which have an
 *        effect even across transaction boundaries.
 *        This is important because isolation ("ACID - Atomicity, Consistency,
 *        Isolation, Durability") can be an issue beyond the scope of a 
 *        transaction.</li>
 *    <li>In a multi-threaded environment, transactions may be automatically
 *        rolled back at any time "by the system" due to deadlock resolution,
 *        leaving the client in the delicate situation to restart the
 *        transaction (tricky in praxis!) or abort the process.
 *        In contrast, a client of an ACDP database never has to worry
 *        about restarting a unit due to an intervention of the system.</li>
 * </ul>
 * <p>
 * A write operation, that is, an {@linkplain Table#insert insert},
 * {@linkplain Table#delete delete}, {@linkplain Table#update update},
 * {@linkplain Table#updateAll updateAll}, {@linkplain
 * Table#updateAllSupplyValues updateAllSupplyValues} or {@linkplain
 * Table#updateAllChangeValues updateAllChangeValues} operation, executed
 * outside a unit is called a <em>Kamikaze write</em>.
 * Kamikaze writes outperform write operations executed within a unit but since
 * they cannot be rolled back, a Kamikaze write that fails poses a threat to the
 * integrity of the persisted data, though ACDP takes care that Kamikaze writes
 * are safe for use in a multi-threaded environment.
 * A typical use case of Kamikaze writes is the initial filling of an empty
 * table with a large amount of data.
 * Read the description of the {@link Unit} interface to learn more about units
 * and the common usage pattern.
 * 
 * <h1>Read Zones</h1>
 * A read-only operation, that is, a {@linkplain Table#get get}, an {@linkplain
 * Table#iterator iterate}, a {@linkplain Table#numberOfRows numberOfRows} or
 * a terminal operation of a {@linkplain Table#rows stream pipeline} can be
 * invoked within a unit or a read zone to ensure that no concurrent writes are
 * taken place in the database while the operation is being executed.
 * Several read-only operations can be invoked within a read zone to cope with
 * all kinds of view inconsistencies.
 * Read the description of the {@link ReadZone} interface to learn more about
 * read zones and the common usage pattern.
 * 
 * <h1>Service Operations</h1>
 * Besides those <em>regular</em> write and read database operations mentioned
 * in the sections "Units" and "Read Zones" above, ACDP provides several
 * <em>service</em> operations, most of them available only for a WR database.
 * 
 * <dl>
 *    <dt>Level 0</dt>
 *    <dd>These are read-only service operations providing meta information
 *    about a particular table or the database.
 *    They can be invoked within a read zone or a unit in order to prevent any
 *    writes while the service operation is running.
 *    <p>
 *    Complete listing: all methods declared in the inner interfaces of the
 *    {@link Information} interface.
 *    Instances of classes implementing the {@link acdp.Information.DatabaseInfo
 *    DatabaseInfo} and {@link acdp.Information.TableInfo TableInfo} interfaces
 *    can be obtained by invoking the {@link Database#info} and {@link
 *    Table#info} methods, respectively.</dd>
 *    <dt>Level 1</dt>
 *    <dd>These are service operations that, as those on level 0, do not change
 *    the database but generate and save persistent data outside the database.
 *    Since they run within a read zone by default, invoking them inside a read
 *    zone or a unit has no effect.
 *    <p>
 *    Complete listing: {@link Database#convert}, {@link Database#zip},
 *    {@link Table#zip}.</dd>
 *    <dt>Level 2</dt>
 *    <dd>These are service operations that remove data from the backing files
 *    of a particular table.
 *    <p>
 *    They cannot be invoked inside a read zone or a unit and their effects
 *    onto the data persisted in the target table and in any other involved
 *    tables cannot be reverted.
 *    The integrity of the target table and of any involved tables may be harmed
 *    if a system crash occurs while a service operation on this level is
 *    running.
 *    <p>
 *    Complete listing: {@link Table#truncate}, {@link Table#compactVL},
 *    {@link Table#compactFL}.</dd>
 *    <dt>Level 3</dt>
 *    <dd>Operations changing the <em>structure</em> of the database or which
 *    involve the modification of the internal format of some tables' backing
 *    files are level 3 service operations.
 *    <p>
 *    Unlike the other service operations, service operations on this level
 *    cannot be invoked within a session: The database must be closed at the
 *    time a level 3 service operation is invoked.
 *    <p>
 *    As with a service operation on level 2, the effects onto the data
 *    persisted in the database when running a service operation on level 3
 *    cannot be reverted.
 *    The integrity of the database may be harmed if a running service operation
 *    on this level throws an exception due to any reason or if a system crash
 *    occurs.
 *    It may therefore be a good idea to {@linkplain acdp.Database#zip
 *    backup the database} before executing a service operation on this level.
 *    <p>
 *    Complete listing: All methods declared in the {@link acdp.tools.Refactor}
 *    class.
 *    </dd>
 * </dl>
 * 
 * All but the level 0 service operations are <em>synchronized</em>.
 * This means that service operations beyond level 0 cannot interfere with any
 * other database operation running in parallel.
 * 
 * <h1>Closing</h1>
 * Never forget to {@linkplain #close close} the database when you are done
 * with it.
 * Since this interface extends the {@link AutoCloseable} interface you can
 * apply the <em>try-with-resources</em> statement, see the code snippet above.
 * 
 * <h1>Zipping</h1>
 * The database can be packed into a single zip-archive-file by invoking the
 * {@link #zip} method.
 * Conversly, unzipping the generated zip-archive-file restores the database.
 * 
 * <h1>Durability</h1>
 * Mass storage devices, especially non-removal ones, usually apply caching
 * strategies that defer writing data to the storage medium to an unknown later
 * time.
 * In case of a system crash this may lead to a loss of data.
 * On the other hand, committed changes are expected to be durable even in the
 * case of a system crash.
 * A common way to cope with this problem is to keep a transaction log:
 * After a system crash has occurred any committed transactions can be redone
 * with the help of the transaction log.
 * To work properly, the transaction log must be saved on stable storage.
 * However, most computer disk drives are not considered stable storage.
 * Even if ACDP logs any before data for rolling back uncommitted write
 * operations of a unit, it does not maintain a transaction log.
 * Instead, ACDP provides a switch in the database layout labeled {@code
 * forceWriteCommit}.
 * If set equal to "{@code on}" ACDP forces any data changes to be {@linkplain
 * java.nio.channels.FileChannel#force materialized} at the time a sequence of
 * write operations is committed.
 * While this method guarantees durability in the case of a system crash even
 * in an environment that lacks stable storage, it deteriorates performance
 * because it interferes with disk controlling policies.
 * This is why it can be turned off.
 * If this feature is turned off then changes are forced being materialized not
 * until the database is {@linkplain #close closed}.
 * However, you can always force changes to be materialized by invoking the
 * {@link #forceWrite} method.
 * Recovering from a system crash that occurred while the database is open can
 * be very hard if {@code forceWriteCommit} is turned off, unless there exists
 * a recent {@linkplain #zip backup of the database} so that all or some of the
 * lost changes can be recovered with little work.
 * Note that Kamikaze writes (see section "Units" above) behave insensitive to
 * the position of this switch or to an invocation of the {@code forceWrite()}
 * method: Changes made with Kamikaze writes are never forced being
 * materialized, not even when the database is closed.
 * (At the time of writing this description, non-volatile RAM-based mass storage
 * devices are being discussed, which presumably let the user find out if data
 * sent to such a device has been successfully persisted.)
 * <p>
 * There should be no need for clients to implement this interface.
 *
 * @author Beat Hoermann
 */
public interface Database extends AutoCloseable {
	/**
	 * Opens the database as a <em>weakly typed</em> database.
	 * <p>
	 * It is a good idea to treat the returned database instance as a shared
	 * singleton.
	 * <p>
	 * Opening the same database (having the same main database file) more than
	 * once is possible if the database is an RO database.
	 * (Read section "WR and RO Database" of this interface description to learn
	 * more about the two kinds of databases.)
	 * However, there are limitations if the database is a WR database:
	 * Opening a WR database more than once within the same process is not
	 * possible while opening a WR database more than once each time in a
	 * differenct process is possible if the WR database is opened with write
	 * protection turned on (which is the case if the {@code writeProtect}
	 * argument is set equal to {@code true}) <em>and</em> provided that the
	 * operating system supports {@linkplain java.nio.channels.FileLock shared
	 * locks}.
	 * (If it were allowed to open the same WR database in one process with
	 * write protection deactivated and in another process with write protection
	 * activated, this would compromise the concept of the <em>read zone</em>.)
	 * 
	 * @param  mainFile The main database file, not allowed to be {@code null}.
	 *         If the database is a WR database then the main file is identical
	 *         to the layout file.
	 *         Otherwise, the database is an RO database and the main file is
	 *         identical to the one and only one RO database file.
	 * @param  opMode The operating mode of the database.
	 *         If the value is equal to zero then the backing table files are
	 *         immediately closed as soon as they become idle.
	 *         If the value is positive and equal to {@code n} then the backing
	 *         table files are closed {@code max(10, n)} milliseconds after they
	 *         become idle or when the database is closed.
	 *         If the value is equal to -1 then the backing table files once
	 *         opened remain open until the database is closed.
	 *         If the value is equal to -2 then the <em>compressed</em> content
	 *         of an RO database is completely mapped into memory.
	 *         If the value is equal to -3 then the <em>uncompressed</em>
	 *         content of an RO database is completely mapped into memory.
	 *         A value equal to zero is recommended only if the database is a
	 *         WR database and the operating system can't sustain a bunch of
	 *         files all being opened at the same time.
	 *         In a server environment, a positive value is recommended or,
	 *         provided that the database is an RO database and there is enough
	 *         memory available, a value equal to -2 (less memory required, less
	 *         fast) or -3 (more memory required, faster).
	 *         Note that a value equal to -2 or equal to -3 raises an {@code
	 *         IllegalArgumentException} if the database is a WR database.
	 * @param  writeProtect Protects a WR database from being modified.
	 *         If set to {@code true} then no data of the database can be
	 *         inserted, deleted or updated.
	 *         Note that this parameter has no effect if the database is an RO
	 *         database.
	 *         A write protected database needs less system resources than a
	 *         writable database.
	 *         
	 * @return The database, never {@code null}.
	 * 
	 * @throws NullPointerException If {@code mainFile} is {@code null}.
	 * @throws IllegalArgumentException If {@code opMode} is less than -3 or if
	 *         {@code opMode} is equal to -2 or -3 and the database is a WR
	 *         database.
	 * @throws InvalidPathException If the database layout contains an invalid
	 *         file path.
	 *         This exception never happens if the database is an RO database.
	 * @throws ImplementationRestrictionException If a table has too many
	 *         columns needing a separate null information.
	 *         This exception never happens if the database is an RO database.
	 * @throws OverlappingFileLockException If the database was already opened
	 *         before in this process (Java virtual machine) or in another
	 *         process that holds a lock which cannot co-exist with the lock to
	 *         be acquired.
	 *         This exception never happens if the database is an RO database.
	 * @throws CreationException If the database can't be created due to any
	 *         other reason including problems with the database layout, problems
	 *         with any of the tables' sublayouts and problems with the backing
	 *         files of the database.
	 */
	static Database open(Path mainFile, int opMode, boolean writeProtect) throws
							NullPointerException, IllegalArgumentException,
							InvalidPathException, ImplementationRestrictionException,
										OverlappingFileLockException, CreationException {
		return new Database_(mainFile, opMode, writeProtect, -1, null);
	}
	
	/**
	 * Returns the name of this database. 
	 * 
	 * @return The name of this database, never {@code null} and never an empty
	 *         string.
	 */
	String name();
	
	/**
	 * Returns the information object of this database.
	 * 
	 * @return The information object of this database, never {@code null}.
	 */
	DatabaseInfo info();
	
	/**
	 * Tests if this database has a table with the specified name.
	 * 
	 * @param  name The name.
	 * 
	 * @return The boolean value {@code true} if and only if this database has a
	 *         table with the specified name.
	 */
	boolean hasTable(String name);

	/**
	 * Returns the table with the specified name.
	 * 
	 * @param  tableName The name of the table as written in the database layout.
	 * 
	 * @return The table with the specified name, never {@code null}.
	 * 
	 * @throws IllegalArgumentException If this database has no table with the
	 *         specified name.
	 */
	Table getTable(String tableName) throws IllegalArgumentException;
	
	/**
	 * Returns all tables of this database.
	 * <p>
	 * The order in which the tables appear in the returned array is equal to
	 * the order in which the tables appear in the database layout.
	 * 
	 * @return The tables of this database, never {@code null} and never empty.
	 */
	Table[] getTables();
	
	/**
	 * Opens and returns the unit.
	 * Invoking this method inside a unit opens a <em>nested</em> unit.
	 * <p>
	 * Use this method as follows:
	 * 
	 * <pre>
	 * // db of type Database or CustomDatabase, not null.
	 * // Precondition: db.info().isWritable()
	 * try (Unit u = db.getUnit()) {
	 *    ...
	 *    u.commit();
	 * }</pre>
	 * 
	 * Note that calling this method in a read-only database throws an exception.
	 * <p>
	 * For more details consult section "Units" of this interface description.
	 * 
	 * @return The unit, never <code>null</code>.
	 * 
	 * @throws UnitBrokenException If the unit is broken.
	 * @throws ShutdownException If this database is closed.
	 * @throws ACDPException If this database is read-only or if this method is
	 *         invoked within a read zone.
	 */
	Unit getUnit() throws UnitBrokenException, ShutdownException, ACDPException;
	
	/**
	 * Opens a read zone.
	 * Invoking this method inside a read zone opens a <em>nested</em> read zone.
	 * <p>
	 * Use this method as follows:
	 * 
	 * <pre>
	 * // db of type Database or CustomDatabase, not null.
	 * try (ReadZone rz = db.openReadZone()) {
	 *    ...
	 * }</pre>
	 * 
	 * Apart from returning an instance of {@code ReadZone}, this method has no
	 * effect if this database is read-only.
	 * <p>
	 * For more details consult section "Read Zones" of this interface
	 * description.
	 * 
	 * @return The read zone, never {@code null}.
	 * 
	 * @throws ShutdownException If this database is closed.
	 *         This exception never happens if this database is read-only.
	 */
	ReadZone openReadZone() throws ShutdownException;
	
	/**
	 * Converts this database, which must be a WR database, to an RO database and
	 * saves it to the specified file.
	 * <p>
	 * Note that existing references to rows of a particular table are valid only
	 * in the converted RO database if the FL file space of the table in the
	 * original WR database has no {@linkplain Table#compactFL gaps}.
	 * <p>
	 * If you open the WR database just for the purpose of converting it then it
	 * is a good idea to {@linkplain #open open the database as a weakly typed},
	 * write protected database and setting the operating mode to -1.
	 * <p>
	 * The implementation of this method is such that concurrent writes can not
	 * take place while this method is running.
	 * It is therefore safe to invoke this method anytime during a session.
	 * 
	 * @param  roDbFilePath The path of the RO database file.
	 *         The value must be the path of a non-existing file.
	 * 
	 * @throws UnsupportedOperationException If this database is an RO database.
	 * @throws NullPointerException If {@code roDbFilePath} is {@code null}.
	   @throws ImplementationRestrictionException If at least one of the tables
	 *         of the WR database has more than {@code Integer.MAX_VALUE} rows
	 *         <em>or</em> if the length of the byte representation of a stored
	 *         column value of a table of the WR database exceeds {@code
	 *         Integer.MAX_VALUE} <em>or</em> if at least one of the tables of
	 *         the WR database has too many columns <em>or</em> if the number of
	 *         row gaps in at least one of the tables of the WR database is
	 *         greater than {@code Integer.MAX_VALUE}.
	 * @throws ShutdownException If this database is closed.
	 * @throws CryptoException If decrypting the byte representation of a stored
	 *         column value of a table of the WR database fails.
	 *         This exception never happens if the WR database does not apply
	 *         encryption.
	 * @throws IOFailureException If the specified file already exists or if
	 *         another I/O error occurs.
	 */
	void convert(Path roDbFilePath) throws UnsupportedOperationException,
							NullPointerException, ImplementationRestrictionException,
							ShutdownException, CryptoException, IOFailureException;
	/**
	 * Creates a zip archive with the specified path and adds all files related
	 * to this database to that zip archive.
	 * <p>
	 * In case of an RO database the {@linkplain Database#open main file} is the
	 * only file related to the database.
	 * Since the main file of an RO database is already quite compact, zipping
	 * an RO database generally does not make sense.
	 * <p>
	 * If the {@code home} argument is set to {@code false} then the files
	 * appear in the resulting zip archive with the path information as
	 * contained in the database layout with the exception of the main file
	 * itself which appears with its file name only.
	 * (Recall that in a WR database the main file and the layout file, hence,
	 * the file containing the database layout, are identical.)
	 * <p>
	 * If the {@code home} argument is set to {@code true} then the same file
	 * structure is created as if the {@code home} argument was set to {@code
	 * false} but with a single root directory having a name equal to the name
	 * of the main directory.
	 * (The main directory is the directory housing the main file.)
	 * <p>
	 * For example, if the main file "v" has a path equal to "/a/<b>b</b>/v" (or
	 * "C:\a\<b>b</b>\v") and some other file "w" of the database has a path
	 * equal to "/a/<b>b</b>/e/w" (or "C:\a\<b>b</b>\e\w") with <b>b</b> denoting
	 * the main directory of the database then "v" and "w" will appear in the
	 * zip archive as "v" and "e/w", respectively, provided that the {@code home}
	 * argument is equal to {@code false}.
	 * If the {@code home} argument is equal to {@code true} then "v" and "w"
	 * will appear in the zip archive as "<b>b</b>/v" and "<b>b</b>/e/w",
	 * respectively.
	 * <p>
	 * The database can easily be restored by unzipping the resulting zip
	 * archive to any directory.
	 * However, the database can be opened without any prior modifications only
	 * if the following two conditions are satisfied:
	 * 
	 * <ul>
	 * 	<li>All file paths contained in the database layout are paths relative
	 *        to the main directory.</li>
	 * 	<li>No path points to a file residing outside the main directory, that
	 *        is, no file is located outside the main directory.
	 *        (Via the ".." path element a path relative to the main directory
	 *        can point to a file outside of the main directory.
	 *        Applying the ".." path element is perfectly okay as long as the
	 *        path points to a file inside the main directory.)</li>
	 * </ul>
	 * <p>
	 * The implementation of this method is such that concurrent writes can not
	 * take place while this method is running.
	 * It is therefore safe to invoke this method anytime during a session.
	 * 
	 * @param  zipArchive The file path of the zip archive, not allowed to be
	 *         {@code null}.
	 * @param  level the compression level or -1 for the default compression
	 *         level.
	 *         Valid compression levels are greater than or equal to zero and
	 *         less than or equal to nine.
	 * @param  home The information whether all the files should be unzipped
	 *         into a single directory with a name equal to the name of the main
	 *         directory.
	 *         
	 * @throws NullPointerException If {@code zipArchive} is {@code null}.
	 * @throws IllegalArgumentException If the compression level is invalid.
	 * @throws ShutdownException If this database is closed.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	void zip(Path zipArchive, int level, boolean home) throws
											NullPointerException, IllegalArgumentException,
														ShutdownException, IOFailureException;
	/**
	 * Forces any changes to this database being materialized.
	 * This method has no effect if this database is read-only or if the switch
	 * in the database layout labeled {@code forceWriteCommit} is set equal to
	 * "{@code on}".
	 * 
	 * @throws IOFailureException If an I/O error occurs.
	 */
	void forceWrite() throws IOFailureException;
	
	/**
    * Closes this database and releases any system resources associated with
    * it.
    * <p>
    * If this database is already closed then invoking this method has no
    * effect.
    *
    * @throws IOFailureException If an I/O error occurs.
	 */
	@Override
	void close() throws IOFailureException;
	
	/**
	 * As the {@link #name} method, returns the name of this database. 
	 * 
	 * @return The name of this database, never {@code null} and never an empty
	 *         string.
	 */
	@Override
	String toString();
}
