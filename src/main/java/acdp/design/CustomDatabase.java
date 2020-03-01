/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.design;

import java.lang.AutoCloseable;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

import acdp.Database;
import acdp.ReadZone;
import acdp.Unit;
import acdp.Information.DatabaseInfo;
import acdp.exceptions.ACDPException;
import acdp.exceptions.ConsistencyException;
import acdp.exceptions.CreationException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.ShutdownException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.Database_;
import acdp.tools.Setup;

/**
 * Defines the super class of a <em>custom database class</em>.
 * (See the section "Weakly and Strongly Typed" in the description of the
 * {@link Database} interface to learn about the motivation of a custom
 * database class.)
 * <p>
 * At a minimum, a concrete custom database class declares for each table a
 * variable (typically with the {@code final} modifier) referencing that table,
 * for example,
 * 
 * <pre>
 * final MyTable myTable = new MyTable();</pre>
 * 
 * where {@code MyTable} denotes a concrete subclass of the {@link CustomTable}
 * class.
 * If you can prove that your code invokes a table's constructor only once in
 * the java virtual machine then you can declare the table variable as a class
 * variable, hence, with a {@code static} modifier.
 * <p>
 * If you plan to use the {@linkplain Setup Setup Tool} for creating the
 * database layout and all backing files of the database then make sure that
 * your custom database class is annotated with the 
 * {@link Setup.Setup_Database @Setup_Database} annotation and that all of your
 * table declarations are annotated with the
 * {@link Setup.Setup_TableDeclaration @Setup_TableDeclaration} annotation, for
 * example,
 * <pre>
 * {@literal @Setup_Database(}
 *    name = "My Database",
 *    tables = { "My Table", "Your Table" }
 * )
 * public final class MyDB extends CustomDatabase {
 *    {@literal @Setup_TableDeclaration("My Table")}
 *    final MyTable myTable = new MyTable();
 *    {@literal @Setup_TableDeclaration("Your Table")}
 *    final YourTable yourTable = new YourTable();
 * 
 *    MyDB(...) {
 *       open(..., myTable, yourTable);
 *    }
 * }</pre>
 * <p>
 * Since an ACDP database must finally be closed, you may want to apply the
 * <em>try-with-resources</em> statement like this ({@code MyDB} denotes a
 * subclass of the {@code CustomDatabase} class):
 * 
 * <pre>
 * try (MyDB db = new MyDB(...)) {
 *    MyTable myTable = db.myTable;
 *    <em>do something with</em> myTable
 * }</pre>
 * 
 * This code snippet also shows how to access a particular table (here the
 * "My Table") from a <em>strongly typed</em> custom database.
 * Compare this to the corresponding code snippet in the {@link Database} class
 * description.
 *
 * @author Beat Hoermann
 */
public abstract class CustomDatabase implements AutoCloseable {
	/**
	 * The database to which almost all of the methods provided by this class
	 * are delegated.
	 */
	private Database db = null;
	/**
	 * The information whether the database is open or closed.
	 */
	private boolean open = false;
	/**
	 * The array of custom tables as passed to the open method.
	 */
	private CustomTable[] customTables = null;
	
	/**
	 * Constructs but does not {@linkplain #open(Path, int, boolean, int,
	 * CustomTable...) open} the database.
	 */
	protected CustomDatabase() {
	}
	
	/**
	 * Returns the information whether the database is open or closed.
	 * 
	 * @return The boolean value {@code true} if and only if the database is
	 *         open.
	 */
	protected final boolean isOpen() {
		return open;
	}
	
	/**
	 * Opens the database as a <em>strongly typed</em> database.
	 * <p>
	 * The order of the custom tables in the specified array of custom tables
	 * must be the same as the order in which the corresponding table layouts
	 * appear in the database layout.
	 * <p>
	 * The database can only be opened once.
	 * This method throws an {@code IllegalStateException} if the database is
	 * opened again, regardless of whether the database is currently open or
	 * closed.
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
	 *         If the value is equal to -2 then the compressed content of an RO
	 *         database is completely mapped into memory.
	 *         If the value is equal to -3 then the <em>un</em>compressed
	 *         content of an RO database is completely mapped into memory.
	 *         A value equal to zero is recommended only if the database is a
	 *         WR database and the operating system can't sustain a bunch of
	 *         files all being opened at the same time.
	 *         In a server environment a positive value is recommended or,
	 *         provided that the database is an RO database and there is enough
	 *         memory available, a value equal to -2 (less memory required, less
	 *         fast) or -3 (more memory required, faster).
	 *         Note that a value equal to -2 or equal to -3 raises an {@code
	 *         IllegalArgumentException} if the database is a WR database.
	 * @param  writeProtect Protects the database from being written.
	 *         If set to {@code true} then no data can be modified in any of the
	 *         database's tables.
	 *         Note that this parameter has no effect if the database is an RO
	 *         database.
	 *         A read-only database needs less system resources than a writable
	 *         database.
	 * @param  consistencyNumber The consistency number of the database.
	 *         This value must be equal to the corresponding value saved in the
	 *         database layout.
	 *         If this is not the case then this method throws a {@code
	 *         ConsistencyException}.
	 * @param  customTables The array of custom tables, not allowed to be {@code
	 *         null} and not allowed to be empty.
	 * 
	 * @throws NullPointerException If {@code mainFile} or {@code customTables}
	 *         is {@code null}.
	 * @throws IllegalArgumentException If {@code opMode} is less than -3 or if
	 *         {@code opMode} is equal to -2 or -3 and the database is a WR
	 *         database.
	 * @throws ConsistencyException If the logic of the database is inconsistent
	 *         with its data.
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
	 *         with any of the tables' sublayouts, problems with the array of
	 *         custom tables and problems with the backing files of the database.
	 * @throws IllegalStateException If the database was opened before.
	 */
	protected final void open(Path mainFile, int opMode, boolean writeProtect,
							int consistencyNumber, CustomTable... customTables) throws
										NullPointerException, IllegalArgumentException,
										ConsistencyException, InvalidPathException,
										ImplementationRestrictionException,
										OverlappingFileLockException, CreationException,
										IllegalStateException {
		Objects.requireNonNull(customTables, "The array of custom tables is " +
																	"not allowed to be null.");
		if (db != null) {
			throw new IllegalStateException("Database was opened before. Can't " +
															"open database more than once.");
		}
		// customTables != null && db == null
		db = new Database_(mainFile, opMode, writeProtect, consistencyNumber,
																						customTables);
		open = true;
		this.customTables = customTables;
		
		for (CustomTable table : customTables) {
			table.setDatabase(this);
		}
	}
	
	/**
	 * Checks if the database is open.
	 * 
	 * @throws IllegalStateException If the database is closed.
	 */
	private final void checkIfOpen() throws IllegalStateException {
		if (!open) {
			throw new IllegalStateException(db == null ? "Database is closed." :
														"Database \"" + db + "\" ist closed");
		}
	}
	
	/**
	 * See {@linkplain Database#name()}
	 * 
	 * @return The name of the database, never {@code null} and never an empty
	 *         string.
	 * 
	 * @throws IllegalStateException If the database was never opened.
	 */
	public final String name() throws IllegalStateException {
		if (db == null) {
			throw new IllegalStateException("Database was never opened.");
		}
		return db.name();
	}
	
	/**
	 * See {@linkplain Database#info()}
	 * 
	 * @return The information object of the database, never {@code null}.
	 * 
	 * @throws IllegalStateException If the database is closed.
	 */
	protected final DatabaseInfo getInfo() throws IllegalStateException {
		checkIfOpen();
		return db.info();
	}
	
	/**
	 * Returns all tables of this database.
	 * <p>
	 * The returned value is a copy of the {@code customTables} argument of
	 * the {@link #open(Path, int, boolean, int, CustomTable...) open} method.
	 * 
	 * @return The tables of the database, never {@code null}.
	 * 
	 * @throws IllegalStateException If the database is closed.
	 */
	protected final CustomTable[] getTables() throws IllegalStateException {
		checkIfOpen();
		return Arrays.copyOf(customTables, customTables.length);
	}
	
	/**
	 * See {@linkplain Database#getUnit()}
	 * 
	 * @return The unit, never <code>null</code>.
	 * 
	 * @throws UnitBrokenException If the unit is broken.
	 * @throws ShutdownException If the database is closed.
	 * @throws ACDPException If the database is read-only or if this method is
	 *         invoked within a read zone.
	 * @throws IllegalStateException If the database is closed.
	 */
	public final Unit getUnit() throws UnitBrokenException, ShutdownException,
														ACDPException, IllegalStateException {
		checkIfOpen();
		return db.getUnit();
	}
	
	/**
	 * See {@linkplain Database#openReadZone()}
	 * 
	 * @return  The read zone, never {@code null}.
	 * 
	 * @throws ShutdownException If the synchronization manager is shut down.
	 *         This exception never happens if the database is read-only.
	 * @throws IllegalStateException If the database is closed.
	 */
	public final ReadZone openReadZone() throws ShutdownException,
																			IllegalStateException {
		checkIfOpen();
		return db.openReadZone();
	}
	
	/**
	 * See {@linkplain Database#convert(Path)}
	 * 
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
	 * @throws ShutdownException If the database's file channel provider or the
	 *         synchroniziation manager is shut down.
	 *         A reason for this exception to happen is that the WR database is
	 *         closed.
	 * @throws CryptoException If decrypting the byte representation of a stored
	 *         column value of a table of the WR database fails.
	 *         This exception never happens if the WR database does not apply
	 *         encryption.
	 * @throws IOFailureException If the specified file already exists or if
	 *         another I/O error occurs.
	 * @throws IllegalStateException If the database is closed.
	 */
	protected final void convert(Path roDbFilePath) throws
						UnsupportedOperationException, NullPointerException,
						ImplementationRestrictionException, ShutdownException,
						CryptoException, IOFailureException, IllegalStateException {
		checkIfOpen();
		db.convert(roDbFilePath);
	}
	
	/**
	 * See {@linkplain Database#zip(Path, int, boolean)}
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
	 * @throws ShutdownException If the database's file channel provider or the
	 *         synchroniziation manager is shut down.
	 *         A reason for this exception to happen is that the WR database is
	 *         closed.
	 * @throws IOFailureException If an I/O error occurs.
	 * @throws IllegalStateException If the database is closed.
	 */
	protected final void zip(Path zipArchive, int level, boolean home) throws
											UnsupportedOperationException,
											NullPointerException, IllegalArgumentException,
											ShutdownException, IOFailureException,
											IllegalStateException {
		checkIfOpen();
		db.zip(zipArchive, level, home);
	}
	
	/**
	 * See {@linkplain Database#forceWrite}
	 * 
	 * @throws IOFailureException If an I/O error occurs.
	 * @throws IllegalStateException If the database is closed.
	 */
	public final void forceWrite() throws IOFailureException,
																			IllegalStateException {
		checkIfOpen();
		db.forceWrite();
	}
	
	/**
	 * This method is invoked by the {@link #close} method immediately before
	 * the database is closed, provided that the database is {@linkplain
	 * #open(Path, int, boolean, int, CustomTable...) open}.
	 * <p>
	 * This implementation does nothing.
	 * Subclasses may want to override this method to close, for example, an
	 * index file.
	 */
	protected void customClose() {
	}
	
	/**
    * Closes this database and releases any system resources associated with
    * it.
    * <p>
    * Note that closing the database is final: Reopening the database by
    * invoking the {@link #open(Path, int, boolean, int, CustomTable...) open}
    * method will fail.
    * <p>
    * If the database is closed then invoking this method has no effect.
    *
    * @throws IOFailureException if an I/O error occurs.
	 */
	@Override
	public final void close() throws IOFailureException {
		if (open) {
			// open implies db != null
			customClose();
			db.close();
			open = false;
		}
	}
	
	/**
	 * See {@linkplain Database#toString()}
	 * 
	 * @throws IllegalStateException If the database was never opened.
	 */
	@Override
	public String toString() throws IllegalStateException {
		if (db == null) {
			throw new IllegalStateException("Database was never opened.");
		}
		return db.toString();
	}
}
