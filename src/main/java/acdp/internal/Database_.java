/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static java.nio.file.StandardOpenOption.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipOutputStream;

import javax.crypto.Cipher;

import acdp.Database;
import acdp.ReadZone;
import acdp.Table;
import acdp.Unit;
import acdp.Information.DatabaseInfo;
import acdp.Information.TableInfo;
import acdp.design.CustomTable;
import acdp.design.ICipherFactory;
import acdp.exceptions.ACDPException;
import acdp.exceptions.ConsistencyException;
import acdp.exceptions.CreationException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.MissingEntryException;
import acdp.exceptions.ShutdownException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.CryptoProvider.ROCrypto;
import acdp.internal.CryptoProvider.WRCrypto;
import acdp.internal.Table_.ColumnParams;
import acdp.internal.Table_.TableParams;
import acdp.internal.misc.FileChannelProvider;
import acdp.internal.misc.ZipEntryCollector;
import acdp.internal.store.wr.WRStore;
import acdp.internal.store.wr.WRtoRO;
import acdp.internal.store.wr.WRStore.GlobalBuffer;
import acdp.misc.Layout;
import acdp.misc.Utils;
import acdp.misc.Layout.LtEntry;

/**
 * Implements a {@linkplain Database database}.
 *
 * @author Beat Hoermann
 */
public final class Database_ implements Database {
	// Replicate C++ friend mechanism, see https://stackoverflow.com/questions/
	// 182278/is-there-a-way-to-simulate-the-c-friend-concept-in-java
	public static final class Friend	{private Friend() {}};
	static final Friend friend = new Friend();
	
	/**
	 * Fills a list with URLs pointing to jar- and class-files.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class URLCollector extends SimpleFileVisitor<Path> {
		private static final String JAR_EXT = ".jar";
		private static final String CLS_EXT = ".class";
		
		private final Path dir;
		private final List<URL> urlList;
		private boolean found;
		
		/**
		 * The constructor.
		 * 
		 * @param dir The directory containing the jar- and/or class-files.
		 * @param urlList The list to fill with URLs of jar- and/or class-files.
		 */
		private URLCollector(Path dir, List<URL> urlList) {
			this.dir = dir;
			this.urlList = urlList;
			found = false;
		}

		@Override
		public FileVisitResult visitFile(Path file,
									BasicFileAttributes attrs) throws FileIOException {
			final String fn = file.getFileName().toString();
			try {
				if (fn.endsWith(JAR_EXT))
					urlList.add(file.toUri().toURL());
				else if (fn.endsWith(CLS_EXT) && !found) {
					urlList.add(dir.toUri().toURL());
					found = true;
				}
			} catch (MalformedURLException e) {
				throw new FileIOException(file, e);
			}
			return FileVisitResult.CONTINUE;
		}
	}
	
	/**
	 * Loads the class with the specified class name.
	 * If the specified classpath string is not equal to {@code null} then this
	 * method creates a class loader on the underlying classpath denoted by the
	 * classpath string, otherwise this method takes the class loader of this
	 * class.
	 * 
	 * @param  cn The name of the class, not allowed to be {@code null}.
	 * @param  cp The classpath string denoting the directory which houses the
	 *         class file of the class with the specified class name and any
	 *         depending class files.
	 *         This value may be {@code null}.
	 * @param  home The home directory.
	 *         If {@code cp} denotes a relative path then the home directory is
	 *         used to convert that relative path to an absolute path.
	 *         
	 * @return The loaded class, never {@code null}.
	 * 
	 * @throws Exception If an exception occurs.
	 *         There are many reasons why this method may throw an exception.
	 */
	public static final Class<?> loadClass(String cn, String cp,
																	Path home) throws Exception {
		final Class<?> cl;
		if (cp != null) {
			// Build classpath.
			final Path classpath = Utils.buildPath(cp, home);
			// Search for jar- and/or class-files in the classpath.
			final List<URL> urlList = new ArrayList<URL>();
			Files.walkFileTree(classpath, new URLCollector(classpath, urlList));
			// Load type factory class.
			try (URLClassLoader clLoader = new URLClassLoader(
															urlList.toArray(new URL[0]))) {
				cl = clLoader.loadClass(cn);
			}
		}
		else {
			cl = Utils.class.getClassLoader().loadClass(cn);
		}
		return cl;
	}
	
	// Keys of layout entries.
	public static final String lt_name = "name";
	public static final String lt_version = "version";
	public static final String lt_consistencyNumber = "consistencyNumber";
	public static final String lt_cipherFactoryClassName =
																		"cipherFactoryClassName";
	public static final String lt_cipherFactoryClasspath =
																		"cipherFactoryClasspath";
	public static final String lt_cipherChallenge = "cipherChallenge";
	public static final String lt_forceWriteCommit = "forceWriteCommit";
	public static final String lt_recFile = "recFile";
	public static final String lt_tables = "tables";
	
	public static final String off = "off";
	public static final String on = "on";
	
	/**
	 * Just implements the {@code DatabaseInfo} interface.
	 * 
	 * @author Beat Hoermann
	 */
	public static final class Info implements DatabaseInfo {
		private final Database_ db;
		private final Layout layout;
		
		/**
		 * Creates the information object of the specified database.
		 * 
		 * @param  db The database, not allowed to be {@code null}
		 * 
		 * @throws NullPointerException If {@code db} is {@code null}.
		 */
		Info(Database_ db) throws NullPointerException {
			this.db = db;
			this.layout = db.dbLayout;
		}
		
		@Override
		public final Path mainFile() {
			return db.mainFile;
		}
		
		@Override
		public final String name() {
			return db.name();
		}

		@Override
		public final String version() {
			return layout.contains(lt_version) ? layout.getString(lt_version) :
																								null;
		}

		@Override
		public final DBType type() {
			return db.wr ? DBType.WR : DBType.RO;
		}

		@Override
		public final boolean isWritable() {
			return db.isWritable();
		}

		@Override
		public final int consistencyNumber() {
		   return Integer.parseInt(layout.getString(lt_consistencyNumber));
		}

		@Override
		public final String cipherFactoryClassName() {
			return layout.contains(lt_cipherFactoryClassName) ?
									layout.getString(lt_cipherFactoryClassName) : null;
		}

		@Override
		public final String cipherFactoryClasspath() {
			return layout.contains(lt_cipherFactoryClasspath) ?
									layout.getString(lt_cipherFactoryClasspath) : null;
		}

		@Override
		public final String cipherChallenge() {
			return layout.contains(lt_cipherChallenge) ?
												layout.getString(lt_cipherChallenge) : null;
		}

		@Override
		public final boolean appliesEncryption() {
			return db.wr && db.wrCrypto != null || !db.wr && db.roCrypto != null;
		}
		
		@Override
		public final boolean encryptsRODatabase() {
			return db.wr && db.roCrypto != null;
		}

		@Override
		public final Path recFile() {
			return db.recFile;
		}
		
		@Override
		public final boolean forceWriteCommit() {
			return db.forceWriteCommit;
		}

		@Override
		public final TableInfo[] getTableInfos() {
			final Table[] tables = db.getTables();
			final TableInfo[] tableInfoArr = new Table_.Info[tables.length];
			int i = 0;
			for (Table table : tables) {
				tableInfoArr[i++] = new Table_.Info((Table_) table);
			}
			return tableInfoArr;
		}
	}
	
	/**
	 * Keeps together the parameters needed to create a database layout and
	 * checks if the given values are plausible.
	 * 
	 * @author Beat Hoermann
	 */
	static final class DatabaseParams {
		private final String name;
		private final String version;
		private final String cipherFactoryClassName;
		private final String cipherFactoryClasspath;
		private final String cipherChallenge;
		private final List<TableParams> listOfTableParams;
		
		/**
		 * The constructor.
		 * 
		 * @param  name The name of the database, not allowed to be {@code null}
		 *         and not allowed to be an empty string.
		 * @param  version The version of the database, may be {@code null} or
		 *         an empty string.
	    * @param  cfcn The class name of the cipher factory, may be {@code null}
	    *         or an empty string.
	    * @param  cfcp The directory housing the class file of the cipher
	    *         factory class and any depending class files, may be {@code
	    *         null} or an empty string.
	    * @param  cc The cipher challenge, may be {@code null} or an empty
	    *         string.
		 * @param  listOfTableParams The parameters of the tables, not allowed to
		 *         be {@code null} and not allowed to be empty.
		 *  
		 * @throws IllegalArgumentException If at least one of the arguments is
		 *         inappropriate.
		 */
		DatabaseParams(String name, String version, String cfcn, String cfcp,
								String cc, List<TableParams> listOfTableParams) throws
																		IllegalArgumentException {
			if (name == null || name.isEmpty())
				throw new IllegalArgumentException("The name of the database is " +
																	"null or an empty string.");
			else if (cfcp != null && !cfcp.isEmpty() && (cfcn == null ||
																					cfcn.isEmpty()))
				throw new IllegalArgumentException(ACDPException.prefix(name,
							null) + "A path for the cipher factory class is " +
									"specified but the name of the class is missing.");
			else if (cc != null && !cc.isEmpty() && (cfcn == null ||
																					cfcn.isEmpty()))
				throw new IllegalArgumentException(ACDPException.prefix(name,
							null) + "A cipher challenge is specified but the name " +
												"of the cipher factory class is missing.");

			else if (cfcn != null && !cfcn.isEmpty() && (cc == null ||
																					cc.isEmpty()))
				throw new IllegalArgumentException(ACDPException.prefix(name,
							null) + "The name of the cipher factory is specified " +
													"but the cipher challenge is missing.");
			else if (listOfTableParams == null || listOfTableParams.size() == 0)
				throw new IllegalArgumentException(ACDPException.prefix(name,
							null) + "The list of table parameters is null or empty.");
			else {
				final Set<String> tableNames = new HashSet<String>();
				for (TableParams tableParams : listOfTableParams) {
					String tableName = tableParams.name();
					if (tableNames.contains(tableName)) {
						throw new IllegalArgumentException(ACDPException.prefix(name,
								tableName) + "The list of table parameters contains " +
													"severeal tables having the same name.");
					}
					tableNames.add(tableName);
				}
			}
			
			this.name = name;
			this.version = version;
			this.cipherFactoryClassName = cfcn;
			this.cipherFactoryClasspath = cfcp;
			this.cipherChallenge = cc;
			this.listOfTableParams = listOfTableParams;
		}
		
		/**
		 * Returns the name of the database.
		 * 
		 * @return The name of the database, never {@code null} and never an
		 *         empty string.
		 */
		final String name() {
			return name;
		}
		
		/**
		 * Returns the version of the database.
		 * 
		 * @return The version of the database, may be {@code null} or an empty
		 *         string.
		 */
		final String version() {
			return version;
		}
		
		/**
		 * Returns the class name of the cipher factory.
		 * 
		 * @return The class name of the cipher factory, may be {@code null} or
		 *         an empty string.
		 */
		final String cipherFactoryClassName() {
			return cipherFactoryClassName;
		}
		
		/**
		 * Returns the classpath of the cipher factory.
		 * 
		 * @return The classpath of the cipher factory, may be {@code null} or
		 *         an empty string.
		 */
		final String cipherFactoryClasspath() {
			return cipherFactoryClasspath;
		}
		
		/**
		 * Returns the cipher challenge.
		 * 
		 * @return The cipher challenge, may be {@code null} or an empty string.
		 */
		final String cipherChallenge() {
			return cipherChallenge;
		}
		
		/**
		 * Returns the parameters of the tables.
		 * 
		 * @return The list of table parameters, never {@code null} and never
		 *         empty and does not contain any two table parameters with the
		 *         same name.
		 */
		final List<TableParams> listOfTableParams() {
			return listOfTableParams;
		}
	}
	/**
	 * The main file of the database, never {@code null}.
	 */
	private final Path mainFile;
	/**
	 * The database layout file used to control access to the WR database via
	 * file locking or {@code null} if the database is an RO database.
	 */
	private final FileIO dbLayoutFile;
	/**
	 * The layout of the database, never {@code null}.
	 */
	private final Layout dbLayout;
	/**
	 * Indicates if the database is a WR database ({@code true}) or a an RO
	 * database ({@code false}).
	 */
	private final boolean wr;
	/**
	 * The name of the database, never {@code null} and never an empty string.
	 */
	private final String name;
	/**
	 * Indicates if the database is writable ({@code true}) or read-only ({@code
	 * false}).
	 */
	private final boolean writable;
	/**
	 * The WR crypto object.
	 * This value is {@code null} if and only if this database is an RO
	 * database or if the database is a WR database and the WR database does
	 * not apply encryption.
	 */
	private final WRCrypto wrCrypto;
	/**
	 * The RO crypto object.
	 * This value is {@code null} if and only if this database is an RO database
	 * and the RO database does not apply encryption or if the database is a
	 * WR database and converting it to an RO database does not require
	 * encrypting the data in the RO database.
	 */
	private final ROCrypto roCrypto;
	/**
	 * The file channel provider.
	 * This value is {@code null} if and only if the database is an RO database
	 * and the operating mode is either -2 or -3.
	 */
	private final FileChannelProvider fcProvider;
	/**
	 * The file space state tracker.
	 * This value is {@code null} if and only if the database is read-only.
	 */
	private final FileSpaceStateTracker fssTracker;
	/**
	 * The recorder file.
	 * This value is {@code null} if and only if the database is an RO database.
	 */
	private final Path recFile;
	/**
	 * See {@linkplain #forceWriteCommit() here}.
	 */
	private final boolean forceWriteCommit;
	/**
	 * The synchronization manager, never {@code null}.
	 */
	private final SyncManager syncManager;
	/**
	 * The table registry, never {@code null} and never empty.
	 * Maps a non-null and non-empty table name to a non-null table object.
	 */
	private final Map<String, Table_> tableReg;
	
	/**
	 * Computes the set of names of the referenced tables from the specified list
	 * of table parameters.
	 * 
	 * @param  listOftableParams The parameters needed to create the individual
	 *         table layouts of the database, not allowed to be {@code null} and
	 *         not allowed to be empty.
	 *         
	 * @return The set of names of the referenced tables, never {@code null} but
	 *         may be empty.
	 */
	private static final Set<String> referencedTables(
														List<TableParams> listOftableParams) {
		Set<String> refdTables = new HashSet<>();
		for (TableParams tp : listOftableParams) {
			for (ColumnParams cp : tp.listOfColumnParams()) {
				final String refdTableName = cp.refdTable();
				if (refdTableName != null) {
					if (refdTableName.equals("."))
						refdTables.add(tp.name());
					else {
						refdTables.add(refdTableName);
					}
				}
			}
		}
		return refdTables;
	}
	
	/**
	 * Creates an returns a new database layout.
	 * <p>
	 * This method does not create the backing files of the database, giving you
	 * a chance for manually modifying the paths to the backing files before the
	 * files are created.
	 * Calling the {@link #createFiles} method eventually creates the backing
	 * files.
	 * 
	 * @param  databaseParams The parameters needed to create the database
	 *         layout, not allowed to be {@code null}.
	 *         
	 * @return The created database layout.
	 * 
	 * @throws NullPointerException If {@code databaseParams} is {@code null}.
	 */
	static final Layout createLayout(DatabaseParams databaseParams)
																	throws NullPointerException {
		Layout layout = new Layout();
		
		// Add layout entries for the database.
		layout.add(lt_name, databaseParams.name());
		final String version = databaseParams.version();
		if (version != null && !version.isEmpty()) {
			layout.add(lt_version, version);
		}
		layout.add(lt_consistencyNumber, "0");
		final String cfcn = databaseParams.cipherFactoryClassName();
		if (cfcn != null && !cfcn.isEmpty()) {
			layout.add(lt_cipherFactoryClassName, cfcn);
			final String cfcp = databaseParams.cipherFactoryClasspath();
			if (cfcp != null && !cfcp.isEmpty()) {
				layout.add(lt_cipherFactoryClasspath, cfcp);
			}
			final String cc = databaseParams.cipherChallenge();
			if (cc != null && !cc.isEmpty()) {
				layout.add(lt_cipherChallenge, cc);
			}
		}
		layout.add(lt_forceWriteCommit, off);
		layout.add(lt_recFile, "rec");
		Layout dbTablesLayout = layout.addLayout(lt_tables);
		
		Set<String> refdTables = referencedTables(databaseParams.
																			listOfTableParams());
		// Create and add table layouts.
		for (TableParams tableParams : databaseParams.listOfTableParams()) {
			final String tableName = tableParams.name();
			dbTablesLayout.add(tableName, Table_.createLayout(tableParams,
															refdTables.contains(tableName)));
		}
		
		return layout;
	}
	
	/**
	 * Creates the backing files of the database, failing if at least one of the
	 * files already exists.
	 * All but the optionally created database's <em>recorder file</em> are
	 * empty.
	 * 
	 * @param  layout The layout of the database, not allowed to be {@code null}.
	 * @param  layoutDir The directory of the layout, not allowed to be
	 *         {@code null}.
	 *         The layout's directory is used to convert relative file paths
	 *         contained in the layout to absolute file paths.
	 *         
	 * @throws NullPointerException If one of the parameters is {@code null}.
	 * @throws MissingEntryException If a required entry in the layout is
	 *         missing.
	 * @throws IllegalArgumentException If the path string of a file in the
	 *         store sublayout of a table layout is an empty string.
	 * @throws InvalidPathException If the path string of a file in the layout
	 *         is invalid.
	 * @throws IOFailureException If a file already exists or if another I/O
	 *         error occurs.
	 */
	public static final void createFiles(Layout layout, Path layoutDir) throws
			NullPointerException, MissingEntryException, IllegalArgumentException,
												InvalidPathException, IOFailureException {
		if (layout.contains(lt_recFile)) {
			Unit_.establishRecorderFile(Utils.buildPath(layout.getString(
																		lt_recFile), layoutDir));
		}
		// Create table files.
		for (LtEntry entry : layout.getLayout(lt_tables).entries()) {
			Table_.createFiles((Layout) entry.value, layoutDir);
		}
	}
	
	/**
	 * The cipher challenge probe.
	 * The cipher challenge probe is an array of arbitrary byte constants.
	 * It is used to find out if a given cipher properly works.
	 */
	private static final byte[] cipherChallengeProbe = { 48, 127, -43, -125,
																-78, 3, -27, 102, 89, 76, -8 };
	
	/**
	 * Creates a cipher object that can be readily used.
	 * 
	 * @param  cf The cipher factory, not allowed to be {@code null}.
	 * @param  wr Indicates if the database is a WR ({@code true}) or an RO
	 *         ({@code false}) database.
	 * @param  encrypt Initialize the cipher for encryption ({@code true}) or
	 *         decryption ({@code false}).
	 * 
	 * @return The cipher or {@code null} if the specified cipher factory is
	 *         implemented such that it returns {@code null} when asked to
	 *         create a cipher for the specified database type.
	 * 
	 * @throws Exception If creating and initializing the cipher fails.
	 */
	private static final Cipher getCipher(ICipherFactory cf, boolean wr,
															boolean encrypt) throws Exception {
		final Cipher cipher;
		if (wr)
			cipher = cf.createAndInitWrCipher(encrypt);
		else {
			cipher = cf.createRoCipher();
			if (cipher != null) {
				cf.initRoCipher(cipher, encrypt);
			}
		}
		return cipher;
	}
	
	/**
	 * Computes the cipher challenge with the specified cipher.
	 * The cipher is assumed to be initialized for encryption.
	 * 
	 * @param  cipher The cipher, initialized for encryption, not allowed to be
	 *         {@code null}.
	 * 
	 * @return The cipher challenge, never {@code null}.
	 * 
	 * @throws CryptoException If computing the cipher challenge fails.
	 */
	private static final String computeCC(Cipher cipher) throws CryptoException {
		try {
			return new BigInteger(cipher.doFinal(cipherChallengeProbe)).
																						toString(36);
		} catch (Exception e) {
			throw new CryptoException(e);
		}
	}
	
	/**
	 * Computes the cipher challenge.
	 * The cipher challenge is used to find out if a given cipher properly works.
	 * It is saved in the database layout.
	 * 
	 * @param  cf The cipher factory, not allowed to be {@code null}.
	 * @param  wr Indicates if the database is a WR ({@code true}) or an RO
	 *         ({@code false}) database.
	 * 
	 * @return The cipher challenge or {@code null} if the specified cipher
	 *         factory is implemented such that it returns {@code null} when
	 *         asked to create a cipher for the specified database type.
	 * 
	 * @throws CreationException If computing the cipher challenge fails.
	 */
	static final String computeCipherChallenge(ICipherFactory cf,
														boolean wr) throws CreationException {
		try {
			final Cipher cipher = getCipher(cf, wr, true);
			if (cipher == null)
				return null;
			else {
				return computeCC(cipher);
			}
		} catch (Exception e) {
			throw new CreationException("Computing cipher challenge failed.", e);
		}
	}
	
	/**
	 * Checks the main file of the database.
	 * 
	 * @throws CreationException If the main file of the database does not exist
	 *         or is not readable or is a directory.
	 */
	private final void checkMainFile() throws NullPointerException,
																				CreationException {
		if (!Files.exists(mainFile))
			throw new CreationException("File does not exist: " + mainFile + ".");
		else if (!Files.isReadable(mainFile))
			throw new CreationException("File is not readable: " + mainFile + ".");
		else if (Files.isDirectory(mainFile)) {
			throw new CreationException("File is a directory: " + mainFile + ".");
		}
	}
	
	/**
	 * Tries to read the layout from the main file of the database assuming that
	 * the database is an RO database.
	 * 
	 * @return The layout of the RO database or {@code null} if the database
	 *         is a WR database or if reading the layout failed due to any other
	 *         reason.
	 */
	private final Layout tryReadingROLayout() {
		Layout layout = null;
		
		try (InputStream is = new FileInputStream(mainFile.toFile())) {
			final byte[] buf = new byte[8];
			int n = is.read(buf);
			while (n != -1 && n < 8) {
				n += is.read(buf, n, 8 - n);
			}
			if (n == 8) {
				// Skip to the beginning of the database layout.
				final long m = Utils.unsFromBytes(buf, 8) - 8;
				if (is.skip(m) == m) {
					try (InputStream gis = new GZIPInputStream(is)) {
						layout = Layout.fromInputStream(gis);
					}
				}
			}
		} catch (Exception e) {
		}
		
		return layout;
	}
	
	/**
	 * Opens the layout file of the database and acquires a lock thus locking the
	 * WR database.
	 * 
	 * @param  writeProtect The information whether the WR database is write
	 *         protected or not.
	 * 
	 * @return The main file of the database.
	 * 
	 * @throws OverlappingFileLockException If the lock cannot be acquired
	 *         because this process (Java virtual machine) already holds a lock
	 *         or another process already holds a lock that cannot co-exist with
	 *         the lock to be acquired.
	 * @throws CreationException If opening the main file of the database fails.
	 */
	private final FileIO lock(boolean writeProtect) throws
										OverlappingFileLockException, CreationException {
		final FileIO file;
		
		try {
			// Open main file.
			if (writeProtect)
				file = new FileIO(mainFile);
			else {
				// An exclusive lock can only be acquired if the file is opened for
				// write access.
				file = new FileIO(mainFile, READ, WRITE);
			}
			// Lock main file.
			// If file.tryLock(...) throws an OverlappingFileLockException then
			// this process already holds a lock. Unfortunately, I couldn't find
			// an easy way to find out if this lock is an exclusive lock.
			// Therefore, we have to live with the fact that we can't open a write
			// protected WR database more than once within the same process.
			if (file.tryLock(0, Long.MAX_VALUE, writeProtect) == null) {
				// Another process already holds a lock that cannot co-exist with
				// the lock that has just been requested.
				throw new OverlappingFileLockException();
			}
		} catch (FileIOException e) {
			throw new CreationException(this, e);
		}
		// file != null
		
		return file;
	}
	
	/**
	 * Reads the database layout from the specified file assuming that the
	 * database is a WR database.
	 * <p>
	 * This method works even if the file is locked with an exclusive lock.
	 * 
	 * @param  layoutFile The file that contains the database layout.
	 * 
	 * @return The layout of the WR database, never {@code null}.
	 * 
	 * @throws CreationException If reading the layout from the main file of the
	 *         database fails.
	 */
	private final Layout readWRLayout(FileIO layoutFile) throws
																				CreationException {
		final Layout layout;
		try {
			// The input stream returned by layoutFile.getInputStream() can't be
			// closed because this would close the underlying file channel which
			// in turn would cause the lock on the layout file to be removed.
			layout = Layout.fromInputStream(layoutFile.getInputStream());
		} catch (Exception e) {
			throw new CreationException("Failed reading database layout: " +
																				mainFile + ".", e);
		}
		return layout;
	}
	
	/**
	 * Checks the specified database layout.
	 * 
	 * @param  layout The database layout.
	 * @param  wr Indicates if the database is a WR ({@code true}) or an RO
	 *         ({@code false}) database.
	 * 
	 * @throws CreationException If the database layout is invalid.
	 */
	private final void checkLayout(Layout layout, boolean wr) throws
																				CreationException {
		try {
			final String name = layout.getString(lt_name);
			if (name.isEmpty())
				throw new IllegalArgumentException("Name of database is an " +
																					"empty string.");
			else if (layout.contains(lt_version) &&
													layout.getString(lt_version).isEmpty())
				throw new IllegalArgumentException("Version of database is an " +
																					"empty string.");
			else if (layout.getString(lt_consistencyNumber).isEmpty())
				throw new IllegalArgumentException("Consistency number of " +
																"database is an empty string.");
			else if (layout.contains(lt_cipherFactoryClassName) &&
								layout.getString(lt_cipherFactoryClassName).isEmpty())
				throw new IllegalArgumentException("The name of the cipher " +
														"factory class is an empty string.");
			else if (wr && layout.getString(lt_recFile).isEmpty())
				throw new IllegalArgumentException("Recorder file path is an " +
																					"empty string.");
			else if (wr && layout.getString(lt_forceWriteCommit).isEmpty())
					throw new IllegalArgumentException("Force write on commit is " +
																				"an empty string.");
			else if (layout.getLayout(lt_tables).size() == 0) {
				throw new IllegalArgumentException("Table layouts are missing.");
			}
			if (layout.contains(lt_cipherFactoryClasspath)) {
				final String cfcp = layout.getString(lt_cipherFactoryClasspath);
				if (cfcp.isEmpty())
					throw new IllegalArgumentException("The path of the cipher " +
														"factory class is an empty string.");
				else if (!layout.contains(lt_cipherFactoryClassName))
					throw new IllegalArgumentException("A path for the cipher " +
											"factory class is specified but the " +
											"name of the class is missing: " + cfcp + ".");
				else if (!Files.isDirectory(Paths.get(cfcp))) {
					throw new IllegalArgumentException("The path of the cipher " +
											"factory is not a  directory: " + cfcp + ".");
				}
			}
			if (layout.contains(lt_cipherChallenge)) {
				final String cc = layout.getString(lt_cipherChallenge);
				if (cc.isEmpty())
					throw new IllegalArgumentException("The cipher challenge is " +
																				"an empty string.");
				else if (!layout.contains(lt_cipherFactoryClassName)) {
					throw new IllegalArgumentException("A cipher challenge is " +
													"specified but the name of the cipher " +
													"factory class is missing: " + cc + ".");
				}
			}
			// Check value of force write commit.
			if (wr) {
				final String val = layout.getString(lt_forceWriteCommit);
				if (!val.equals(on) && !val.equals(off)) {
					throw new IllegalArgumentException("Force write on commit has " +
													"an illegal value: " + val +
													". Legal values are \"on\" or \"off\".");
				}
			}
			// Check if consistency number can be parsed to be string.
			Integer.parseInt(layout.getString(lt_consistencyNumber));
		} catch (Exception e) {
			throw new CreationException(ACDPException.prefix(name, null) +
																"Invalid database layout.", e);
		}
	}
	
	/**
	 * Creates the cipher factory.
	 * 
	 * @param  cn The name of the cipher factory class, not allowed to be {@code
	 *         null}.
	 * @param  cp The classpath string denoting the directory which houses the
	 *         class file of the class with the specified class name and any
	 *         depending class files.
	 *         This value may be {@code null}.
	 * @param  home The home directory.
	 *         If {@code cp} denotes a relative path then the home directory is
	 *         used to convert that relative path to an absolute path.
	 * 
	 * @return The cipher factory, never {@code null}.
	 * 
	 * @throws CreationException If creating the cipher factory fails.
	 */
	private final ICipherFactory createCipherFactory(String cn, String cp,
														Path home) throws CreationException {
		ICipherFactory cipherFactory = null;
		try {
			// Load cipher factory class.
			final Class<?> cl = loadClass(cn, cp, home);
			// Test if cl is indeed a cipher factory class.
			if (!ICipherFactory.class.isAssignableFrom(cl)) {
				throw new IllegalArgumentException("Class name is not a name of " +
															"a cipher factory class: " + cn);
			}
			cipherFactory = (ICipherFactory) cl.newInstance();
		} catch (Exception e) {
			throw new CreationException(this, "Creating cipher failed.", e);
		}
		return cipherFactory;
	}
	
	/**
	 * Verifies the cipher created from the specified cipher factory.
	 * This method throws a {@code CreationException} if verifying the cipher
	 * is not successful or fails.
	 * <p>
	 * The purpose of this method is to ensure that the cipher returned by the
	 * cipher factory is the right one for this database.
	 * Invoking this method already at the stage of opening the database
	 * provides a <em>fail-fast</em> experience if the cipher does not work as
	 * expected.
	 * 
	 * @param  cf The cipher factory, not allowed to be {@code null}.
	 * @param  wr Indicates if the database is a WR ({@code true}) or an RO
	 *         ({@code false}) database.
	 * 
	 * @throws CreationException If verifying the cipher is not successful or
	 *         if there is a problem which prevents this method from running
	 *         properly.
	 */
	private final void verifyCipher(ICipherFactory cf, boolean wr) throws
																				CreationException {
		// Create cipher.
		final Cipher cipher;
		try {
			cipher = getCipher(cf, wr, true);
		} catch (Exception e) {
			throw new CreationException(this, "Creating cipher failed.", e);
		}
		
		if (cipher != null) {
			if (dbLayout.contains(lt_cipherChallenge)) {
				// cipher != null && cipher challenge exists
				final byte[] ccProbe;
				try {
					ccProbe = cipher.doFinal(cipherChallengeProbe);
				} catch (Exception e) {
					throw new CreationException(this, "Verifying the cipher failed.",
																									e);
				}
				if (!dbLayout.getString(lt_cipherChallenge).equals(
													new BigInteger(ccProbe).toString(36))) {
					throw new CreationException(this, "The cipher instance " +
										"derived from the cipher factory \"" +
										cf.getClass().getName() +
										"\" was not able to reproduce the cipher " +
										"challenge registered in the database layout: " +
										dbLayout.getString(lt_cipherChallenge) + ".");
				}
			}
			else {
				// cipher != null && cipher challenge does not exist
				throw new CreationException(this, "Verifying the cipher failed " +
														"because the cipher challenge is " +
														"missing in the database layout.");
			}
		}
		else if (dbLayout.contains(lt_cipherChallenge)) {
			// cipher == null && cipher challenge exists
			throw new CreationException(this, "A cipher challenge is specified " +
										"in the database layout but the cipher factory " +
										"returns null when asked to create the cipher.");
		}
	}
	
	/**
	 * Checks the specified table layouts.
	 * 
	 * @param  tableLtEntries The table layout entries from the layout of the
	 *         database.
	 * 
	 * @throws CreationException If at least one table layout is invalid.
	 */
	private final void checkTableLayouts(LtEntry[] tableLtEntries) throws
																				CreationException {
		String tableName = null;
		try {
			for (LtEntry entry : tableLtEntries) {
				tableName = entry.key;
				if (tableName.isEmpty())
					throw new IllegalArgumentException("The name of the table in " +
											"the \"" + lt_tables + "\" sublayout of the " + 
											"database layout is an empty string.");
				if (tableName.charAt(0) == '#')
					throw new IllegalArgumentException("The name of the table " +
											"starts with a number sign ('#') character.");
				else if (!(entry.value instanceof Layout)) {
					throw new IllegalArgumentException("The name of the table in " +
											"the \"" + lt_tables + "\" sublayout of the " +
											"database layout is associated with a value " +
											"of a type different from \"Layout\".");
				}
			}
		} catch (Exception e) {
			throw new CreationException(this, tableName, "Invalid table layout.",
																									e);
		}
		for (LtEntry entry : tableLtEntries) {
			Table_.checkLayout(this, entry.key, (Layout) entry.value);
		}
	}
	
	/**
	 * Checks the specified array custom tables.
	 * 
	 * @param  customTables The array of custom tables.
	 * @param  tableLts The table layouts as found in the database layout.
	 * 
	 * @throws CreationException If the length of the array of custom tables is
	 *         not equal to the number of table layouts or if at least one
	 *         custom table in the array of custom tables is {@code null} or
	 *         if at least one column instance is reused.
	 */
	private final void checkCustomTables(CustomTable[] customTables,
												Layout tableLts)	throws CreationException {
		if (customTables.length != tableLts.size())
			throw new CreationException(this, "The length of the array of " +
									"custom tables is not equal to the number of " +
									"table sublayouts as found in the database layout.");
		else {
			final Set<Column_<?>> colSet = new HashSet<>();
			for (CustomTable table : customTables) {
				if (table == null) 
					throw new CreationException(this, "At least one table " +
									"contained in the array of custom tables is null.");
				else {
					final Column_<?>[] cols = table.getBackingTable(friend).
																							tableDef();
					for (Column_<?> col : cols) {
						if (colSet.contains(col))
							throw new CreationException(this, "Instance of column \"" +
											col.getClass().getName() + "\" of table \"" +
											table.getClass().getName() + "\" is reused.");
						else {
							colSet.add(col);
						}
					}
				}
			}
		}
	}
	
	/**
	 * Computes the table registry.
	 * 
	 * @param customTables The array of custom tables.
	 * @param tableLtEntries The table layout entries from the layout of the
	 *        database.
	 * 
	 * @return The table registry.
	 */
	private final Map<String, Table_> computeTableReg(CustomTable[] customTables,
																	LtEntry[] tableLtEntries) {
		final int n = customTables.length;
		Map<String, Table_> tableReg = new HashMap<>(n * 4 / 3 + 1);
		for (int i = 0; i < n; i++) {
			tableReg.put(tableLtEntries[i].key, customTables[i].getBackingTable(
																							friend));
		}
		return tableReg;
	}
	
	/**
	 * Iterates the specified table layout entries, creates the associated tables
	 * and, as a side effect, creates and returns the table registry.
	 * <p>
	 * Assumes that the {@link #checkTableLayouts} method has been called before.
	 * 
	 * @param  tableLtEntries The table layout entries from the layout of the
	 *         database.
	 * @param  layoutDir The directory of the layout.
	 *         The layout's directory is used to convert relative file paths
	 *         contained in the layout to absolute file paths.
	 *         
	 * @return The table registry.
	 * 
	 * @throws CreationException If a table can't be created because creating
	 *         the type for at least one column fails due to any reason,
	 *         including an invalid type descriptor or an error while creating
	 *         a custom column type.
	 */
	private final Map<String, Table_> createTables(LtEntry[] tableLtEntries,
												Path layoutDir) throws CreationException {
		Map<String, Table_> tableReg = new HashMap<>();
		for (LtEntry entry : tableLtEntries) {
			final String name = entry.key;
			tableReg.put(name, new Table_(name, (Layout) entry.value, layoutDir,
																								this));
		}
		return tableReg;
	}
	
	/**
	 * Iterates the specified table layout entries and collects for each table
	 * layout the names of the tables referenced by that table layout.
	 * <p>
	 * The result is an empty set if and only if there exists no table in the
	 * database that references itself or another table in the database.
	 * <p>
	 * Assumes that the {@link #checkTableLayouts} method has been called before.
	 * 
	 * @param  tableLts The table layouts as found in the database layout.
	 * @param  tableLtEntries The table layout entries from the layout of the
	 *         database.
	 * 
	 * @return The set of the names of the referenced tables of the database,
	 *         never {@code null} but may be empty.
	 *         
	 * @throws CreationException If at least one of the referenced tables is not
	 *         a table of the database.
	 */
	private final Set<String> referencedTables(Layout tableLts,
									LtEntry[] tableLtEntries) throws CreationException {
		final Set<String> refdTables = new HashSet<>();
		
		for (LtEntry entry : tableLtEntries) {
			final Set<String> set = Table_.referencedTables((Layout) entry.value);
			for (String refdTableName : set) {
				if (!tableLts.contains(refdTableName)) {
					throw new CreationException(this, refdTableName, "Invalid " +
														"table layout because the table " +
														"is referenced by a column but it "+
														"is not a table of the database.");
				}
				refdTables.add(refdTableName);
			}
		}
		
		return refdTables;
	}
	
	/**
	 * Initializes each table contained in the specified table registry and
	 * connects it with a WR store.
	 * <p>
	 * Assumes that the {@link #checkCustomTables} method has been called before.
	 * 
	 * @param  tableReg The table registry.
	 * @param  tableLts The table layouts as found in the database layout.
	 * @param  tableLtEntries The table layout entries from the layout of the
	 *         database.
	 * @param  layoutDir The directory of the layout.
	 *         The layout's directory is used to convert relative file paths
	 *         contained in the layout to absolute file paths.
	 *         
	 * @throws InvalidPathException If the path string of a data file in the
	 *         store sublayout of a table is invalid.
	 * @throws CreationException If at least one column of the table definition
	 *         of a table is not in accordance with its corresponding column as
	 *         defined in the sequence of column sublayouts or if at least one
	 *         of the referenced tables is not a table of the database or if
	 *         the store of a table can't be created due to any reason including
	 *         problems with the store sublayout and the backing files of the
	 *         store.
	 */
	private final void initWRTables(Map<String, Table_> tableReg,
					Layout tableLts, LtEntry[] tableLtEntries, Path layoutDir) throws
													InvalidPathException, CreationException {
		final GlobalBuffer globalBuffer = new GlobalBuffer(2 * Utils.oneMiB);
		final Set<String> refdTables = referencedTables(tableLts, tableLtEntries);
		for (Entry<String, Table_> trEntry : tableReg.entrySet()) {
			final String name = trEntry.getKey();
			trEntry.getValue().initWRTable(this, name, tableLts.getLayout(name),
									layoutDir, refdTables.contains(name), globalBuffer);
		}
	}
	
	/**
	 * Initializes each table contained in the specified table registry and
	 * connects it with an RO store.
	 * <p>
	 * Assumes that the {@link #checkCustomTables} method has been called before.
	 * 
	 * @param  tableReg The table registry.
	 * @param  tableLts The table layouts as found in the database layout.
	 * @param  dbFile The database file of the RO database.
	 * @param  opMode The operating mode of the RO database.
	 *         
	 * @throws ImplementationRestrictionException If a table has too many
	 *         columns.
	 * @throws CreationException If at least one column of the table definition
	 *         of a table is not in accordance with its corresponding column as
	 *         defined in the sequence of column sublayouts or if the store of a
	 *         table can't be created due to any reason including problems with
	 *         the layout and an I/O error while reading the database file.
	 */
	private final void initROTables(Map<String, Table_> tableReg,
										Layout tableLts, Path dbFile, int opMode) throws
								ImplementationRestrictionException, CreationException {
		for (Entry<String, Table_> trEntry : tableReg.entrySet()) {
			final String name = trEntry.getKey();
			trEntry.getValue().initROTable(this, name, tableLts.getLayout(name),
																					dbFile, opMode);
		}
	}
	
	/**
	 * Checks if the recorder file is corrupted or if there exist any
	 * uncommitted write operations from an earlier session.
	 * 
	 * @param  recFilePath The path of the recorder file, not allowed to be
	 *         {@code null}.
	 * 
	 * @throws CreationException If the recorder file is corrupted or if there
	 *         exist any uncommitted write operations from an earlier session or
	 *         if an I/O error occurs.
	 */
	private final void checkRecFile(Path recFilePath) throws CreationException {
		try (FileIO recFile = new FileIO(recFilePath)) {
			if (Unit_.existsUncommittedWrites(recFile)) {
				throw new ACDPException("There exist uncommitted write " +
											"operations! Restart database with write " +
											"protection turned off in order to roll " +
											"back the uncommitted write operations.");
			}
		} catch (FileIOException e) {
			throw new CreationException(this, e);
		} catch (Exception e) {
			throw new CreationException(this, "Recorder file is either " +
											"corrupted or there exist uncommitted write " +
											"operations from an earlier session.", e);
		}
	}
	
	/**
	 * Opens the database.
	 * 
	 * @param  mainFile The main database file, not allowed to be {@code null}.
	 *         If the database is a WR database then the main file is identical
	 *         to the layout file.
	 *         Otherwise, the database is an RO database and the main file is
	 *         the one and only one RO database file.
	 *         Anyway, the main file contains the database layout.
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
	 * @param  writeProtect Protects The database from being written.
	 *         If set to {@code true} then no data can be modified in any of the
	 *         database's tables.
	 *         Note that this parameter has no effect if the database is an RO
	 *         database.
	 *         A read-only database needs less system resources than a writable
	 *         database.
	 * @param  consistencyNumber The consistency number of the database.
	 *         If {@code customTables} is not equal to {@code null} then this
	 *         value must be equal to the corresponding value saved in the
	 *         database layout.
	 *         If this is not the case then this method throws a {@code
	 *         ConsistencyException}.
	 *         If {@code customTables} is equal to {@code null} then this value
	 *         has no effect.
	 * @param  customTables The array of custom tables, not allowed to be empty.
	 *         If the value is {@code null} then this constructor creates a
	 *         weakly typed database and the table parameters are taken from the
	 *         database layout.
	 * 
	 * @throws NullPointerException If {@code mainFile} is {@code null}.
	 * @throws IllegalArgumentException If {@code opMode} is less than -3 or if
	 *         {@code opMode} is equal to -2 or -3 and the database is a WR
	 *         database.
	 * @throws ConsistencyException If the logic of the database is inconsistent
	 *         with its data.
	 *         This exception never happens if the database is a weakly typed
	 *         database.
	 * @throws InvalidPathException If the database layout contains an invalid
	 *         file path.
	 *         This exception never happens if the database is an RO database.
	 * @throws ImplementationRestrictionException If a table has too many
	 *         columns needing a separate null information.
	 * @throws OverlappingFileLockException If the database was already opened
	 *         before in this process (Java virtual machine) or in another
	 *         process that holds a lock which cannot co-exist with the lock to
	 *         be acquired.
	 *         This exception never happens if the database is an RO database.
	 * @throws CreationException If the database can't be created due to any
	 *         other reason including problems with the database layout, problems
	 *         with any of the tables' sublayouts, problems with the optional
	 *         array of custom tables and problems with the backing files of a
	 *         database.
	 */
	public Database_(Path mainFile, int opMode, boolean writeProtect,
							int consistencyNumber, CustomTable[] customTables) throws
										NullPointerException, IllegalArgumentException,
										ConsistencyException, InvalidPathException,
										ImplementationRestrictionException,
										OverlappingFileLockException, CreationException {
		this.mainFile = Objects.requireNonNull(mainFile, "Main database file " +
																"is not allowed to be null.");
		checkMainFile();
		
		// Read DB layout from main file and initialize the flag that tells if
		// the database is a WR or an RO database.
		Layout layout = tryReadingROLayout();
		wr = layout == null;
		if (wr) {
			// Database is a WR database.
			// Acquire a shared (exclusive) lock if the WR database is (not) write
			// protected.
			dbLayoutFile = lock(writeProtect);
			layout = readWRLayout(dbLayoutFile);
		}
		else {
			// Database is an RO database. Do not acquire a lock.
			dbLayoutFile = null;
		}
		
		// Check the database layout.
		checkLayout(layout, wr);
		
		// Initialize some simple fields.
		dbLayout = layout;
		name = dbLayout.getString(lt_name);
		
		// Check data consistency if database is strongly type.
		if (customTables != null) {
			final int dataCN = Integer.parseInt(dbLayout.getString(
																			lt_consistencyNumber));
			if (dataCN != consistencyNumber) {
				throw new ConsistencyException(this, consistencyNumber, dataCN);
			}
		}
		
		// Set the layout directory.
		final Path layoutDir = mainFile.getParent();
		
		// Create crypto providers and verify cipher factory.
		if (!dbLayout.contains(lt_cipherFactoryClassName)) {
			wrCrypto = null;
			roCrypto = null;
		}
		else {
			final ICipherFactory cf = createCipherFactory(
											dbLayout.getString(lt_cipherFactoryClassName),
											dbLayout.contains(lt_cipherFactoryClasspath) ? 
											dbLayout.getString(lt_cipherFactoryClasspath) :
																					null, layoutDir);
			verifyCipher(cf, wr);
			final CryptoProvider cryptoProv = new CryptoProvider(cf);
			wrCrypto = wr ? cryptoProv.createWRCrypto() : null;
			roCrypto = cryptoProv.createROCrypto();
		}
		
		// Check opMode
		if (opMode < -3)
			throw new IllegalArgumentException(ACDPException.prefix(this) +
										"Wrong code for operating mode: " + opMode + ".");
		else if (wr && (opMode == -2 || opMode == -3)) {
			throw new IllegalArgumentException(ACDPException.prefix(this) +
							"Invalid operating mode for WR database: " + opMode + ".");
		}
		
		// Find out if the database is writable.
		// A database is writable if and only if it is a WR database that is not
		// write protected.
		this.writable = wr && !writeProtect;
		
		// Create file channel provider.
		// Note that opMode >= -3 and that opMode == -2 || opMode == -3 imply
		// that the database is an RO database.
		if (writable)
			// opMode >= -1 because database is a WR database.
			fcProvider = new FileChannelProvider(opMode, READ, WRITE);
		else if (opMode != -2 && opMode != -3)
			// !writable
			fcProvider = new FileChannelProvider(opMode, READ);
		else {
			// RO database && (opMode == -2 || opMode == -3)
			fcProvider = null;
		}
		
		// Create file space state tracker.
		fssTracker = writable ? new FileSpaceStateTracker() : null;
		
		// Create the path of the recorder file if the database is a WR database.
		recFile = wr ? Utils.buildPath(dbLayout.getString(lt_recFile),
																				layoutDir) : null;
		// Initialize forceWriteCommit flag.
		forceWriteCommit = wr ? dbLayout.getString(lt_forceWriteCommit).
																				equals(on) : false;
		// Create the synchronization manager.
		// If the database is a WR database that is not write protected then this
		// is the moment where ACDP tries to rollback uncommitted write operations
		// from an earlier session.
		syncManager = new SyncManager(recFile, this);
		
		// If the database is a write protected WR database then throw an
		// exception if there exists uncommitted write operations from an earlier
		// session.
		if (wr && writeProtect) {
			checkRecFile(recFile);
		}
		
		// Prepare for table creation and initialization.
		final Layout tableLts = dbLayout.getLayout(lt_tables);
		final LtEntry[] tableLtEntries = tableLts.entries();
		
		// Check the table layouts.
		checkTableLayouts(tableLtEntries);
		
		// Create tables and table registry.
		if (customTables != null) {
			// Strongly typed database.
			// Check array of custom tables and convert it to a table registry.
			checkCustomTables(customTables, tableLts);
			this.tableReg = computeTableReg(customTables, tableLtEntries);
		}
		else {
			// Weakly typed database.
			// Create weakly typed tables and the table registry.
			this.tableReg = createTables(tableLtEntries, layoutDir);
		}
		
		// Initialize tables. This is the place where the table and columns get
		// their names and where the reference columns get the names of the
		// referenced tables. Furthermore, this is the place where the tables'
		// stores are created.
		if (wr) {
			initWRTables(tableReg, tableLts, tableLtEntries, layoutDir);
			// Initialize the stores of the tables. This must be done after all
			// tables are created and initialized due to stores referencing other
			// stores.
			for (Table_ table : tableReg.values()) {
				table.initWRStore();
			}
		}
		else {
			initROTables(tableReg, tableLts, mainFile, opMode);
			// Initialize the stores of the tables. This must be done after all
			// tables are created and initialized due to stores referencing other
			// stores.
			for (Table_ table : tableReg.values()) {
				table.initROStore();
			}
		}
	}
	
	/**
	 * Returns the layout of this database.
	 * 
	 * @return The database's layout, never {@code null}.
	 */
	public final Layout layout() {
		return dbLayout;
	}
	
	/**
	 * Saves the layout of the WR database.
	 * <p>
	 * This method allows you to save the changes made to the layout of a WR
	 * database even though the layout file is locked.
	 * (On my platform, a shared lock on the layout file prevents layout.save()
	 * from working correctly - it just empties the layout file without throwing
	 * an exception.)
	 * 
	 * @throws NullPointerException If the database is an RO database.
	 * @throws FileIOException If an I/O error occurs.
	 */
	final void saveLayout() throws NullPointerException, FileIOException {
		dbLayoutFile.truncate(0);
		try {
			dbLayout.toOutputStream(dbLayoutFile.getOutputStream(),
																				dbLayout.indent());
		} catch (IOException e) {
			throw new FileIOException(dbLayoutFile.path, e);
		}
	}
	
	/**
	 * Returns the information whether the database is writable or not.
	 * The database is writable if and only if the database is a WR database
	 * and if it is not {@linkplain Database#open write protected}.
	 * 
	 * @return The boolean value {@code true} if the database is writable,
	 *         {@code false} otherwise.
	 */
	public final boolean isWritable() {
		return writable;
	}
	
	/**
	 * Returns the WR crypto object.
	 * 
	 * @return The WR crypto object.
	 *         This value is {@code null} if and only if this database is an RO
	 *         database or if the database is a WR database and the WR database
	 *         does not apply encryption.
	 */
	public final WRCrypto wrCrypto() {
		return wrCrypto;
	}
	
	/**
	 * Returns the RO crypto object.
	 * 
	 * @return The RO crypto object.
	 *         This value is {@code null} if and only if this database is an RO
	 *         database and the RO database does not apply encryption or if the
	 *         database is a WR database and converting it to an RO database
	 *         does not encrypt the data in the RO database.
	 */
	public final ROCrypto roCrypto() {
		return roCrypto;
	}
	
	/**
	 * Returns the file channel provider.
	 * 
	 * @return The file channel provider.
	 *         This value is {@code null} if and only if the database is an RO
	 *         database and the operating mode is either -2 or -3.
	 */
	public final FileChannelProvider fcProvider() {
		return fcProvider;
	}
	
	/**
	 * Returns the file space state tracker.
	 * 
	 * @return The file space state tracker or {@code null} if and only if the
	 *         database is read-only.
	 */
	public final FileSpaceStateTracker fssTracker() {
		return fssTracker;
	}
	
	/**
	 * Indicates if changes to the database are forced being {@linkplain
	 * java.nio.channels.FileChannel#force materialized} when a sequence of
	 * write operations is committed in order to guarantee durability (the "D"
	 * in "ACID") even in the case of a system crash.
	 * If this value is {@code true} then data changes are materialized when
	 * a series of write operations is committed.
	 * Furthermore, changes to the recorder file are immediately materialized.
	 * Otherwise, changes to the database, as well as changes to the recorder
	 * file are forced being materialized not until the database is closed.
	 * Depending on the storage device, changes may be earlier materialized in
	 * parts or as a whole.
	 * 
	 * @return The boolean value {@code true} if changes to the database are
	 *         forced being materialized as part of committing a sequence of
	 *         write operations, {@code false} if changes to the database and
	 *         to the recorder file are forced being materialized not until the
	 *         database is closed.
	 */
	final boolean forceWriteCommit() {
		return forceWriteCommit;
	}
	
	/**
	 * Returns the synchronization manager.
	 * 
	 * @return The synchronization manager, never {@code null}.
	 */
	final SyncManager syncManager() {
		return syncManager;
	}
	
	/**
	 * Opens the ACDP zone.
	 * See section "ACDP zone" of the {@link SyncManager} class description to
	 * learn about the ACDP zone.
	 * 
	 * @throws ShutdownException If the synchronization manager is shut down.
	 * @throws ACDPException If the database is read-only or if this method is
	 *         invoked within a read zone or a unit.
	 */
	public final void openACDPZone() throws ShutdownException, ACDPException {
		syncManager.openACDPZone();
	}
	
	/**
	 * Closes the ACDP zone.
	 * 
	 * @throws ACDPException If the ACDP zone was never opened or it was opened
	 *         in a thread different from the current thread or if the ACDP zone
	 *         contains an open read zone.
	 */
	public final void closeACDPZone() throws ACDPException {
		syncManager.closeACDPZone();
	}
	
	/**
	 * Deeply copies the layout of this WR database and adapts the elements on
	 * the database level so that they fit the requirements of an RO database.
	 * The elements on the level of the tables and the stores are not adapted.
	 * <p>
	 * This database has to be a WR database.
	 * 
	 * @return The copied database layout with elements on the database level
	 *         compatible with the requirements of an RO database.
	 */
	public final Layout createRoLayout() {
		final Layout ro_layout = new Layout(dbLayout);
		
		// Remove "recFile", "forceWrite" and "cipherChallenge" entry.
		ro_layout.remove(Database_.lt_recFile);
		ro_layout.remove(Database_.lt_forceWriteCommit);
		if (ro_layout.contains(Database_.lt_cipherChallenge)) {
			ro_layout.remove(Database_.lt_cipherChallenge);
		}
		
		// Add "cipherChallenge" entry with new value.
		final ROCrypto roCrypto = roCrypto();
		if (roCrypto != null) {
			final Cipher roCipher = roCrypto.get();
			roCrypto.init(roCipher, true);
			// The computeCC method should not throw an exception because the
			// cipher was already tested at the time the database was created.
			ro_layout.add(Database_.lt_cipherChallenge, computeCC(roCipher));
		}
		
		return ro_layout;
	}
	
	@Override
	public final String name() {
		return name;
	}
	
	@Override
	public final DatabaseInfo info() {
		return new Info(this);
	}
	
	@Override
	public final boolean hasTable(String name) {
		return tableReg.get(name) != null;
	}

	@Override
	public final Table getTable(String tableName) throws
																		IllegalArgumentException {
		if (!tableReg.containsKey(tableName)) {
			throw new IllegalArgumentException(ACDPException.prefix(this) +
											"The database has no table with the name \"" +
											tableName + "\".");
		}
		return tableReg.get(tableName);
	}
	
	@Override
	public final Table[] getTables() {
		final Table_[] tables = new Table_[tableReg.size()];
		int i = 0;
		for (LtEntry e : dbLayout.getLayout(lt_tables).entries()) {
			tables[i++] = tableReg.get(e.key);
		}
		return tables;
	}
	
	@Override
	public final Unit getUnit() throws UnitBrokenException, ShutdownException,
																					ACDPException {
		return syncManager.issueUnit();
	}
	
	@Override
	public final ReadZone openReadZone() throws ShutdownException {
		return syncManager.openReadZone();
	}
	
	@Override
	public final void convert(Path roDbFilePath) throws
							UnsupportedOperationException, NullPointerException,
							ImplementationRestrictionException, ShutdownException,
														CryptoException, IOFailureException {
		if (!wr) {
			throw new UnsupportedOperationException(ACDPException.prefix(this) +
																"Database is an RO database.");
		}
		// Database is a WR database.
		new WRtoRO().run(this, roDbFilePath);
	}
	
	@Override
	public final void zip(Path zipArchive, int level, boolean home) throws
										NullPointerException, IllegalArgumentException,
													ShutdownException, IOFailureException {
		try (ReadZone rz = this.openReadZone();
				FileOutputStream fos = new FileOutputStream(zipArchive.toFile());
							BufferedOutputStream bos = new BufferedOutputStream(fos);
							ZipOutputStream zos = new ZipOutputStream(bos)) {
			if (level != -1) {
				zos.setLevel(level);
			}
			final Path mainDir = mainFile.getParent();
			final ZipEntryCollector zec = new ZipEntryCollector(mainDir,
												(home ? mainDir.getFileName() : null), zos);
			
			final Buffer buffer = writable ? new Buffer() : null;
			if (writable)
				// The layout file is locked with an exclusive lock.
				zec.add(mainFile, os -> dbLayoutFile.copyFile(os, buffer));
			else {
				// If this database is a write protected WR database then the layout
				// file is locked with a shared locked. No problems arise in such
				// a case.
				zec.add(mainFile);
			}
			if (wr) {
				if (writable)
					// The recorder file is locked with an exclusive lock.
					zec.add(lt_recFile, dbLayout, os -> syncManager.copyRecFile(os,
																							buffer));
				else {
					zec.add(lt_recFile, dbLayout);
				}
				for (Table_ table : tableReg.values()) {
					((WRStore) table.store()).zip(zec, false);
				}
			}
		} catch (IOException e) {
			throw new IOFailureException(this, e);
		}
	}
	
	@Override
	public final void forceWrite() throws  IOFailureException {
		try {
			syncManager.forceWrite();
		} catch (FileIOException e) {
			throw new IOFailureException(this, e);
		}
	}

	@Override
	public final void close() throws IOFailureException {
		try {
			syncManager.shutdown();
			// Unsynchronized read-only operations may still be running at this
			// moment. In a read-only database all read-only operations are
			// unsynchronized while in a writable database only read-only
			// operations not executed within a read zone or a unit are
			// unsynchronized.
			//
			// After the synchronization manager is shut down it is impossible
			// that the database gets corrupted due to the shutdown of the file
			// channel provider.
			if (fcProvider != null) {
				fcProvider.shutdown();
			}
			// Unsynchronized read-only operations now "receive" an exception if
			// they request a file channel.
			// Remove lock.
			if (dbLayoutFile != null) {
				dbLayoutFile.close();
			}
		} catch (FileIOException e) {
			throw new IOFailureException(this, e);
		}
	}
	
	/**
	 * Removes this WR database from permanent memory.
	 * <p>
	 * Deletes the recorder file and the backing table files of the WR database
	 * and, depending on the value of the {@code deleteLayoutFile} parameter,
	 * the database layout file.
	 * <p>
	 * Note that the database won't work properly anymore after this method has
	 * been executed.
	 * The database should be closed as soon as possible.
	 * 
	 * @param  deleteLayoutFile The information whether the database layout file
	 *         should be deleted or not.
	 *         If set to {@code true} then this method deletes the database
	 *         layout file.
	 * 
	 * @throws UnsupportedOperationException If the database is an RO database.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void deleteFiles(boolean deleteLayoutFile) throws
										UnsupportedOperationException, FileIOException {
		// Delete backing table files.
		for (Table_ table : tableReg.values()) {
			table.deleteFiles();
		}
		// Database is a WR database.
		// Delete recorder file. This works (on my platform only?) although the
		// recorder file is locked with an exclusive lock.
		try {
			Files.delete(recFile);
		} catch(IOException e) {
			new FileIOException(recFile, e);
		}
		// Optionally delete layout file. This works (on my platform only?) even
		// if the layout file is locked with an exclusive lock.
		if (deleteLayoutFile) {
			dbLayoutFile.delete();
		}
	}
	
	@Override
	public String toString() {
		return name;
	}
}