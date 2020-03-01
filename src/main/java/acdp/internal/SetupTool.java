/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import acdp.Column;
import acdp.design.CustomDatabase;
import acdp.design.CustomTable;
import acdp.design.ICipherFactory;
import acdp.exceptions.CreationException;
import acdp.internal.Database_.DatabaseParams;
import acdp.internal.Table_.ColumnParams;
import acdp.internal.Table_.TableParams;
import acdp.misc.Layout;
import acdp.tools.Setup;
import acdp.tools.Setup.Setup_Column;
import acdp.tools.Setup.Setup_Database;
import acdp.tools.Setup.Setup_Table;
import acdp.tools.Setup.Setup_TableDeclaration;
import acdp.types.ArrayOfRefType;
import acdp.types.RefType;
import acdp.types.Type;

/**
 * See {@linkplain Setup here}.
 *
 * @author Beat Hoermann
 */
public final class SetupTool {
	/**
	 * Prints an error message to the "standard" output stream, including the
	 * specified class name and the specified message into the printed text.
	 * 
	 * @param className The class name.
	 * @param msg The message.
	 */
	private final void error(String className, String msg) {
		System.out.println("ERROR \"" + className + "\": " + msg);
	}
	
	/**
	 * Prints an error message to the "standard" output stream, including the
	 * specified arguments into the printed text.
	 * 
	 * @param table The name of a table or the name of a table class.
	 * @param col the name of a column.
	 * @param msg The message.
	 */
	private final void error(String table, String col, String msg) {
		System.out.println("ERROR \"" + table + "\", \"" + col + "\": " + msg);
	}
	
	/**
	 * Creates the cipher factory from the specified class.
	 * 
	 * @param  cfClass The cipher factory class, not allowed to be {@code null}.
	 * 
	 * @return The cipher factory or {@code null} if creating the cipher factory
	 *         fails.
	 */
	private final ICipherFactory createCipherFactory(Class<?> cfClass) {
		// Does the class implement the ICipherFactory interface?
		if (!ICipherFactory.class.isAssignableFrom(cfClass)) {
			System.out.println("ERROR: Class \"" + cfClass.getName() +
							"\" does not implement the \"ICipherFactory\" interface.");
			return null;
		}
			
		// Try creating an instance.
		final ICipherFactory cf;
		try {
			cf = (ICipherFactory) cfClass.newInstance();
		} catch (ReflectiveOperationException e) {
			System.out.println("ERROR: Cannot create an instance of the cipher " +
							"factory using the \"" + cfClass.getName() + "\" class. " +
							"Ensure that the class has a no-argument constructor " +
							"and that the access level modifier for both, the class " +
							"and the constructor is set to \"public\".");
			return null;
		}
		return cf;
	}
	
	/**
	 * Creates the cipher factory from the specified cipher factory class and
	 * tests it.
	 * 
	 * @param  cfClass The cipher factory class, not allowed to be {@code null}.
	 *         
	 * @return The boolean value {@code true} if the test is successful, {@code
	 *         false} otherwise.
	 *         
	 * @throws CreationException If the test fails due to an incorrect
	 *         implementation of the cipher factory.
	 */
	private final boolean testCipherFactory(Class<?> cfClass) throws
																				CreationException {
		final ICipherFactory cf = createCipherFactory(cfClass);
		if (cf == null)
			return false;
		else {
			// cf != null
			try {
				final CryptoProvider cryptoProv = new CryptoProvider(cf);
				cryptoProv.createWRCrypto();
				cryptoProv.createROCrypto();
			} catch (CreationException e) {
				System.out.println("ERROR: Creating a crypto object with the " +
											"cipher factory created from the \"" +
											cfClass.getName() + "\" class failed. For " +
											"further details consult the stacktrace.");
				throw e;
			}
			return true;
		}
	}
	
	/**
	 * Loads the class from the specified class name and tests if the class is
	 * a valid database class.
	 * 
	 * @param  dbcn The name of the database class.
	 * 
	 * @return The valid database class or {@code null} if loading the class
	 *         failed or the class turned out to not to be a database class.
	 */
	private final Class<?> getDbClass(String dbcn) {
		Class<?> dbClass = null;
		
		try {
			dbClass = SetupTool.class.getClassLoader().loadClass(dbcn);
		} catch (ClassNotFoundException | NoClassDefFoundError e) {
			error(dbcn, "Class not found. Is the class on the classpath?");
			return null;
		}
		// dbClass != null
		
		boolean error = false;
		if (!dbClass.isAnnotationPresent(Setup_Database.class)) {
			error(dbcn, "Class not annotated with the \"@Setup_Database\" " +
							"annotation.");
			error = true;
		}
		
		if (!CustomDatabase.class.isAssignableFrom(dbClass)) {
			error(dbcn,"Class is not a subclass of the \"CustomDatabase\" class.");
			error = true;
		}
		
		if (error)
			return null;
		else {
			return dbClass;
		}
	}
	
	/**
	 * Loads the cipher factory class from the specified cipher factory class
	 * name considering the specified classpath.
	 * 
	 * @param  dbcn The name of the database class.
	 * @param  cfcn The name of the cipher factory class, not allowed to be
	 *         an empty string.
	 * @param  cfcp The classpath of the cipher factory.
	 * 
	 * @return The cipher factory class or {@code null} if loading the class
	 *         failed.
	 */
	private final Class<?> getCipherFactoryClass(String dbcn, String cfcn,
																						String cfcp) {
		Class<?> cfClass = null;
		
		if (cfcp.isEmpty()) {
			try {
				cfClass = SetupTool.class.getClassLoader().loadClass(cfcn);
			} catch (ClassNotFoundException | NoClassDefFoundError e) {
				error(dbcn, "Class \"" + cfcn + "\" not found. Is the class " +
																		"on the classpath?");
			}
		}
		else {
			boolean error = false;
			try {
				Paths.get(cfcp);
			} catch (InvalidPathException e) {
				error(dbcn, "The annotated classpath for the cipher factory " +
																		"is invalid: " + cfcp);
				error = true;
			}
			
			if (!error) {
				try {
					cfClass = Database_.loadClass(cfcn, cfcp,
														Paths.get(".").toAbsolutePath());
				} catch (Exception e0) {
					// Try without extra classpath.
					try {
						cfClass = SetupTool.class.getClassLoader().loadClass(cfcn);
					} catch (ClassNotFoundException | NoClassDefFoundError e) {
						error(dbcn, "Class \"" + cfcn + "\" not found. Resolving " +
										"with the annotated classpath \"" + cfcp +
										"\" didn't succeed either. If annotated " +
										"classpath is relative then put the class on " +
										"the classpath of the Setup Tool for the " +
										"moment and try again.");
					}
				}
			}
		}
		
		return cfClass;
	}
	
	/**
	 * Checks the specified column names and the specified declared fields of
	 * the table class for correctness and computes the map that maps a column
	 * name to its corresponding declared field.
	 * 
	 * @param  tcn The name of the table class.
	 * @param  cNames The annotated column names.
	 * @param  declFields The declared fields of the table class.
	 * 
	 * @return The map or {@code null} if computing the map fails.
	 */
	private final Map<String, Field> colsMap(String tcn, String[] cNames,
																			Field[] declFields) {
		final Map<String, Field> map = new HashMap<>();
		
		// Get the sublist of annotated fields.
		final List<Field> annFields = new ArrayList<>();
		for (Field field : declFields) {
			if (field.isAnnotationPresent(Setup_Column.class)) {
				annFields.add(field);
			}
		}
		
		for (String cn : cNames) {
			if (cn.isEmpty()) {
				error(tcn, "The \"value\" element of the \"@Setup_Table\" " +
						"annotation contains an empty string. The name of a column " +
						"is not allowed to be an empty string.");
				return null;
			}
			
			// Find the field annotated with the name.
			Field theField = null;
			int i = 0;
			while (i < annFields.size() && theField == null) {
				final Field annField = annFields.get(i++);
				if (cn.equals(annField.getAnnotation(Setup_Column.class).value())) {
					theField = annField;
				}
			}
			
			if (theField == null) {
				error(tcn, "The \"value\" element of the \"@Setup_Table\" " +
						"annotation contains the \"" + cn + "\" column name which " +
						"has no corresponding declared field annotated with the " +
						"\"@Setup_Column\" annotation.");
				return null;
			}
			else if (map.containsKey(cn)) {
				error(tcn, "The \"value\" element of the \"@Setup_Table\" " +
						"annotation contains the \"" + cn + "\" column name more " +
						"than once. Column names must be unique within an ACDP " +
						"table.");
				return null;
			}
			else {
				map.put(cn, theField);
			}
		}
		
		if (map.size() < annFields.size()) {
			error(tcn, "There are fields that are annotated with the \"" +
						"@Setup_Column\" annotation having a \"value\" " +
						"that is not unique or that has no corresponding element " +
						"in the \"value\" element of the \"@Setup_Table\" " +
						"annotation.");
			return null;
		}

		return map;
	}
	
	/**
	 * Computes the column parameters.
	 * As a side effect this method computes the list of column objects.
	 * 
	 * @param  tcn The name of the table class.
	 * @param  tClass The table class.
	 * @param  cols The empty list of column objects.
	 *         As a side effect this methods fills this list with the column
	 *         objects
	 *         
	 * @return The column parameters or {@code null} if computing the column
	 *         parameters failed.
	 */
	private final List<ColumnParams> getColumnParams(String tcn, Class<?> tClass,
																			List<Column<?>> cols) {
		final List<ColumnParams> columnParams = new ArrayList<>();
		
		final String[] cNames = tClass.getAnnotation(Setup_Table.class).value();
		final Map<String, Field> map = colsMap(tcn, cNames, tClass.
																			getDeclaredFields());
		if (map == null) {
			return null;
		}
		
		for (String cn : cNames) {
			final Field field = map.get(cn);
			
			Object table = null;
			try {
				table = tClass.newInstance();
			} catch (IllegalAccessException e) {
				error(tcn, "Can't instantiate table class. Ensure that the " +
									"no-arg constructor is declared with a public " +
									"access level modifier.");
				return null;
			} catch (InstantiationException e) {
				error(tcn, "Can't instantiate table class. Ensure that there " +
									"exists a no-arg constructor.");
				return null;
			}
			
			// The column itself.
			Column<?> col = null;
			try {
				col = (Column<?>) field.get(table);
			} catch (IllegalAccessException e) {
				error(tcn, cn, "Can't access the column. Ensure that table " +
									"class and column are declared with a public " +
									"access level modifier.");
				return null;
			} catch (NullPointerException e) {
				error(tcn, cn, "Column does not seem to be declared static.");
				return null;
			}
			
			if (col == null) {
				error(tcn, cn, "Column does not seem to be declared final.");
				return null;
			}
			
			// Get the name of the referenced table if the type of the column
			// references a table.
			final String refdTable = field.getAnnotation(Setup_Column.class).
																						refdTable();
			final Type colType = ((Column_<?>) col).type();
			if (colType instanceof RefType || colType instanceof ArrayOfRefType) {
				if (refdTable.isEmpty()) {
					error(tcn, cn, "The column is not annotated with the name " +
										"of a referenced table, although the column " +
										"references a table.");
					return null;
				}
			}
			else if (!refdTable.isEmpty()) {
				error(tcn, cn, "The column is annotated with the referenced " +
										"table \"" + refdTable + "\" but the column " +
										"does not reference a table.");
				return null;
			}
			
			cols.add(col);
			columnParams.add(new ColumnParams(cn, refdTable.isEmpty() ? null :
																						refdTable));
		}
		
		return columnParams;
	}
	
	/**
	 * Checks the specified table names and the specified declared fields of
	 * the database class for correctness and computes the map that maps a table
	 * name to its corresponding declared field.
	 * 
	 * @param  dbcn The name of the database class.
	 * @param  tNames The annotated table names.
	 * @param  declFields The declared fields of the database class.
	 * 
	 * @return The map or {@code null} if computing the map fails.
	 */
	private final Map<String, Field> tablesMap(String dbcn, String[] tNames,
																			Field[] declFields) {
		final Map<String, Field> map = new HashMap<>();
		
		// Get the sublist of annotated fields.
		final List<Field> annFields = new ArrayList<>();
		for (Field field : declFields) {
			if (field.isAnnotationPresent(Setup_TableDeclaration.class)) {
				annFields.add(field);
			}
		}
		
		for (String tn : tNames) {
			if (tn.isEmpty()) {
				error(dbcn, "The \"tables\" element of the \"@Setup_Database\" " +
						"annotation contains an empty string. The name of a table " +
						"is not allowed to be an empty string.");
				return null;
			}
			if (tn.charAt(0) == '#')  {
				error(dbcn, "The \"tables\" element of the \"@Setup_Database\" " +
						"annotation contains a table name that starts with the " +
						"number sign ('#') character. The name of a table is not " +
						"allowed to start with the number sign ('#') character.");
				return null;
			}
			
			// Find the field annotated with the name.
			Field theField = null;
			int i = 0;
			while (i < annFields.size() && theField == null) {
				final Field annField = annFields.get(i++);
				if (tn.equals(annField.getAnnotation(Setup_TableDeclaration.class).
																							value())) {
					theField = annField;
				}
			}
			
			if (theField == null) {
				error(dbcn, "The \"tables\" element of the \"@Setup_Database\" " +
						"annotation contains the \"" + tn + "\" table name which " +
						"has no corresponding declared field annotated with the " +
						"\"@Setup_TableDeclaration\" annotation.");
				return null;
			}
			else if (map.containsKey(tn)) {
				error(dbcn, "The \"tables\" element of the \"@Setup_Database\" " +
						"annotation contains the \"" + tn + "\" table name more " +
						"than once. Table names must be unique within an ACDP " +
						"database.");
				return null;
			}
			else {
				map.put(tn, theField);
			}
		}
		
		if (map.size() < annFields.size()) {
			error(dbcn, "There are fields that are annotated with the \"" +
						"@Setup_TableDeclaration\" annotation having a \"value\" " +
						"that is not unique or that has no corresponding element " +
						"in the \"tables\" element of the \"@Setup_Database\" " +
						"annotation.");
			return null;
		}

		return map;
	}
	
	/**
	 * Computes the table parameters.
	 * 
	 * @param  dbcn The name of the database class.
	 * @param  dbClass The database class.
	 * @param  tNames The annotated table names.
	 * 
	 * @return The table parameters or {@code null} if computing the table
	 *         parameters failed.
	 */
	private final List<TableParams> getTableParams(String dbcn,
														Class<?> dbClass, String[] tNames) {
		final List<TableParams> tableParams = new ArrayList<>();
		
		final Map<String, Field> map = tablesMap(dbcn, tNames, dbClass.
																			getDeclaredFields());
		if (map == null) {
			return null;
		}
		
		for (String tn : tNames) {
			final Class<?> tClass = map.get(tn).getType();
			final String tcn = tClass.getName();

			if (!tClass.isAnnotationPresent(Setup_Table.class)) {
				error(tcn, "Class not annotated with the \"@Setup_Table\" " +
								"annotation.");
				return null;
			}
			
			if (!CustomTable.class.isAssignableFrom(tClass)) {
				error(tcn, "The class is not a subclass of the \"CustomTable\" " +
								"class.");
				return null;
			}
			
			final List<Column<?>> cols = new ArrayList<>();
			
			// Compute the column parameters.
			final List<ColumnParams> columnParams = getColumnParams(tcn, tClass,
																								cols);
			if (columnParams == null) {
				return null;
			}
			
			if (columnParams.size() == 0) {
				error(tcn, "The \"value\" element of the \"@Setup_Table\" " +
								"annotation is an empty array. An ACDP table must " +
								"have at least one column.");
				return null;
			}
			
			tableParams.add(new TableParams(tn, cols.toArray(new Column_<?>[
																cols.size()]), columnParams));
		}
		if (tableParams.size() == 0) {
			error(dbcn, "The \"tables\" element of the \"@Setup_Database\" " +
							"annotation is an empty array. An ACDP database must " +
							"have at least one table.");
			return null;
		}
		
		// Test if referenced tables exist.
		final Set<String> tNamesSet = map.keySet();
		for (TableParams tParams : tableParams) {
			for (ColumnParams cParams : tParams.listOfColumnParams()) {
				String refdTable = cParams.refdTable();
				if (refdTable != null && !tNamesSet.contains(refdTable)) {
					error(tParams.name(), cParams.name(), "Column references " +
											"non-existent table \"" + refdTable + "\".");
					return null;
				}
			}
		}
		
		return tableParams;
	}
	
	/**
	 * Computes the database parameters.
	 * 
	 * @param  dbcn The name of the database class.
	 * 
	 * @return The database parameters, or {@code null} if computing the
	 *         database parameters fails.
	 */
	private final DatabaseParams getDatabaseParams(String dbcn) {
		Class<?> dbClass = getDbClass(dbcn);
		if (dbClass == null) {
			return null;
		}
		// dbClass != null && valid database class.
		
		final Setup_Database dbAnn = dbClass.getAnnotation(Setup_Database.class);
		
		// Name of database.
		final String dbName = dbAnn.name();
		if (dbName.isEmpty()) {
			error(dbcn, "The database name is not allowed to be an empty string.");
			return null;
		}
		
		// Optional version of database.
		final String dbVersion = dbAnn.version();
		
		// Optional class name of cipher factory.
		String cfcn = dbAnn.cipherFactoryClassName();

		// Optional classpath of cipher factory.
		final String cfcp = dbAnn.cipherFactoryClasspath();
		if (cfcn.isEmpty() && !cfcp.isEmpty()) {
			error(dbcn, "The database is annotated with the following path for " +
							"the cipher factory but the cipher factory class name " +
							"itself is not annotated:" + cfcp + ".");
			return null;
		}
		
		Class<?> cfClass = null;
		if (cfcn.isEmpty()) {
			// Try default class name of the cipher factory.
			cfcn = dbcn + "$CipherFactory";
			try {
				cfClass = SetupTool.class.getClassLoader().loadClass(cfcn);
			} catch (ClassNotFoundException | NoClassDefFoundError e) {
				// No cipher factory available: Database does not apply encryption.
				cfcn = "";
			}
		}
		else {
			cfClass = getCipherFactoryClass(dbcn, cfcn, cfcp);
			if (cfClass == null) {
				return null;
			}
		}
		// cfcn.isEmtpy() if and only if cfClass == null
		
		// Test the cipher factory class.
		if (cfClass != null) {
			if (!testCipherFactory(cfClass)) {
				return null;
			}
		}
		
		// Compute the cipher challenge, if necessary.
		final String cc = cfClass == null ? null : Database_.
							computeCipherChallenge(createCipherFactory(cfClass), true);
		if (cc != null) {
			System.out.println("INFO: The cipher challenge is \"" + cc + "\".");
		}
		
		// Compute the table parameters.
		final List<TableParams> tableParams = getTableParams(dbcn, dbClass,
																					dbAnn.tables());
		if (tableParams == null) {
			return null;
		}
		
		return new DatabaseParams(dbName, dbVersion, cfcn, cfcp, cc, tableParams);
	}
	
	/**
	 * Creates the layout directory if it does not yet exist.
	 * <p>
	 * If the specified directory is {@code null} then this method returns
	 * the directory returned by a call to {@code Paths.get(dbName)}.
	 * 
	 * @param  dbName The name of the database.
	 * @param  dir The directory where the generated database files are to be
	 *         stored.
	 *         If this value is {@code null} then this method takes as directory
	 *         the value returned by {@code Paths.get(dbName).toAbsolutePath()}
	 *         where {@code dbName} denotes the name of the database.
	 * 
	 * @return The layout directory or {@code null} if the specified path is not
	 *         a directory.
	 *         
	 * @throws FileIOException If creating the directory fails.
	 */
	private final Path createLayoutDir(String dbName, Path dir) throws
																					FileIOException {
		if (dir == null) {
			dir = Paths.get(dbName);
		}
		
		if (!Files.exists(dir))
			try {
				Files.createDirectories(dir);
			} catch (IOException e) {
				throw new FileIOException(dir, e);
			}
		else {
			if (!Files.isDirectory(dir)) {
				System.out.println("ERROR: Path is not a directory: " +
																			dir.toAbsolutePath());
				dir = null;
			}
		}
		
		return dir;
	}
	
	/**
	 * Runs the database setup.
	 * <p>
	 * This method may throw an exception that is different from the listed
	 * exception.
	 * 
	 * @param  dbcn The name of the database class, for instance
	 *         "{@code com.example.MyDB}".
	 *         The database class is the class annotated with {@link
	 *         Setup_Database @Setup_Database}.
	 * @param  dir The directory where the generated database files are to be
	 *         stored.
	 *         If this value is {@code null} then this method takes as directory
	 *         the value returned by a call to {@code Paths.get(dbName)} where
	 *         {@code dbName} denotes the name of the database.
	 *        
	 * @throws FileIOException If saving the genereated database files fails.
	 */
	public final void run(String dbcn, Path dir) throws FileIOException {
		final DatabaseParams databaseParams = getDatabaseParams(dbcn);
		if (databaseParams != null) {
			final String dbName = databaseParams.name();
			// Create the layout directory.
			final Path layoutDir = createLayoutDir(dbName, dir);
			if (layoutDir != null) {
				// Create the database layout.
				final Layout layout = Database_.createLayout(databaseParams);
				// Save everything into the layout directory.
				Database_.createFiles(layout, layoutDir);
				final Path layoutFile = layoutDir.resolve("layout");
				try {
					layout.toFile(layoutFile, null, CREATE_NEW, WRITE);
				} catch (IOException e) {
					throw new FileIOException(layoutFile, e);
				}
				// We are done!
				System.out.println("INFO: Layout and backing files of database \"" +
									dbName + "\" successfully created and saved to \"" +
														layoutDir.toAbsolutePath() + "\".");
			}
		}
	}
}