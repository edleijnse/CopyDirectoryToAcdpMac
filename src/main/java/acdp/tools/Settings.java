/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.tools;

import static acdp.Information.*;

import java.io.IOException;
import java.nio.file.Path;

import acdp.Database;
import acdp.design.CustomDatabase;
import acdp.design.CustomTable;
import acdp.exceptions.IOFailureException;
import acdp.internal.Database_;
import acdp.internal.Table_;
import acdp.internal.store.wr.WRStore;
import acdp.misc.Layout;
import acdp.misc.Layout.LtEntry;
import acdp.misc.Layout.Seq;
import acdp.types.ArrayOfRefType;
import acdp.types.RefType;
import acdp.types.Type.Scheme;

/**
 * Lets you view all settings of a WR database and enables you to safely modify
 * some of them.
 * <p>
 * The settings of an ACDP WR database are stored in the <em>layout file</em>
 * which is identical to the <em>main file</em> passed as first argument to
 * the {@link Database#open} and {@link CustomDatabase#open(Path, int, boolean,
	int, CustomTable...)} methods.
 * The settings can also be obtained by a call to the {@link Database#info}
 * method, however, this requires the database to be opened and the settings
 * can't be modified that way.
 * On the other hand, the meaning of a particular setting may be better
 * explained in the method descriptions of the {@link DatabaseInfo}, {@link
 * TableInfo}, {@link WRStoreInfo} and {@link ColumnInfo} interfaces rather
 * than in the method descriptions of this class.
 * <p>
 * Editing the layout file manually or with the help of the {@link Layout}
 * class carries the risk of corrupting the layout file and can even lead to
 * data integrity being violated.
 * This class allows you to safely modify some settings without corrupting the
 * layout file and without the risk of harming data integrity.
 * The modifications take effect as soon as the database is opened.
 * <p>
 * Don't forget to invoke the {@link #save} method to save your modifications
 * to the layout file.
 * <p>
 * You may want to apply method chaining like this:
 * 
 * <pre>
 * new Settings(Paths.get("layoutFile")).setVersion("7.0").setConsistencyNumber(4).save();</pre>
 * 
 * The methods of this class assume that the layout file is such that the
 * database can be opened without throwing an exception.
 * <p>
 * Check the {@link Refactor} class in case you miss a method.
 *
 * @author Beat Hoermann
 */
public final class Settings {
	/**
	 * The database layout.
	 */
	private final Layout layout;
	
	/**
	 * Creates a database settings object based on the layout saved in the
	 * specified file.
	 * 
	 * @param  layoutFile The layout file.
	 * 
	 * @throws NullPointerException If {@code layoutFile} is {@code null}.
	 * @throws IOFailureException If the specified file does not exist or if
	 *         another I/O error occurs.
	 */
	public Settings(Path layoutFile) throws NullPointerException,
																				IOFailureException {
		try {
			layout = Layout.fromFile(layoutFile);
		} catch (IOException e) {
			throw new IOFailureException(e);
		}
	}
	
	/**
	 * Returns the name of the database.
	 * 
	 * @return The name of the database.
	 */
	public final String getName() {
		return layout.getString(Database_.lt_name);
	}
	
	/**
	 * Checks if the specified value is {@code null} or an empty string.
	 * 
	 * @param  value The value to be checked.
	 * @param  message The message of the exception.
	 * 
	 * @throws IllegalArgumentException If {@code value} is {@code null} or an
	 *         empty string.
	 */
	private final void check(String value, String message) {
		if (value == null || value.isEmpty()) {
			throw new IllegalArgumentException(message);
		}
	}
	
	/**
	 * Sets the name of the database to the specified value.
	 * 
	 * @param  value The new name of the database, not allowed to be {@code null}
	 *         and not allowed to be an empty string.
	 * 
	 * @return This object.
	 * 
	 * @throws IllegalArgumentException If {@code value} is {@code null} or an
	 *         empty string.
	 */
	public final Settings setName(String value) throws IllegalArgumentException {
		check(value, "New name of the database is null or an empty string.");
		layout.replace(Database_.lt_name, value);
		return this;
	}
	
	/**
	 * Returns the version of the database, if any is specified in the layout.
	 * 
	 * @return The version of the database or {@code null} if this database
	 *         has no version.
	 */
	public final String getVersion() {
		if (layout.contains(Database_.lt_version))
			return layout.getString(Database_.lt_version);
		else {
			return null;
		}
	}
	
	/**
	 * Sets the version of the database to the specified value.
	 * <p>
	 * If the layout already contains a version number and the specified value
	 * is {@code null} or an empty string then the version field is removed
	 * from the layout.
	 * 
	 * @param  value The version of the database.
	 * 
	 * @return This object.
	 */
	public final Settings setVersion(String value) {
		if (value == null || value.isEmpty()) {
			if (layout.contains(Database_.lt_version)) {
				layout.remove(Database_.lt_version);
			}
		}
		else {
			if (layout.contains(Database_.lt_version))
				layout.replace(Database_.lt_version, value);
			else {
				layout.add(Database_.lt_version, value);
			}
		}
		return this;
	}
	
	/**
	 * Returns the consistency number.
	 * 
	 * @return The consistency number.
	 */
	public final int getConsistencyNumber() {
		return Integer.parseInt(layout.getString(Database_.lt_consistencyNumber));
	}
	
	/**
	 * Sets the consistency number to the specified value.
	 * 
	 * @param  value The new consistency number.
	 * 
	 * @return This object.
	 */
	public final Settings setConsistencyNumber(int value) {
		layout.replace(Database_.lt_consistencyNumber, String.valueOf(value));
		return this;
	}
	
	/**
	 * Returns the class name of the cipher factory, if any is specified in the
	 * layout.
	 * 
	 * @return The class name of the cipher factory, or {@code null} if no
	 *         cipher factory class name is specified in the layout.
	 */
	public final String getCipherFactoryClassName() {
		if (layout.contains(Database_.lt_cipherFactoryClassName)) 
			return layout.getString(Database_.lt_cipherFactoryClassName);
		else {
			return null;
		}
	}
	
	/**
	 * Sets the extra classpath of the cipher factory.
	 * <p>
	 * If the database layout already contains an extra cipher factory classpath
	 * and the specified value is {@code null} or an empty string then the extra
	 * cipher factory classpath is removed from the database layout.
	 * <p>
	 * Note that this method does not check whether the specified value is a
	 * valid path and whether the path actually exists.
	 * Opening the database will throw an exception if the specified value is
	 * illegal.
	 * 
	 * @param  value The extra classpath of the cipher factory.
	 * 
	 * @return This object.
	 */
	public final Settings setCipherFactoryClasspath(String value) {
		if (value == null || value.isEmpty()) {
			if (layout.contains(Database_.lt_cipherFactoryClasspath)) {
				layout.remove(Database_.lt_cipherFactoryClasspath);
			}
		}
		else {
			if (layout.contains(Database_.lt_cipherFactoryClasspath))
				layout.replace(Database_.lt_cipherFactoryClasspath, value);
			else {
				layout.add(Database_.lt_cipherFactoryClasspath, value);
			}
		}
		return this;
	}
	
	/**
	 * Returns the cipher challenge, if any is specified in the layout.
	 * 
	 * @return The cipher challenge, or {@code null} if no cipher challenge is
	 *         specified in the layout.
	 */
	public final String getCipherChallenge() {
		if (layout.contains(Database_.lt_cipherChallenge)) 
			return layout.getString(Database_.lt_cipherChallenge);
		else {
			return null;
		}
	}
	
	/**
	 * Indicates whether "{@linkplain DatabaseInfo#forceWriteCommit() force
	 * write on commit}" is turned on or off.
	 * <p>
	 * Consult the chapter "Durability" of the {@link Database} interface
	 * description to learn more about this database property.
	 * 
	 * @return The boolean value {@code true} if and only if "force write on
	 *         commit" is turned on.
	 */
	public final boolean getForceWriteCommit() {
		final String str = layout.getString(Database_.lt_forceWriteCommit);
		return str.equals(Database_.off) ? false : true;
	}
	
	/**
	 * Sets the value of the "{@linkplain DatabaseInfo#forceWriteCommit() force
	 * write on commit}" database property.
	 * <p>
	 * Consult the chapter "Durability" of the {@link Database} interface
	 * description to learn more about this database property.
	 * 
	 * @param  value The new value of "force write on commit".
	 * 
	 * @return This object.
	 */
	public final Settings setForceWriteCommit(boolean value) {
		layout.replace(Database_.lt_forceWriteCommit, value ? Database_.on :
																					Database_.off);
		return this;
	}
	
	/**
	 * Returns the path (including the file name) of the database's recorder
	 * file.
	 * 
	 * @return The path (including the file name) of the recorder file.
	 */
	public final String getRecFile() {
		return layout.getString(Database_.lt_recFile);
	}
	
	/**
	 * Sets the path (including the file name) of the database's recorder file to
	 * the specified value.
	 * <p>
	 * Note that this method does not check whether the specified value is a
	 * valid path and whether the recorder file actually exists.
	 * Opening the database will throw an exception if the specified value is
	 * not a valid path or if the recorder file does not exist.
	 * 
	 * @param  value The new path (including the file name) of the recorder file,
	 *         not allowed to be {@code null} or an empty string.
	 * 
	 * @return This object.
	 * 
	 * @throws IllegalArgumentException If {@code value} is {@code null} or an
	 *         empty string.
	 */
	public final Settings setRecFile(String value) throws
																		IllegalArgumentException {
		check(value, "New path of recorder file is null or an empty string.");
		layout.replace(Database_.lt_recFile, value);
		return this;
	}
	
	/**
	 * Returns the names of the tables in the database.
	 * <p>
	 * The order of the returned array is identical to the order in which the
	 * tables appear in the layout.
	 * 
	 * @return The names of the database's table, never {@code null}.
	 */
	public final String[] getTableNames() {
		final LtEntry[] entries = layout.getLayout(Database_.lt_tables).entries();
		final String[] arr = new String[entries.length];
		for (int i = 0; i < entries.length; i++) {
			arr[i] = entries[i].key;
		}
		return arr;
	}
	
	/**
	 * Returns the layout of the table with the specified name.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null}
	 *         or an empty string.
	 * 
	 * @return The layout of the table with the specified name.
	 * 
	 * @throws IllegalArgumentException If {@code tableName} is {@code null} or
	 *         an empty string or if the database has no table with such a name.
	 */
	private final Layout getTableLayout(String tableName) throws
																		IllegalArgumentException {
		final Layout tables = layout.getLayout(Database_.lt_tables);
		check(tableName, "The specified table name is null or an empty string.");
		if (!tables.contains(tableName)) {
			throw new IllegalArgumentException("There is no table in the " +
										"database with the specified name: " + tableName);
		}
		return tables.getLayout(tableName);
	}
	
	/**
	 * Sets the name of the specified table to the specified new value.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null} or
	 *         an empty string.
	 * @param  value The new name of the table, not allowed to be {@code null}
	 *         or an empty string and not allowed to start with the number sign
	 *         ('#') character.
	 * 
	 * @return This object.
	 * 
	 * @throws IllegalArgumentException If one of the arguments is {@code null}
	 *         or an empty string or if the database has no table with a name
	 *         equal to the {@code tableName} argument or if the {@code value}
	 *         argument starts with the number sign ('#') character or if a
	 *         table with a name equal to the {@code value} argument already
	 *         exists.
	 */
	public final Settings setTableName(String tableName, String value) throws
																		IllegalArgumentException {
		check(value, "The specified new table name is null or an empty string");
		if (value.charAt(0) == '#') {
			throw new IllegalArgumentException("The specified new table name " +
										"starts with a number sign ('#') character: " +
										value + ".");
		}
		final Layout tables = layout.getLayout(Database_.lt_tables);
		
		// Change name in "tables" layout.
		tables.replaceKey(tableName, value);
		
		// Change name in columns referencing the table.
		for (String tn : tables.toMap().keySet()) {
			final Seq cols = tables.getLayout(tn).getSeq(Table_.lt_columns);
			for (int i = 0; i < cols.size(); i++) {
				final Layout col = cols.getLayout(i);
				if (col.contains(Table_.lt_refdTable) && col.getString(
													Table_.lt_refdTable).equals(tableName)) {
					col.replace(Table_.lt_refdTable, value);
				}
			}
		}
		
		return this;
	}
	
	/**
	 * Returns the "store" sublayout of the table with the specified name.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null} or
	 *         an empty string.
	 * 
	 * @return The "store" sublayout of the table with the specified name.
	 * 
	 * @throws IllegalArgumentException If {@code tableName} is {@code null} or
	 *         an empty string or if the database has no table with such a name.
	 */
	private final Layout getStoreLayout(String tableName) throws
																		IllegalArgumentException {
		return getTableLayout(tableName).getLayout(Table_.lt_store);
	}
	
	/**
	 * Returns the number of bytes required for referencing a row in the
	 * specified table.
	 * <p>
	 * See the description of the {@linkplain Setup Setup Tool} for further
	 * information about the {@code nobsRowRef}, {@code nobsOutrowPtr} and
	 * {@code nobsRefCount} properties.
	 * <p>
	 * This value can be changed by invoking the {@link Refactor#nobsRowRef}
	 * method.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null}
	 *         or an empty string.
	 * 
	 * @return The value of the {@code nobsRowRef} table property.
	 *
	 * @throws IllegalArgumentException If {@code tableName} is {@code null} or
	 *         an empty string or if the database has no table with such a name.
	 */
	public final int getNobsRowRef(String tableName) throws
																		IllegalArgumentException {
		return Integer.parseInt(getStoreLayout(tableName).getString(
																		WRStore.lt_nobsRowRef));
	}
	
	/**
	 * Returns the number of bytes required for referencing any {@linkplain
	 * Scheme#OUTROW outrow} data in the specified table.
	 * <p>
	 * See the description of the {@linkplain Setup Setup Tool} for further
	 * information about the {@code nobsRowRef}, {@code nobsOutrowPtr} and
	 * {@code nobsRefCount} properties.
	 * <p>
	 * This value can be changed by invoking the {@link Refactor#nobsOutrowPtr}
	 * method.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null} or
	 *         an empty string.
	 * 
	 * @return The value of the {@code nobsOutrowPtr} table property.
	 *         If the table has no such property then this value is zero.
	 *
	 * @throws IllegalArgumentException If {@code tableName} is {@code null} or
	 *         an empty string or if the database has no table with such a name.
	 */
	public final int getNobsOutrowPtr(String tableName) throws
																		IllegalArgumentException {
		final Layout l = getStoreLayout(tableName);
		if (l.contains(WRStore.lt_nobsOutrowPtr))
			return Integer.parseInt(l.getString(WRStore.lt_nobsOutrowPtr));
		else {
			return 0;
		}
	}
	
	/**
	 * Returns the number of bytes used by the reference counter in the header
	 * of a row in the specified table.
	 * <p>
	 * See the description of the {@linkplain Setup Setup Tool} for further
	 * information about the {@code nobsRowRef}, {@code nobsOutrowPtr} and
	 * {@code nobsRefCount} properties.
	 * <p>
	 * This value can be changed by invoking the {@link Refactor#nobsRefCount}
	 * method.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null} or
	 *         an empty string.
	 * 
	 * @return The value of the {@code nobsRefCount} table property.
	 *         If the table has no such property then this value is zero.
	 *
	 * @throws IllegalArgumentException If {@code tableName} is {@code null} or
	 *         an empty string or if the database has no table with such a name.
	 */
	public final int getNobsRefCount(String tableName) throws
																		IllegalArgumentException {
		final Layout l = getStoreLayout(tableName);
		if (l.contains(WRStore.lt_nobsRefCount))
			return Integer.parseInt(l.getString(WRStore.lt_nobsRefCount));
		else {
			return 0;
		}
	}
	
	/**
	 * Returns the path (including the file name) of the specified table's FL
	 * data file.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null} or
	 *         an empty string.
	 *         
	 * @return The path (including the file name) of the FL data file.
	 * 
	 * @throws IllegalArgumentException If {@code tableName} is {@code null} or
	 *         an empty string or if the database has no table with such a name.
	 */
	public final String getFlDataFile(String tableName) throws
																		IllegalArgumentException {
		return getStoreLayout(tableName).getString(WRStore.lt_flDataFile);
	}
	
	/**
	 * Sets the path (including the file name) of the specified table's FL data
	 * file to the specified value.
	 * <p>
	 * Note that this method does not check whether the specified value is a
	 * valid path and whether the FL data file actually exists.
	 * Opening the database will throw an exception if the specified value is
	 * not a valid path or if the FL data file does not exist.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null} or
	 *         an empty string.
	 * @param  value The new path (including the file name) of the table's FL
	 *         data file, not allowed to be {@code null} or an empty string.
	 * 
	 * @return This object.
	 * 
	 * @throws IllegalArgumentException If one of the arguments is {@code null}
	 *         or an empty string or if the database has no table with the
	 *         specified name.
	 */
	public final Settings setFlDataFile(String tableName, String value) throws
																		IllegalArgumentException {
		check(value, "New path of FL data file is null or an empty string.");
		getStoreLayout(tableName).replace(WRStore.lt_flDataFile, value);
		return this;
	}
	
	/**
	 * Returns the path (including the file name) of the specified table's VL
	 * data file, provided that the path is specified in the table layout.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null} or
	 *         an empty string.
	 *         
	 * @return The path (including the file name) of the VL data file or {@code
	 *         null} if no such path is specified in the table layout.
	 * 
	 * @throws IllegalArgumentException If {@code tableName} is {@code null} or
	 *         an empty string or if the database has no table with the specified
	 *         name.
	 */
	public final String getVlDataFile(String tableName) throws
																		IllegalArgumentException {
		final Layout store = getStoreLayout(tableName);
		if (store.contains(WRStore.lt_vlDataFile))
			return store.getString(WRStore.lt_vlDataFile);
		else {
			return null;
		}
	}
	
	/**
	 * Sets the path (including the file name) of the specified table's VL data
	 * file to the specified value, provided that the path is specified in
	 * the table layout.
	 * <p>
	 * Note that this method does not check whether the specified value is a
	 * valid path and whether the VL data file actually exists.
	 * Opening the database will throw an exception if the specified value is
	 * not a valid path or if the VL data file does not exist.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null} or
	 *         an empty string.
	 * @param  value The new path (including the file name) of the table's VL
	 *         data file, not allowed to be {@code null} or an empty string.
	 * 
	 * @return This object.
	 * 
	 * @throws IllegalArgumentException If one of the arguments is {@code null}
	 *         or an empty string or if the database has no table with the
	 *         specified name or if the table layout does not contain such a
	 *         path.
	 */
	public final Settings setVlDataFile(String tableName, String value) throws
																		IllegalArgumentException {
		check(value, "New path of VL data file is null or an empty string.");
		final Layout store = getStoreLayout(tableName);
		if (store.contains(WRStore.lt_vlDataFile))
			store.replace(WRStore.lt_vlDataFile, value);
		else {
			throw new IllegalArgumentException("The layout of the \"" + tableName +
								"\" table does not contain a path to a VL data file.");
		}
		return this;
	}
	
	/**
	 * Returns the "columns" sequence of the table with the specified name.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null} or
	 *         an empty string.
	 * 
	 * @return The "columns" sequence of the table with the specified name.
	 * 
	 * @throws IllegalArgumentException If {@code tableName} is {@code null} or
	 *         an empty string or if the database has no table with such a name.
	 */
	private final Seq getColsSeq(String tableName) throws
																		IllegalArgumentException {
		return getTableLayout(tableName).getSeq(Table_.lt_columns);
	}
	
	/**
	 * Returns the names of the columns of the specified table.
	 * <p>
	 * The order of the returned array is identical to the order in which the
	 * columns appear in the table layout.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null} or
	 *         an empty string.
	 * 
	 * @return The names of the table's columns, never {@code null}.
	 * 
	 * @throws IllegalArgumentException If {@code tableName} is {@code null} or
	 *         an empty string or if the database has no table with such a name.
	 */
	public final String[] getColNames(String tableName) throws
																		IllegalArgumentException {
		final Seq cols = getTableLayout(tableName).getSeq(Table_.lt_columns);
		final String[] arr = new String[cols.size()];
		for (int i = 0; i < cols.size(); i++) {
			arr[i] = cols.getLayout(i).getString(Table_.lt_name);
		}
		return arr;
	}
	
	/**
	 * Returns the layout of the column with the specified name.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null} or
	 *         an empty string.
	 * @param  columnName The name of the column, not allowed to be {@code null}
	 *         or an empty string.
	 * 
	 * @return The column layout.
	 * 
	 * @throws IllegalArgumentException If one of the arguments is {@code null}
	 *         or an empty string or if the database has no table with the
	 *         specified name or if the table has no column with the specified
	 *         name.
	 */
	private final Layout getColLayout(String tableName, String columnName) throws
																		IllegalArgumentException {
		check(columnName,"The specified column name is null or an empty string.");
		Layout colLt = null;
		
		final Seq cols = getColsSeq(tableName);
		int i = 0;
		while (i < cols.size() && colLt == null) {
			final Layout col = cols.getLayout(i++);
			if (col.getString(Table_.lt_name).equals(columnName)) {
				colLt = col;
			}
		}
		
		if (colLt == null) {
			throw new IllegalArgumentException("The table \"" + tableName +
											"\" has no column with the specified name: " +
																				columnName  + ".");
		}
		
		return colLt;
	}
	
	/**
	 * Sets the name of the specified column to the specified new value.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null}
	 *         or an empty string.
	 * @param  columnName The name of the column, not allowed to be {@code null}
	 *         or an empty string.
	 * @param  value The new name of the column, not allowed to be {@code null}
	 *         or an empty string.
	 *
	 * @return This object.
	 * 
	 * @throws IllegalArgumentException If one of the arguments is {@code null}
	 *         or an empty string or if the database has no table with the
	 *         specified name or if the table has no column with the specified
	 *         name or if the table already has a column with a name equal to
	 *         the {@code value} argument.
	 */
	public final Settings setColName(String tableName, String columnName,
											String value) throws IllegalArgumentException {
		check(value, "The specified new column name is null or an empty string.");
		if (!columnName.equals(value)) {
			final Seq cols = getColsSeq(tableName);
			boolean found = false;
			int i = 0;
			while (i < cols.size() && !found) {
				if (cols.getLayout(i++).getString(Table_.lt_name).equals(value)) {
					found = true;
				}
			}
			if (found) {
				throw new IllegalArgumentException("Table \"" + tableName +
												"\" already has a column with this name: " +
																						value + ".");
			}
			getColLayout(tableName, columnName).replace(Table_.lt_name, value);
		}
		
		return this;
	}
	
	/**
	 * Returns the type descriptor of the specified column.
	 * <p>
	 * Invoke the {@link Refactor#modifyColumn} method to change the type of a
	 * column.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null}
	 *         or an empty string.
	 * @param  columnName The name of the column, not allowed to be {@code null}
	 *         or an empty string.
	 * 
	 * @return The column's type descriptor.
	 * 
	 * @throws IllegalArgumentException If one of the arguments is {@code null}
	 *         or an empty string or if the database has no table with the
	 *         specified name or if the table has no column with the specified
	 *         name.
	 */
	public final String getTypeDesc(String tableName, String columnName) throws
																	IllegalArgumentException {
		return getColLayout(tableName, columnName).getString(Table_.lt_typeDesc);
	}
	
	/**
	 * Returns the type factory class name of the specified column, if any is
	 * specified in the column layout. 
	 * <p>
	 * Invoke the {@link Refactor#modifyColumn} method to change the type of a
	 * column.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null}
	 *         or an empty string.
	 * @param  columnName The name of the column, not allowed to be {@code null}
	 *         or an empty string.
	 * 
	 * @return The column's type factory class name or {@code null} if no type
	 *         factory class name is specified in the column layout.
	 * 
	 * @throws IllegalArgumentException If one of the arguments is {@code null}
	 *         or an empty string or if the database has no table with the
	 *         specified name or if the table has no column with the specified
	 *         name.
	 */
	public final String getTypeFactoryClassName(String tableName,
									String columnName) throws IllegalArgumentException {
		final Layout col = getColLayout(tableName, columnName);
		if (col.contains(Table_.lt_typeFactoryClassName))
			return getColLayout(tableName, columnName).getString(
																Table_.lt_typeFactoryClassName);
		else {
			return null;
		}
	}
	
	/**
	 * Returns the extra classpath of the specified column's type factory,
	 * if any is specified in the column layout. 
	 * <p>
	 * Invoke the {@link Refactor#modifyColumn} method to change the type of a
	 * column.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null}
	 *         or an empty string.
	 * @param  columnName The name of the column, not allowed to be {@code null}
	 *         or an empty string.
	 * 
	 * @return The extra classpath of the column's type factory or {@code null}
	 *         if no extra type factory classpath is specified in the column
	 *         layout.
	 * 
	 * @throws IllegalArgumentException If one of the arguments is {@code null}
	 *         or an empty string or if the database has no table with the
	 *         specified name or if the table has no column with the specified
	 *         name.
	 */
	public final String getTypeFactoryClasspath(String tableName,
									String columnName) throws IllegalArgumentException {
		final Layout col = getColLayout(tableName, columnName);
		if (col.contains(Table_.lt_typeFactoryClasspath))
			return getColLayout(tableName, columnName).getString(
																Table_.lt_typeFactoryClasspath);
		else {
			return null;
		}
	}
	
	/**
	 * Sets the extra classpath of the specified column's type factory to the
	 * specified value.
	 * <p>
	 * If the column layout already contains an extra type factory classpath and
	 * the specified value is {@code null} or an empty string then the extra
	 * type factory classpath is removed from the column layout.
	 * <p>
	 * Note that this method does not check whether the specified value is a
	 * valid path and whether the path actually exists.
	 * Opening the database will throw an exception if the specified value is
	 * illegal.
	 * <p>
	 * Invoke the {@link Refactor#modifyColumn} method to change the type of a
	 * column.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null}
	 *         or an empty string.
	 * @param  columnName The name of the column, not allowed to be {@code null}
	 *         or an empty string.
	 * @param  value The extra classpath of the column's type factory.
	 * 
	 * @return This object.
	 * 
	 * @throws IllegalArgumentException If {@code tableName} or {@code
	 *         columnName} is {@code null} or an empty string or if the database
	 *         has no table with the specified name or if the table has no column
	 *         with the specified name.
	 */
	public final Settings setTypeFactoryClasspath(String tableName,
															String columnName, String value)
															throws IllegalArgumentException {
		final Layout col = getColLayout(tableName, columnName);
		if (value == null || value.isEmpty()) {
			if (col.contains(Table_.lt_typeFactoryClasspath)) {
				col.remove(Table_.lt_typeFactoryClasspath);
			}
		}
		else {
			if (col.contains(Table_.lt_typeFactoryClasspath))
				col.replace(Table_.lt_typeFactoryClasspath, value);
			else {
				col.add(Table_.lt_typeFactoryClasspath, value);
			}
		}
		return this;
	}
	
	/**
	 * Returns the name of the table referenced by the specified column, provided
	 * that the column is of {@link RefType} or {@link ArrayOfRefType}.
	 * 
	 * @param  tableName The name of the table, not allowed to be {@code null}
	 *         or an empty string.
	 * @param  columnName The name of the column, not allowed to be {@code null}
	 *         or an empty string.
	 * 
	 * @return The name of the referenced table or {@code null} if the column is
	 *         neither of {@link RefType} nor {@link ArrayOfRefType}.
	 * 
	 * @throws IllegalArgumentException If one of the arguments is {@code null}
	 *         or an empty string or if the database has no table with the
	 *         specified name or if the table has no column with the specified
	 *         name.
	 */
	public final String getRefdTable(String tableName, String columnName) throws
																		IllegalArgumentException {
		final Layout col = getColLayout(tableName, columnName);
		if (col.contains(Table_.lt_refdTable))
			return getColLayout(tableName, columnName).getString(
																			Table_.lt_refdTable);
		else {
			return null;
		}
	}
	
	/**
	 * Saves the settings.
	 * 
	 * @return This object.
	 * 
	 * @throws IOFailureException If an I/O error occurs.
	 */
	public final Settings save() throws IOFailureException {
		try {
			layout.save();
		} catch (IOException e) {
			throw new IOFailureException(e);
		}
		return this;
	}
}