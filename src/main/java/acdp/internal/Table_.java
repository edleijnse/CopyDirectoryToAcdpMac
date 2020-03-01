/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.ZipOutputStream;

import acdp.Column;
import acdp.ColVal;
import acdp.Database;
import acdp.ReadZone;
import acdp.Ref;
import acdp.Row;
import acdp.Table;
import acdp.Information.ColumnInfo;
import acdp.Information.StoreInfo;
import acdp.Information.TableInfo;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CreationException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.DeleteConstraintException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.IllegalReferenceException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.MaximumException;
import acdp.exceptions.MissingEntryException;
import acdp.exceptions.ShutdownException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.misc.ZipEntryCollector;
import acdp.internal.store.Store;
import acdp.internal.store.ro.ROStore;
import acdp.internal.store.wr.WRStore;
import acdp.internal.store.wr.WRStore.GlobalBuffer;
import acdp.internal.types.ArrayOfRefType_;
import acdp.internal.types.RefType_;
import acdp.internal.types.TypeFactory;
import acdp.internal.types.Type_;
import acdp.misc.Layout;
import acdp.misc.Layout.Seq;

/**
 * Implements a {@linkplain Table table}.
 *
 * @author Beat Hoermann
 */
public final class Table_ implements Table {
	// Keys of layout entries.
	public static final String lt_columns = "columns";
	public static final String lt_name = "name";
	public static final String lt_typeDesc = "typeDesc";
	public static final String lt_typeFactoryClassName = "typeFactoryClassName";
	public static final String lt_typeFactoryClasspath = "typeFactoryClasspath";
	public static final String lt_refdTable = "refdTable";
	public static final String lt_store = "store";
	
	/**
	 * Just implements the {@code TableInfo} interface.
	 * 
	 * @author Beat Hoermann
	 */
	public static final class Info implements TableInfo {
		private final Table_ table;
		
		/**
		 * Creates the information object of the specified table.
		 * 
		 * @param  table The table, not allowed to be {@code null}.
		 * 
		 * @throws NullPointerException If {@code table} is {@code null}.
		 */
		Info(Table_ table) throws NullPointerException {
			this.table = Objects.requireNonNull(table);
		}
		
		@Override
		public final String name() {
			return table.name;
		}
		
		@Override
		public final ColumnInfo[] columnInfos() {
			final Column_<?>[] cols = table.tableDef;
			final Column_.Info[] colInfoArr = new Column_.Info[cols.length];
			for (int i = 0; i < cols.length; i++) {
				colInfoArr[i] = new Column_.Info(cols[i]);
			}
			return colInfoArr;
		}
		
		@Override
		public final StoreInfo storeInfo() {
			return table.store instanceof WRStore ?
													new WRStore.Info((WRStore) table.store) :
													new ROStore.Info((ROStore) table.store);
		}
	}
	
	/**
	 * This class keeps the name of a column and, provided that the column
	 * references a table, the name of the referenced table.
	 * This information is needed to create a layout for the column.
	 * 
	 * @author Beat Hoermann
	 */
	static final class ColumnParams {
		private final String name;
		private final String refdTable;
		
		/**
		 * The constructor
		 * 
		 * @param  name The name of a column, not allowed to be {@code null} and
		 *         not allowed to be an empty string.
		 * @param  refdTable The name of the referenced table.
		 *         The value is not allowed to be {@code null} and not allowed to
		 *         be an empty string if and only if this column references a
		 *         table.
		 *        
		 * @throws IllegalArgumentException If {@code name} is {@code null} or an
	    *         empty string.
		 */
		ColumnParams(String name, String refdTable) throws
																		IllegalArgumentException {
			if (name == null || name.isEmpty()) {
				throw new IllegalArgumentException("The name of the column is " +
																	"null or an empty string.");
			}
			this.name = name;
			this.refdTable = refdTable;
		}
		
		/**
		 * Returns the name of the column.
		 * 
		 * @return The name of the column, never {@code null} and never an empty
		 *         string.
		 */
		final String name() {
			return name;
		}
		
		/**
		 * Returns the name of the referenced table, if any.
		 * 
		 * @return The name of the referenced table, may be {@code null} and may
		 *         be an empty string.
		 */
		final String refdTable() {
			return refdTable;
		}
	}
	
	/**
	 * Keeps together some parameters needed to create a table layout and checks
	 * if the given values are plausible.
	 * 
	 * @author Beat Hoermann
	 */
	static final class TableParams {
		private final String name;
		private final Column_<?>[] tableDef;
		private final List<ColumnParams> listOfColumnParams;

		/**
		 * The constructor.
		 * 
		 * @param  name The name of the table, not allowed to be {@code null} and
		 *         not allowed to be an empty string and not allowed to start
		 *         with the number sign ('#') character.
	    * @param  tableDef The table definition of the table, not allowed to be
	    *         {@code null}.
	    * @param  listOfColumnParams The parameters of the columns, not allowed
	    *         to be {@code null} and not allowed to be empty.
		 * 
	    * @throws IllegalArgumentException If {@code name} is {@code null} or an
	    *         empty string or starts with the number sign ('#') character or
	    *         if {@code tableDef} is {@code null} or {@code
	    *         listOfColumnParams} is {@code null} or empty or if the table
	    *         definition and the list of column parameters do not correspond
	    *         to each other.
		 */
		TableParams(String name, Column_<?>[] tableDef,
											List<ColumnParams> listOfColumnParams) throws
																		IllegalArgumentException {
			if (name == null || name.isEmpty() || name.charAt(0) == '#')
				throw new IllegalArgumentException("The name of the table is " +
			                                 "null or an empty string or starts " +
														"with the number sign ('#').");
			else if (tableDef == null)
				throw new IllegalArgumentException(ACDPException.prefix(name) +
															"The table definition is null.");
			else if (listOfColumnParams == null || listOfColumnParams.size() == 0)
				throw new IllegalArgumentException(ACDPException.prefix(name) +
									"The list of column parameters is null or empty.");
			else {
				// Check correspondence between the table definition and the list
				// of column parameters.
				if (tableDef.length != listOfColumnParams.size()) {
					throw new IllegalArgumentException(ACDPException.prefix(name) +
									"The length of the table definition is not equal " +
									"to the length of the list of column parameters.");
				}
				for (int i = 0; i < tableDef.length; i++) {
					final Type_ type = tableDef[i].type();
					final ColumnParams columnParams = listOfColumnParams.get(i);
					final String refdTable = columnParams.refdTable();
					if (type instanceof RefType_ || type instanceof ArrayOfRefType_){
						// The column references a table.
						if (refdTable == null || refdTable.isEmpty()) {
							throw new IllegalArgumentException(ACDPException.prefix(
									name) + "The type of column \"" + columnParams.
									name() + "\" allows for values referencing the " +
									"rows of a table but the parameters of this " +
									"column don't specify such a table.");
						}
					}
					else {
						// The column does not reference a table.
						if (refdTable != null && !refdTable.isEmpty()) {
							throw new IllegalArgumentException(ACDPException.prefix(
									name) + "The type of column \"" + columnParams.
									name() + "\" does not allow for values " +
									"referencing the rows of a table but the " +
									"parameters of this column specify such a " +
									"table: \"" + refdTable + "\".");
						}
					}
				}
			}
			
			this.name = name;
			this.tableDef = tableDef;
			this.listOfColumnParams = listOfColumnParams;
		}
		
		/**
		 * Returns the name of the table.
		 * 
		 * @return The name of the table, never {@code null} and never an empty
		 *         string.
		 */
		final String name() {
			return name;
		}
		
		/**
		 * Returns the table definition.
		 * 
		 * @return The table definitino, never {@code null}.
		 */
		final Column_<?>[] tableDef() {
			return tableDef;
		}
		
		/**
		 * Returns the parameters of the columns.
		 * 
		 * @return The list of column parameters, never {@code null} and never
		 *         empty.
		 */
		final List<ColumnParams> listOfColumnParams() {
			return listOfColumnParams;
		}
	}
	
	/**
	 * Fills the specified column layout with values derived from the specified
	 * arguments.
	 * 
	 * @param col The column, not allowed to be {@code null}.
	 * @param colName The name of the column, not allowed to be {@code null} and
	 *        not allowed to be empty.
	 * @param refdTable The name of the table referenced by the column or {@code
	 *        null} if the column does not reference a table.
	 * @param colLayout The (empty) column layout, not allowed to be {@code
	 *        null}.
	 */
	private static final void fillColLayout(Column_<?> col, String colName,
													String refdTable, Layout colLayout) {
		colLayout.add(lt_name, colName);
		final Type_ type = col.type();
		final String typeDesc = type.typeDesc();
		colLayout.add(lt_typeDesc, typeDesc);
		if (!Type_.isBuiltInType(typeDesc)) {
			colLayout.add(lt_typeFactoryClassName, type.getClass().getName());
		}
		if (refdTable != null) {
			colLayout.add(lt_refdTable, refdTable);
		}
	}
	
	/**
	 * Creates and returns a new table layout.
	 * <p>
	 * This method does not create the backing files of the table, giving you a
	 * chance for modifying the paths to the backing files before the files are
	 * created.
	 * Calling the {@link #createFiles} method eventually creates the backing
	 * files.
	 * 
	 * @param  tableParams Some parameters needed to create the table layout,
	 *         not allowed to be {@code null}.
	 * @param  referenced If {@code true} then the table is referenced by itself
	 *         or by another table within the database.
	 *         
	 * @return The created table layout.
	 * 
	 * @throws NullPointerException If {@code tableParams} is {@code null}.
	 */
	static final Layout createLayout(TableParams tableParams,
										boolean referenced) throws NullPointerException {
		final Layout layout = new Layout();
		
		// Add layout entries for the table.
		final Seq columnLayouts = layout.addSeq(lt_columns);
		
		final Column_<?>[] tableDef = tableParams.tableDef();
		
		// Create and add layout entries for each column of the table.
		int i = 0;
		for (ColumnParams columnParams : tableParams.listOfColumnParams()) {
			fillColLayout(tableDef[i++], columnParams.name(),
									columnParams.refdTable(), columnLayouts.addLayout());
		}
		
		// Create and add layout entries for the table's store.
		layout.add(lt_store, WRStore.createLayout(referenced, tableDef,
																			tableParams.name()));
		return layout;
	}
	
	/**
	 * Creates the backing files of the table, failing if at least one of the
	 * files already exists.
	 * The created files are empty.
	 * 
	 * @param  layout The table's layout, not allowed to be {@code null}.
	 * @param  layoutDir The directory of the table's layout, not allowed to be
	 *         {@code null}.
	 *         The layout's directory is used to convert relative file paths
	 *         contained in the layout to absolute file paths.
	 *         
	 * @throws NullPointerException If one of the parameters is {@code null}.
	 * @throws MissingEntryException If a required entry in the layout is
	 *         missing.
	 * @throws IllegalArgumentException If the path string of a file in the
	 *         store layout is an empty string.
	 * @throws InvalidPathException If the path string of a file in the layout
	 *         is invalid.
	 * @throws IOFailureException If a file already exists or if another I/O
	 *         error occurs.
	 */
	static final void createFiles(Layout layout, Path layoutDir) throws
				NullPointerException, MissingEntryException,
				IllegalArgumentException, InvalidPathException, IOFailureException {
		WRStore.createFiles(layout.getLayout(lt_store), layoutDir);
	}
	
	/**
	 * Checks the specified table layouts.
	 * 
	 * @param db The database.
	 * @param tableName The name of the table.
	 * @param layout The table layout.
	 * 
	 * @throws CreationException If the specified table layout is invalid.
	 */
	static final void checkLayout(Database_ db, String tableName,
													Layout layout) throws CreationException {
		try {
			if (layout.getSeq(lt_columns).size() == 0)
				throw new IllegalArgumentException(ACDPException.prefix(db,
											tableName) + "Column sublayouts are missing.");
			else if (layout.getLayout(lt_store).size() == 0) {
				throw new IllegalArgumentException(ACDPException.prefix(db,
										tableName) + "Layout of table store is missing.");
			}
			else {
				// Check column layouts
				Seq colLayouts = layout.getSeq(lt_columns);
				final Set<String> colNames = new HashSet<>(colLayouts.size() *
																							4 / 3 + 1);
				for (Iterator<Object> it = colLayouts.asList().iterator();
																					it.hasNext(); ) {
					final Layout colLayout = (Layout) it.next();
					final String name = colLayout.getString(lt_name);
					if (name.isEmpty())
						throw new IllegalArgumentException("Column name is an " +
																					"empty string.");
					else if (colNames.contains(name))
						throw new IllegalArgumentException("Another column of this " +
											"table has the same name: " + name + ".");
					else if (colLayout.getString(lt_typeDesc).isEmpty())
						throw new IllegalArgumentException("Type descriptor of " +
											"column \"" + name + "\" is an empty string.");
					else if (colLayout.contains(lt_typeFactoryClassName) &&
								colLayout.getString(lt_typeFactoryClassName).isEmpty())
						throw new IllegalArgumentException("Type factory class " +
											"name of column \"" + name + "\" is an " +
																					"empty string.");
					else if (colLayout.contains(lt_typeFactoryClasspath)) {
						final String tcp = colLayout.getString(
																		lt_typeFactoryClasspath);
						if (tcp.isEmpty())
							throw new IllegalArgumentException("Type factory " +
											"classpath of column \"" + name + "\" is an " +
																					"empty string.");
						else if (!layout.contains(lt_typeFactoryClassName))
							throw new IllegalArgumentException("A type classpath " + 
											"for column \"" + name + "\" is specified " +
									      "but the name of the type factory class is " +
											"missing: " + tcp + ".");
						else if (!Files.isDirectory(Paths.get(tcp))) {
							throw new IllegalArgumentException("Type classpath of " +
											"column \"" + name + "\" is not a " +
											"directory: " + tcp + ".");
						}
					}
					else if (colLayout.contains(lt_refdTable) &&
											colLayout.getString(lt_refdTable).isEmpty()) {
						throw new IllegalArgumentException("Referenced table of " +
											"column \"" + name + "\" is an empty string.");
					}
					else {
						final boolean isBuiltInType = Type_.isBuiltInType(colLayout.
																		getString(lt_typeDesc));
						if (!isBuiltInType && !colLayout.contains(
																		lt_typeFactoryClassName))
							// Column type is a custom type but the name of the class
							// defining the type factory method is missing.
							throw new IllegalArgumentException("Type of column \"" +
													name + "\" is a custom column type " +
													"but the name of the class defining " +
													"the type factory method is missing.");
						else if (isBuiltInType && colLayout.contains(
																		lt_typeFactoryClassName))
							// Column type is a built-in type and the
							// "typeFactoryClassName" entry exists.
							throw new IllegalArgumentException("Type of column \"" +
													name + "\" is a built-in type. No \"" +
													lt_typeFactoryClassName +
													"\" entry required. Remove entry.");
						else if (isBuiltInType && colLayout.contains(
																	lt_typeFactoryClasspath)) {
							// Column type is a built-in type and the
							// "typeFactoryClasspath" entry exists.
							throw new IllegalArgumentException("Type of column \"" +
													name + "\" is a built-in type. No \"" +
													lt_typeFactoryClasspath +
													"\" entry required. Remove entry.");
						}
					}
					colNames.add(name);
				}
			}
		} catch (Exception e) {
			throw new CreationException(db, tableName, "Invalid table layout.", e);
		}
	}
	
	/**
	 * Given the layout of a particular table this method finds all the tables
	 * that this table references.
	 * <p>
	 * Assumes that the specified table layout is {@linkplain #checkLayout
	 * valid}.
	 * 
	 * @param  layout The table layout, not allowed to be {@code null}.
	 * 
	 * @return The set of names of the tables that are referenced by the table
	 *         with the given layout, never {@code null} but may be empty.
	 */
	static final Set<String> referencedTables(Layout layout) {
		Set<String> referencedTables = new HashSet<>();
		Seq columnLayouts = layout.getSeq(lt_columns);
		for (Object obj : columnLayouts.asList()) {
			final Layout columnLayout = (Layout) obj;
			if (columnLayout.contains(lt_refdTable)) {
				referencedTables.add(columnLayout.getString(lt_refdTable));
			}
		}
		return referencedTables;
	}
	
	/**
	 * The table definition, never {@code null} and never empty.
	 */
	private final Column_<?>[] tableDef;
	
	/**
	 * Constructs a table from the specified columns.
	 * 
	 * @param  cols The array of columns of the table, not allowed to be
	 *         {@code null} and not allowed to be empty.
	 *         The <em>table definition</em> is set equal to a copy of this
	 *         array.
	 * 
	 * @throws IllegalArgumentException If {@code cols} is {@code null} or empty
	 *         or if it contains a {@code null} value.
	 */
	public Table_(Column_<?>[] cols) throws IllegalArgumentException {
		if (cols == null || cols.length == 0)
			throw new IllegalArgumentException("The table has no columns.");
		else {
			for (Column_<?> col : cols) {
				if (col == null) {
					throw new IllegalArgumentException("At least one of the " +
															"columns of the table is null.");
				}
			}
		}
		this.tableDef = cols;
	}
	
	/**
	 * Builds the array of columns from the specified sequence of column layouts.
	 * 
	 * @param  colLayouts The sequence of column layouts.
	 * @param  layoutDir The directory of the table layout, not allowed to be
	 *         {@code null}.
	 *         The layout's directory is used to convert relative file paths
	 *         contained in the layout to absolute file paths.
	 * @param  tableName The name of the table.
	 * @param  db The database.
	 * 
	 * @return The columns of the table, never {@code null}.
	 *         None of the columns is {@code null}.
	 * 
	 * @throws CreationException If creating the type for at least one column
	 *         fails due to any reason, including an invalid type descriptor or
	 *         an error while creating a custom column type.
	 */
	private final Column_<?>[] buildColumns(Seq colLayouts, Path layoutDir,
							String tableName, Database_ db) throws CreationException {
		final int n = colLayouts.size();
		Column_<?>[] cols = new Column_<?>[n];
		for (int i = 0; i < n; i++) {
			final Layout colLayout = colLayouts.getLayout(i);
			final String typeDesc = colLayout.getString(lt_typeDesc);
			
			final Type_ type;
			try {
				type = TypeFactory.fetchType(typeDesc,
						colLayout.contains(lt_typeFactoryClassName) ?
							colLayout.getString(lt_typeFactoryClassName) : null,
						colLayout.contains(lt_typeFactoryClasspath) ?
							colLayout.getString(lt_typeFactoryClasspath) : null,
																							layoutDir);
			} catch (CreationException e) {
				throw new CreationException(db, tableName, "Creating type for " +
							"column \"" + colLayout.getString(lt_name) +
							"\" failed. Type descriptor: \"" + typeDesc + "\".", e);
			}
			// type != null
			cols[i] = new Column_<>(type);
		}
		
		return cols;
	}
	
	/**
	 * Constructs a table from the specified table layout.
	 * <p>
	 * Assumes that the {@link #checkLayout} method was called before.
	 * 
	 * @param  name The name of the table, not allowed to be {@code null} and
	 *         not allowed to be an empty string.
	 * @param  layout The table's layout, not allowed to be {@code null}.
	 * @param  layoutDir The directory of the table's layout, not allowed to be
	 *         {@code null}.
	 *         The layout's directory is used to convert relative file paths
	 *         contained in the layout to absolute file paths.
	 * @param  db The database this table belongs to, not allowed to be {@code
	 *         null}.
	 * 
	 * @throws CreationException If creating the type for at least one column
	 *         fails due to any reason, including an invalid type descriptor or
	 *         an error while creating a custom column type.
	 */
	Table_(String name, Layout layout, Path layoutDir, Database_ db) throws
																				CreationException {
		// name != null && !name.isEmpty() && name does not start with the number
		// sign character && db != null && layoutDir != null as per assumption.
		// Furthermore, it is assumed that the checkLayout-method for the
		// specified layout has been called before this method.

		// Build the array of columns from the column sublayouts of the table
		// layout.
		this.tableDef = buildColumns(layout.getSeq(lt_columns), layoutDir, name,
																									db);
	}

	// The following properties can be considered final and non-null after
	// the init-method has been successfully called.
	/**
	 * The table's layout, never {@code null}.
	 */
	private Layout layout;
	
	/**
	 * The database, never {@code null}.
	 */
	private Database_ db;
	
	/**
	 * The name of the table, never {@code null} and never an empty string.
	 */
	private String name;
	
	/**
	 * The store of the table, never {@code null}.
	 */
	private Store store;
	
	/**
	 * Checks if the columns of the table definition are in accordance with the
	 * columns as defined in the specified sequence of column sublayouts.
	 * 
	 * @param  colLayouts The column sublayouts of the table layout.
	 * 
	 * @throws CreationException If at least one column of the table definition
	 *         is not in accordance with its corresponding column as defined in
	 *         the sequence of column sublayouts.
	 */
	private final void checkColumns(Seq colLayouts) throws CreationException {
		// Check if the number of columns in the table definition is equal to the
		// number of column sublayouts.
		// This is always the case for a table constructed with the
		// newInstance-method.
		if (tableDef.length != colLayouts.size()) {
			throw new CreationException(this, "The number of columns in " +
											"the table definition is not equal to " +
											"the number of column sublayouts.");
		}
		// Check the correspondence of the columns and the columns referencing
		// tables.
		for (int i = 0; i < tableDef.length; i++) {
			final Type_ type = tableDef[i].type();
			final Layout colLayout = colLayouts.getLayout(i);
			final String colName = colLayout.getString(lt_name);
				
			if (!type.typeDesc().equals(colLayout.getString(lt_typeDesc)))
				// This is never the case for a table constructed with the
				// newInstance-method.
				throw new CreationException(this, colName, "Type descriptor " +
											"mismatch: \"" + type.typeDesc() + "\", \"" +
											colLayout.getString(lt_typeDesc) + "\".");
			else if (type instanceof RefType_ || type instanceof ArrayOfRefType_) {
				// The column references a table.
				if (!colLayout.contains(lt_refdTable)) {
					throw new CreationException(this, colName, "The type of the " +
											"column allows for values referencing the " +
											"rows of a table but the column sublayout " + 
											"misses the name of the referenced table.");
				}
			}
			else {
				// The column does not reference a table.
				if (colLayout.contains(lt_refdTable)) {
					throw new CreationException(this, colName, 
									"The type of the column does not allow for values " +
									"referencing the rows of a table but the column " +
									"sublayout specifies such a table: \"" + colLayout.
															getString(lt_refdTable) + "\".");
				}
			}
		}
	}
	
	/**
	 * Gives each column its name and, provided that the column references a
	 * table, its referenced table from the column sublayout.
	 * Furthermore, this method sets the column's table and its table definition
	 * index.
	 * 
	 * @param  colLayouts The column sublayouts of the table layout.
	 */
	private final void initColumns(Seq colLayouts) {
		int i = 0;
		for (Column_<?> col : tableDef) {
			final Layout colLayout = colLayouts.getLayout(i);
			col.initialize(colLayout.getString(lt_name), colLayout.contains(
							lt_refdTable) ? colLayout.getString(lt_refdTable) : null,
																							this, i++);
		}
	}
	
	/**
	 * Does some basic initialization.
	 * Does not create the store.
	 * 
	 * @param  layout The table's layout, not allowed to be {@code null}.
	 * @param  db The database this table belongs to, not allowed to be {@code
	 *         null}.
	 * @param  name The name of the table, not allowed to be {@code null} and
	 *         not allowed to be an empty string.
	 *         
	 * @throws CreationException If at least one column of the table definition
	 *         is not in accordance with its corresponding column as defined in
	 *         the sequence of column sublayouts.
	 */
	private final void init(Layout layout, Database_ db, String name) throws
																				CreationException {
		// name != null && !name.isEmpty() && name does not start with the number
		// sign character && db != null as per assumption.
		// Furthermore, it is assumed that the checkTableLayout-method for the
		// specified layout has been called before.
			
		// Initialize some simple fields.
		this.layout = layout;
		this.db = db;
		this.name = name;
			
		final Seq colLayouts = layout.getSeq(lt_columns);
		checkColumns(colLayouts);
		initColumns(colLayouts);
	}
	
	/**
	 * Initializes this table and connects it with a WR store.
	 * <p>
	 * Don't forget to invoke the {@link #initWRStore} method after all tables
	 * of the database have been initialized.
	 * <p>
	 * Assumes that the {@link #checkLayout} method was called before with the
	 * specified layout.
	 * 
	 * @param  db The database this table belongs to, not allowed to be {@code
	 *         null}.
	 * @param  name The name of the table, not allowed to be {@code null} and
	 *         not allowed to be an empty string.
	 * @param  layout The table's layout, not allowed to be {@code null}.
	 * @param  layoutDir The directory of the table's layout, not allowed to be
	 *         {@code null}.
	 *         The layout's directory is used to convert relative file paths
	 *         contained in the layout to absolute file paths.
	 * @param  referenced If {@code true} then the table to create is referenced
	 *         by itself or by another table within the database.
	 * @param  globalBuffer The global buffer, not allowed to be {@code null}.
	 *         
	 * @throws InvalidPathException If the path string of a data file in the
	 *         store sublayout is invalid.
	 * @throws CreationException If at least one column of the table definition
	 *         is not in accordance with its corresponding column as defined in
	 *         the sequence of column sublayouts or if the table's store can't
	 *         be created due to any reason including problems with the store
	 *         sublayout and the backing files of the store.
	 */
	final void initWRTable(Database_ db, String name, Layout layout,
			Path layoutDir, boolean referenced, GlobalBuffer globalBuffer) throws
													InvalidPathException, CreationException {
		// name != null && !name.isEmpty() && name does not start with the number
		// sign character && db != null && layoutDir != null as per assumption.
		// Furthermore, it is assumed that the checkTableLayout-method for the
		// specified layout has been called before.
		
		// Do some basic initialization.
		init(layout, db, name);
		
		// Create store.
		this.store = new WRStore(layout.getLayout(lt_store), layoutDir,
																referenced, globalBuffer, this);
	}
	
	/**
	 * Initializes the WR store of the table.
	 * <p>
	 * Call this method only after the database is completely initialized.
	 * 
	 * @throws ImplementationRestrictionException If the table has too many
	 *         columns needing a separate null information or if the size of
	 *         the FL data block exceeds {@link Integer#MAX_VALUE}.
	 * @throws CreationException If initializing the WR store fails.
	 */
	final void initWRStore() throws ImplementationRestrictionException,
																				CreationException {
		((WRStore) store).initialize();
	}
	
	/**
	 * Initializes this table and connects it with an RO store.
	 * <p>
	 * Don't forget to invoke the {@link #initROStore} method after all tables
	 * of the database have been initialized.
	 * <p>
	 * Assumes that the {@link #checkLayout} method was called before with the
	 * specified layout.
	 * 
	 * @param  db The database this table belongs to, not allowed to be {@code
	 *         null}.
	 * @param  name The name of the table, not allowed to be {@code null} and
	 *         not allowed to be an empty string.
	 * @param  layout The table's layout, not allowed to be {@code null}.
	 * @param  dbFile The database file, not allowed to be {@code null}.
	 * @param  opMode The operating mode of the RO database.
	 * 
	 * @throws ImplementationRestrictionException If the table has too many
	 *         columns.
	 * @throws CreationException If at least one column of the table definition
	 *         is not in accordance with its corresponding column as defined in
	 *         the sequence of column sublayouts or if the store can't be
	 *         created due to any reason including problems with the store
	 *         sublayout and an I/O error while reading the database file.
	 */
	final void initROTable(Database_ db, String name, Layout layout, Path dbFile,
																				int opMode) throws
								ImplementationRestrictionException, CreationException {
		// name != null && !name.isEmpty() && name does not start with the number
		// sign character && db != null && dbFile != null as per assumption.
		// Furthermore, it is assumed that the checkTableLayout-method for the
		// specified layout has been called before.
		
		// Do some basic initialization.
		init(layout, db, name);
		
		// Create store.
		this.store = new ROStore(layout.getLayout(lt_store), dbFile, opMode,this);
	}
	
	/**
	 * Initializes the RO store of the table.
	 * <p>
	 * Call this method only after the database is completely initialized.
	 * 
	 * @throws ImplementationRestrictionException If the table has too many
	 *         columns needing a separate null information or if the size of
	 *         the FL data block exceeds {@link Integer#MAX_VALUE}.
	 * @throws CreationException If initializing the WR store fails.
	 */
	final void initROStore() {
		((ROStore) store).initialize();
	}
	
	/**
	 * Returns the database this table belongs to.
	 * 
	 * @return The databse this table belongs to.
	 */
	public final Database_ db() {
		return db;
	}
	
	/**
	 * Returns the table's store.
	 * 
	 * @return The table's store.
	 */
	public final Store store() {
		return store;
	}
	
	/**
	 * Returns the table definition map.
	 * 
	 * @return The table definition.
	 */
	public final Column_<?>[] tableDef() {
		return tableDef;
	}
	
	@Override
	public final String name() {
		return name;
	}
	
	@Override
	public final TableInfo info() {
		return new Info(this);
	}
	
	@Override
	public final Database getDatabase() {
		return db;
	}
	
	@Override
	public final boolean hasColumn(String name) {
		boolean found = false;
		int i = 0;
		while (i < tableDef.length && !found) {
			found = tableDef[i++].name().equals(name);
		}
		return found;
	}
	
	@Override
	public final Column<?> getColumn(String colName) throws
																	IllegalArgumentException {
		Column<?> col = null;
		int i = 0;
		while (i < tableDef.length && col == null) {
			Column_<?> tableCol = tableDef[i++];
			if (tableCol.name().equals(colName)) {
				col = tableCol;
			}
		}
		if (col == null) {
			throw new IllegalArgumentException(ACDPException.prefix(this) +
										"No column with the name \"" + colName + "\".");
		}
		return col;
	}
	
	@Override
	public final Column<?>[] getColumns() {
		return Arrays.copyOf(tableDef, tableDef.length);
	}
	
	@Override
	public final long numberOfRows() {
		return store.numberOfRows();
	}
	
	@Override
	public final Ref insert(Object... values) throws
								UnsupportedOperationException, NullPointerException,
								IllegalArgumentException, MaximumException,
								CryptoException, ShutdownException,
								ACDPException, UnitBrokenException, IOFailureException {
		return store.insert(values);
	}
	
	@Override
	public final void delete(Ref ref) throws UnsupportedOperationException,
								NullPointerException, DeleteConstraintException,
								IllegalReferenceException, ShutdownException,
								ACDPException, UnitBrokenException, IOFailureException {
		store.delete((Ref_) ref);
	}
	
	@Override
	public final void update(Ref ref, ColVal<?>... colVals) throws
								UnsupportedOperationException, NullPointerException,
								IllegalArgumentException, IllegalReferenceException,
								MaximumException, CryptoException, ShutdownException,
								ACDPException, UnitBrokenException, IOFailureException {
		store.update((Ref_) ref, colVals);
	}
	
	@Override
	public final void updateAll(ColVal<?>... colVals) throws
								UnsupportedOperationException, NullPointerException,
								IllegalArgumentException, MaximumException,
								CryptoException, ShutdownException, ACDPException,
								UnitBrokenException, IOFailureException {
		store.updateAll(colVals);
	}
	
	@Override
	public final <T> void updateAllSupplyValues(Column<T> col,
													ValueSupplier<T> valueSupplier) throws
						UnsupportedOperationException, NullPointerException,
						IllegalArgumentException, ImplementationRestrictionException,
						MaximumException, CryptoException, ShutdownException,
						ACDPException, UnitBrokenException, IOFailureException {
		store.updateAllSupplyValues((Column_<T>) col, valueSupplier);
	}
	
	@Override
	public final <T> void updateAllChangeValues(Column<T> col,
													ValueChanger<T> valueChanger) throws
						UnsupportedOperationException, NullPointerException,
						IllegalArgumentException, ImplementationRestrictionException,
						MaximumException, CryptoException, ShutdownException,
						ACDPException, UnitBrokenException, IOFailureException {
		store.updateAllChangeValues((Column_<T>) col, valueChanger);
	}
	
	@Override
	public final Row get(Ref ref, Column<?>... cols) throws
									NullPointerException, IllegalArgumentException,
									IllegalReferenceException, CryptoException,
									ShutdownException, IOFailureException {
		return store.get((Ref_) ref, cols);
	}
	
	@Override
	public final TableIterator iterator(Column<?>... cols) throws
									NullPointerException, IllegalArgumentException {
		return store.iterator(cols);
	}
	
	@Override
	public final TableIterator iterator(Ref ref, Column<?>... cols) throws
									NullPointerException, IllegalArgumentException,
									IllegalReferenceException {
		return store.iterator((Ref_) ref, cols);
	}
	
	@Override
	public final TableIterator iterator() {
		return store.iterator(new Column[0]);
	}
	
	@Override
	public final Stream<Row> rows(Column<?>... cols) throws NullPointerException,
																		IllegalArgumentException {
		return store.rows(cols);
	}
	
	@Override
	public final void compactVL() throws UnsupportedOperationException,
								ACDPException, ShutdownException, IOFailureException {
		if (!(store instanceof WRStore)) {
			throw new UnsupportedOperationException(ACDPException.prefix(this) +
																"Database is an RO database.");
		}
		// Database is a WR database.
		((WRStore) store).compactVL();
	}
	
	@Override
	public final void compactFL() throws UnsupportedOperationException,
								ImplementationRestrictionException,
								ACDPException, ShutdownException, IOFailureException {
		if (!(store instanceof WRStore)) {
			throw new UnsupportedOperationException(ACDPException.prefix(this) +
																"Database is an RO database.");
		}
		// Database is a WR database.
		((WRStore) store).compactFL();
	}
	
	@Override
	public final void truncate() throws UnsupportedOperationException,
													DeleteConstraintException, ACDPException,
													ShutdownException, IOFailureException {
		if (!(store instanceof WRStore)) {
			throw new UnsupportedOperationException(ACDPException.prefix(this) +
																"Database is an RO database.");
		}
		// Database is a WR database.
		((WRStore) store).truncate();
	}
	
	@Override
	public final void zip(Path zipArchive, int level) throws
					UnsupportedOperationException, NullPointerException,
					IllegalArgumentException, ShutdownException, IOFailureException {
		if (!(store instanceof WRStore)) {
			throw new UnsupportedOperationException(ACDPException.prefix(this) +
																		"Table has an RO store.");
		}
		// Database is a WR database.
		try (ReadZone rz = db.openReadZone();
				FileOutputStream fos = new FileOutputStream(zipArchive.toFile());
							BufferedOutputStream bos = new BufferedOutputStream(fos);
							ZipOutputStream zos = new ZipOutputStream(bos)) {
			if (level != -1) {
				zos.setLevel(level);
			}
			((WRStore) store).zip(new ZipEntryCollector(null, null, zos), true);
		} catch (IOException e) {
			throw new IOFailureException(this, e);
		}
	}
	
	/**
	 * Deletes the backing files of the table.
	 * <p>
	 * Note that the database must be a WR database.
	 * <p>
	 * Note also that the table's store and hence the whole database won't work
	 * properly anymore after this method has been executed.
	 * The database should be closed as soon as possible.
	 *         
	 * @throws UnsupportedOperationException If this table has an RO store.
	 *         This can only be the case if the database is an RO database.
	 * @throws FileIOException If an I/O error occurs.
	 */
	final void deleteFiles() throws UnsupportedOperationException,
																					FileIOException {
		if (!(store instanceof WRStore)) {
			throw new UnsupportedOperationException(ACDPException.prefix(this) +
																"Database is an RO database.");
		}
		((WRStore) store).deleteFiles();
	}
	
	/**
	 * Creates a column layout for the specified column and inserts it at the
	 * specified position within the list of column layouts contained in the
	 * table layout.
	 * 
	 * @param col The column, not allowed to be {@code null}.
	 * @param colName The name of the column, not allowed to be {@code null} and
	 *        not allowed to be empty.
	 * @param refdTable The name of the table referenced by the column or {@code
	 *        null} if the column does not reference a table.
	 * @param index The index of the column within the table definition.
	 */
	final void insertColIntoLayout(Column_<?> col, String colName,
																String refdTable, int index) {
		fillColLayout(col, colName, refdTable, layout.getSeq(lt_columns).
																			insertLayout(index));
	}
	
	/**
	 * Removes the specified column from the table layout.
	 * 
	 * @param col The column, not allowed to be {@code null}.
	 *        The column must be a column of this table.
	 */
	final void removeColFromLayout(Column_<?> col) {
		layout.getSeq(lt_columns).remove(col.index());
	}
	
	/**
	 * Changes the registered type descriptor and the registered type factory
	 * class name (if any) in the column layout of the column specified by
	 * the {@code col0} argument with respect to the new column {@code col1}.
	 * 
	 * @param col0 The column to be modified, not allowed to be {@code null}.
	 *        The column must be a column of this table.
	 * @param col1 The column replacing the other column, not allowed to be
	 *        {@code null}.
	 */
	final void changeColLayout(Column_<?> col0, Column_<?> col1) {
		// Get column layout of col0.
		final Layout colLayout = layout.getSeq(lt_columns).getLayout(
																					col0.index());
		final Type_ type = col1.type();
		final String typeDesc = type.typeDesc();
		
		// Change type descriptor.
		colLayout.replace(lt_typeDesc, typeDesc);
		// Change type factory class name.
		if (!Type_.isBuiltInType(typeDesc)) {
			final String tfcn = type.getClass().getName();
			if (colLayout.contains(lt_typeFactoryClassName)) 
				colLayout.replace(lt_typeFactoryClassName, tfcn);
			else {
				colLayout.add(lt_typeFactoryClassName, tfcn);
			}
		}
		else if (colLayout.contains(lt_typeFactoryClassName)) {
			colLayout.remove(lt_typeFactoryClassName);
		}
	}
	
	@Override
	public String toString() {
		return name;
	}
}