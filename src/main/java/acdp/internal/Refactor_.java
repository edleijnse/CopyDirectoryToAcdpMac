/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import acdp.Column;
import acdp.Row;
import acdp.Table.ValueChanger;
import acdp.design.CustomTable;
import acdp.design.SimpleType;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CreationException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.MaximumException;
import acdp.internal.Table_.ColumnParams;
import acdp.internal.Table_.TableParams;
import acdp.internal.store.wr.WRStore;
import acdp.internal.types.AbstractArrayType;
import acdp.internal.types.ArrayOfRefType_;
import acdp.internal.types.ArrayType_;
import acdp.internal.types.RefType_;
import acdp.misc.Layout;
import acdp.misc.Utils;
import acdp.types.Type;
import acdp.types.Type.Scheme;
import acdp.tools.Refactor;
import acdp.tools.Refactor.Names;

/**
 * Implements the methods provided by the {@link Refactor} class.
 *
 * @author Beat Hoermann
 */
public final class Refactor_ {
	/**
	 * A class implementing the {@link Names} interface.
	 * 
	 * @author Beat Hoermann
	 */
	public static final class Names_ implements Names {
		private final List<ColumnParams> list = new ArrayList<>();
		private final Set<String> colNames = new HashSet<>();
		
		/**
		 * Adds to the list of column names and references the specified column
		 * name and, provided that the column references a table, the specified
		 * name of the referenced table.
		 * 
		 * @param  colName The name of a column, not allowed to be {@code null}
		 *         and not allowed to be an empty string.
		 * @param  tableName The name of the referenced table or {@code null} if
		 *         this column does not reference a table.
		 *         If this column references a table then this value is not
		 *         allowed to be {@code null} and not allowed to be an empty
		 *         string.
		 *         Moreover, the table must exist in the database.
		 * 
		 * @return This instance.
		 * 
		 * @throws IllegalArgumentException If {@code colName} is {@code null} or
		 *         an empty string or if this method was invoked before with the
		 *         same column name.
		 */
		private final Names_ add_(String colName,
									String tableName) throws IllegalArgumentException {
			if (colNames.contains(colName)) {
				throw new IllegalArgumentException("The column name \"" + colName +
																			"\" already exists.");
			}
			colNames.add(colName);
			list.add(new ColumnParams(colName, tableName));
			return this;
		}
		
		/**
		 * Returns the list of column parameters.
		 * 
		 * @return The list of column parameters, never {@code null} but may be
		 *         empty.
		 */
		final List<ColumnParams> getColumnParams() {
			return list;
		}
		
		@Override
		public final Names_ add(String colName) throws IllegalArgumentException {
			return add_(colName, null);
		}
		
		@Override
		public final Names_ add(String colName, String tableName) throws
																		IllegalArgumentException {
			if (tableName == null || tableName.isEmpty()) {
				throw new IllegalArgumentException("The name of the referenced " +
														"table is null or an empty string.");
			}
			return add_(colName, tableName);
		}
	}
	
	/**
	 * See {@linkplain Refactor#addTable}.
	 */
	public final void addTable(Path layoutFile, String tableName,
														CustomTable table, Names names) throws
							UnsupportedOperationException,  NullPointerException,
							IllegalArgumentException, OverlappingFileLockException,
													CreationException, IOFailureException {
		try (Database_ db = new Database_(layoutFile, -1, false, -1, null)) {
			if (!db.isWritable()) {
				throw new UnsupportedOperationException(ACDPException.prefix(db) +
																"Database is an RO database.");
			}
			// The database is a writable WR database.
			
			if (db.hasTable(tableName)) {
				throw new IllegalArgumentException(ACDPException.prefix(db) +
								"The database already has a table with the name \"" +
																				tableName + "\".");
			}
			// The database has not yet a table with such a name.
			
			final List<ColumnParams> columnParams =
															((Names_) names).getColumnParams();
			final Set<WRStore> refdStores = new HashSet<>();
			boolean referenced = false;
			
			// Collect the stores being referenced by the new table and find out
			// if the new table references itself.
			for (ColumnParams cp : columnParams) {
				final String refdTableName = cp.refdTable();
				if (refdTableName != null) {
					if (refdTableName.equals(tableName))
						// The table references itself.
						referenced = true;
					else {
						refdStores.add((WRStore) ((Table_) db.getTable(
																		refdTableName)).store());
					}
				}
			}
			// The referenced tables are contained in the database.
			
			// Create table layout.
			final Layout tableLayout = Table_.createLayout(new TableParams(
						tableName, table.getBackingTable(Database_.friend).tableDef(),
																	columnParams), referenced);
			// Everything's fine with the input parameters. We are ready for
			// modifying any persistent data.
			
			// Create table's backing files.
			Table_.createFiles(tableLayout, layoutFile.getParent());
			
			// Install the reference counter for the newly referenced stores.
			for (WRStore refdStore : refdStores) {
				if (!refdStore.isReferenced()) {
					// refdStore not yet referenced.
					refdStore.installRefCount();
				}
			}
			
			// Add table layout to database layout and save database layout.
			final Layout layout = db.layout();
			layout.getLayout(Database_.lt_tables).add(tableName, tableLayout);
			db.saveLayout();
		} catch(FileIOException e) {
			throw new IOFailureException(e);
		}
	}
	
	/**
	 * Get the stores of the tables of the specified database;
	 * 
	 * @param  db The database.
	 * 
	 * @return The stores of the tables of the database, never {@code null}.
	 */
	private final Set<WRStore> getStores(Database_ db) {
		final Set<WRStore> stores = new HashSet<>();
		for (Table_ tbl : (Table_[]) db.getTables()) {
			stores.add((WRStore) tbl.store());
		}
		return stores;
	}
	
	/**
	 * See {@linkplain Refactor#dropTable}.
	 */
	public final void dropTable(Path layoutFile, String tableName) throws
								UnsupportedOperationException, NullPointerException,
								IllegalArgumentException, OverlappingFileLockException,
													CreationException, IOFailureException {
		try (Database_ db = new Database_(layoutFile, -1, false, -1, null)) {
			if (!db.isWritable()) {
				throw new UnsupportedOperationException(ACDPException.prefix(db) +
																"Database is an RO database.");
			}
			// The database is a writable WR database.
			
			final Table_ table = (Table_) db.getTable(tableName);
			// The database has a table with that name.
			
			// Get the stores of the tables.
			final Set<WRStore> stores = getStores(db);
			
			// Is the table the only table in the database?
			if (stores.size() == 1) {
				throw new IllegalArgumentException(ACDPException.prefix(table) + 
							"This table is the only table in the database. Call the " +
							"\"ACDP.deleteDBFiles\"-method to remove the database.");
			}
			
			final WRStore store = (WRStore) table.store();
			
			// Is the table referenced by another table?
			final Set<WRStore> refdByOthers = store.refdBy(stores);
			refdByOthers.remove(store);
			if (refdByOthers.size() > 0) {
				final Iterator<WRStore> it = refdByOthers.iterator();
				String names = it.next().table.name();
				while (it.hasNext()) {
					names += ", " + it.next().table.name();
				}
				throw new IllegalArgumentException(ACDPException.prefix(table) +
											"The following tables reference this table: " +
											names + ". Table to be dropped is not " +
											"allowed to be referenced by another table.");
			}
			// Everything's fine with the input parameters. We are ready for
			// modifying persistent data.
			
			// Delete table's backing files.
			table.deleteFiles();
			
			// Remove reference counter of newly unreferenced stores.
			for (WRStore refdStore : store.refdStores()) {
				if (refdStore != store && refdStore.refdBy(stores).size() == 1) {
					// refdStore only referenced by store.
					refdStore.removeRefCount();
				}
			}
			
			// Remove table from database layout and save database layout.
			final Layout layout = db.layout();
			layout.getLayout(Database_.lt_tables).remove(tableName);
			db.saveLayout();
		} catch(FileIOException e) {
			throw new IOFailureException(e);
		}
	}
	
	/**
	 * See {@linkplain Refactor#insertColumn}.
	 */
	public final void insertColumn(Path layoutFile, String tableName,
									String colName, Column<?> col, String refdTableName,
									int index, Object initialValue)  throws
								UnsupportedOperationException,  NullPointerException,
								IllegalArgumentException, MaximumException,
								CryptoException, ImplementationRestrictionException,
								OverlappingFileLockException, CreationException,
																				IOFailureException {
		try (Database_ db = new Database_(layoutFile, -1, false, -1, null)) {
			if (!db.isWritable()) {
				throw new UnsupportedOperationException(ACDPException.prefix(db) +
																"Database is an RO database.");
			}
			// The database is a writable WR database.
			
			final Table_ table = (Table_) db.getTable(tableName);
			// The database has a table with that name.
			
			if (colName == null || colName.isEmpty()) {
				throw new IllegalArgumentException(ACDPException.prefix(table,
								colName) + "Column name is null or an empty string.");
			}
			if (table.hasColumn(colName)) {
				throw new IllegalArgumentException(ACDPException.prefix(table) +
								"The table already has a column with the name \"" +
																					colName + "\".");
			}
			// The column name is neither null nor an empty string and the table
			// has not yet a column with such a name.
			
			if (index < 0 || table.getColumns().length < index) {
				throw new IllegalArgumentException(ACDPException.prefix(table) +
												"Invalid insertion index: " + index + ".");
			}
			// Insertion index is valid.
			
			final Type type = ((Column_<?>) col).type();
			
			final WRStore refdStore;
			if (type instanceof RefType_ || type instanceof ArrayOfRefType_) {
				// col is a reference column
				if (refdTableName == null) {
					throw new IllegalArgumentException(ACDPException.prefix(table,
									colName) + "Name of referenced table not allowed " +
									"to be null for reference column to be inserted.");
				}
				if (!db.hasTable(refdTableName)) {
					throw new IllegalArgumentException(ACDPException.prefix(table,
									colName) + "Column to be inserted is a reference " +
									"column supposed to reference table \"" +
									refdTableName + "\". But the database has no " +
									"table with such a name.");
				}
				if (initialValue != null) {
					throw new IllegalArgumentException(ACDPException.prefix(table,
									colName) + "Initial value must be null for " +
									"reference column to be inserted.");
				}
				refdStore = (WRStore) ((Table_) db.getTable(refdTableName)).store();
			}
			else {
				refdStore = null;
			}
			if (!type.isCompatible(initialValue)) {
				throw new IllegalArgumentException(ACDPException.prefix(table,
									colName) + "The initial value is not compatible " +
									"with the type of the column to be inserted: " +
									initialValue + ".");
			}
			// Name of referenced table and initial value checked.
			
			final Column_<?> col_ = (Column_<?>) col;
			final WRStore store = (WRStore) table.store();
			final boolean installRefCount = refdStore != null &&
																		!refdStore.isReferenced();
			// Initialize column.
			col_.initialize(colName, refdTableName, table, index);
			
			// Everything's fine with the input parameters. We are ready for
			// modifying persistent data.
			store.insertCol(store, col_, index, initialValue, installRefCount &&
												refdStore == store, layoutFile.getParent());
			
			// Install the reference counter if necessary. Note that if
			// refdStore == store then the reference counter was already installed
			// by the store.insertCol-method.
			if (installRefCount && refdStore != store) {
				refdStore.installRefCount();
			}
			
			// Insert column into table layout and save the database layout.
			table.insertColIntoLayout(col_, colName, refdStore == null ? null :
																			refdTableName, index);
			db.saveLayout();
		} catch(FileIOException e) {
			throw new IOFailureException(e);
		}
	}
	
	/**
	 * Computes and returns the <em>singular</em> store, if such a store exists.
	 * <p>
	 * The singular store is the store that is exclusively referenced by the
	 * specified column, which means that no other column across all tables of
	 * the database references this store.
	 * Of course, the singular store cannot exist if the column does not
	 * reference a table.
	 * <p>
	 * After removing the column from the table, the reference counter of a
	 * singular store must be removed.
	 * 
	 * @param  col The column.
	 * @param  store The column's store.
	 * @param  db The database.
	 * 
	 * @return The singular store or {@code null} if the singular store does not
	 *         exist.
	 */
	private final WRStore singularStore(Column_<?> col, WRStore store,
																					Database_ db) {
		// Get the store that is referenced by the column, if any.
		WRStore sing = store.refdStore(col);
				
		if (sing != null) {
			// col references sing
			
			// Get all stores that reference sing, must include store.
			final Set<WRStore> refdByStores = sing.refdBy(getStores(db));
			
			if (refdByStores.size() > 1)
				// Other stores reference sing.
				sing = null;
			else {
				// refdByStores.size() == 1
				// sing is only referenced by store. Is col the only column of
				// store that references sing?
				int m = 0;
				for (Column_<?> c : (Column_<?>[]) store.table.getColumns()) {
					if (store.refdStore(c) == sing) {
						m++;
					}
				}
				if (m > 1) {
					// Other columns of store reference sing.
					sing = null;
				}
			}
		}
		
		return sing;
	}
	
	/**
	 * See {@linkplain Refactor#dropColumn}.
	 */
	public final double dropColumn(Path layoutFile, String tableName,
																			String colName) throws
								UnsupportedOperationException,  NullPointerException,
								IllegalArgumentException, OverlappingFileLockException,
													CreationException, IOFailureException {
		try (Database_ db = new Database_(layoutFile, -1, false, -1, null)) {
			if (!db.isWritable()) {
				throw new UnsupportedOperationException(ACDPException.prefix(db) +
																"Database is an RO database.");
			}
			// The database is a writable WR database.
			
			final Table_ table = (Table_) db.getTable(tableName);
			// The database has a table with that name.
			
			final Column_<?> col = (Column_<?>) table.getColumn(colName);
			// The table has a column with that name.
			
			if (table.tableDef().length == 1) {
				throw new IllegalArgumentException(ACDPException.prefix(table,
											colName) + "Column to be dropped is the only "+
											"column of the table. Call the \"dropTable\"" +
											"-method to remove the table.");
			}
			// The column to be removed is not the only column of the table.

			final WRStore store = (WRStore) table.store();
			final WRStore singularStore = singularStore(col, store, db);
			
			// Everything's fine with the input parameters. We are ready for
			// modifying persistent data.
			store.removeCol(col, store == singularStore);
			
			// Remove the reference counter if there exists a singular store. Note
			// that if store == singularStore then the reference counter was
			// already removed by the store.removeCol-method.
			if (singularStore != null && store != singularStore) {
				singularStore.removeRefCount();
			}
			
			// Remove column from table layout and save the database layout.
			table.removeColFromLayout(col);
			db.saveLayout();
			
			return store.computeGapsRatio();
		} catch(FileIOException e) {
			throw new IOFailureException(e);
		}
	}
	
	/**
	 * A helper class for conveniently exploring some basic properties of a
	 * column type.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class TypeHelper {
		/**
		 * The constructor.
		 */
		TypeHelper() {
		}
		
		/**
		 * The value type of an ST.
		 * 
		 * @param  st The type which has to be an ST.
		 * 
		 * @return The value type of the specified simple type.
		 */
		final Class<?> vt(Type st) {
			return ((SimpleType<?>) st).valueType();
		}
		
		/**
		 * The element type of an A[ST].
		 * 
		 * @param  at The type which has to be an A[ST].
		 * 
		 * @return The element type of the specified array type.
		 */
		final SimpleType<?> et(Type at) {
			return ((ArrayType_) at).elementType();
		}
		
		/**
		 * The length of an ST.
		 * 
		 * @param  st The type which has to be an ST.
		 * 
		 * @return The length of the specified simple type.
		 */
		final int length(Type st) {
			return ((SimpleType<?>) st).length();
		}
		
		/**
		 * The nullable property of an ST.
		 * 
		 * @param  st The type which has to be an ST.
		 * 
		 * @return The boolean value {@code true} if the specified simple type
		 *         allows {@code null} values, {@code false} if not.
		 */
		final boolean nullable(Type st) {
			return ((SimpleType<?>) st).nullable();
		}
		
		/**
		 * The maximum size of an A[ST] or A[RT].
		 * 
		 * @param  at The type which has to be an A[ST] or A[RT].
		 * 
		 * @return The maximum size of the specified array type.
		 */
		final int maxSize(Type at) {
			return ((AbstractArrayType) at).maxSize();
		}
		
		/**
		 * Indicates if the type is an RT.
		 * 
		 * @param  t The type to test.
		 * 
		 * @return The boolean value {@code true} if the specified type is an RT,
		 *         {@code false} if not.
		 */
		final boolean isRT(Type t) {
			return t instanceof RefType_;
		}
		
		/**
		 * Indicates if the type is an A[ST] or A[RT].
		 * 
		 * @param  t The type to test.
		 * 
		 * @return The boolean value {@code true} if the specified type is an
		 *         array type, {@code false} if not.
		 */
		final boolean isA(Type t) {
			return t instanceof AbstractArrayType;
		}
		
		/**
		 * Indicates if the type is an A[RT].
		 * 
		 * @param  t The type to test.
		 * 
		 * @return The boolean value {@code true} if the specified type is an
		 *         A[RT], {@code false} if not.
		 */
		final boolean isAofRT(Type t) {
			return t instanceof ArrayOfRefType_;
		}
		
		/**
		 * Indicates if the type is an outrow type.
		 * 
		 * @param  t The type to test.
		 * 
		 * @return The boolean value {@code true} if the specified type is an
		 *         outrow type, {@code false} if not.
		 */
		final boolean isOutrow(Type t) {
			return t.scheme() == Scheme.OUTROW;
		}
	}
	
	/**
	 * Finds out if the modification from one column type to the other is a
	 * <em>trivial modification</em>.
	 * A trivial modification is a modification that does not require an
	 * accommodation of the FL data file.
	 * 
	 * @param  t0 The type of the column to be modified.
	 * @param  t1 The type of the column replacing the column to be modified.
	 * @param  t The type helper.
	 * 
	 * @return The integer value 0, 1, 2 if the modification is not a trivial
	 *         modification, a trivial modification which need no further action
	 *         and a trivial modification which just requires to check if all
	 *         stored values are not {@code null}, respectively.
	 */
	private final int trivialCase(Type t0, Type t1, TypeHelper t) {
		// Preconditions:
		// 1. Type descriptors are differen.
		// 2. Both types are ST having same value types OR both types are A[RT]
		//    or both types are A[ST] where element types have same value types.
		int tc = 0;
		
		if (!t.isA(t0)) {
			// Both types are ST having same VT
			if (t.isOutrow(t0) && t.isOutrow(t1) && t.length(t0) == t.length(t1)) {
				// Nullable property only property that changes.
				tc = t.nullable(t0) ? 2 : 1;
			}
		}
		else {
			// Both types are A[ST] having same VT OR both types are A[RT]
			if (Utils.lor(t.maxSize(t0)) == Utils.lor(t.maxSize(t1))) {
				if (t.isAofRT(t0)) {
					// Both types are A[RT]
					if (t0.scheme() == t1.scheme()) {
						// Same storage schemes, different values of maxSize.
						tc = 1;
					}
				}
				else {
					// Both types are A[ST]
					final Type et0 = t.et(t0);
					if (et0.typeDesc().equals(t.et(t1).typeDesc()) &&
																				t.isOutrow(et0)) {
						// Array-types are identical except for maxSize or scheme
						// and their element types have an OUTROW scheme.
						tc = 1;
					}
				}
			}
		}
		
		return tc;
	}
	
	/**
	 * See {@linkplain Refactor#modifyColumn}.
	 */
	public final <T> double modifyColumn(Path layoutFile, String tableName,
				String colName, Column<T> col, ValueChanger<T> valueChanger) throws
									UnsupportedOperationException, NullPointerException, 
									IllegalArgumentException, MaximumException,
									CryptoException, ImplementationRestrictionException,
									OverlappingFileLockException, CreationException,
																				IOFailureException {
		try (Database_ db = new Database_(layoutFile, -1, false, -1, null)) {
			if (!db.isWritable()) {
				throw new UnsupportedOperationException(ACDPException.prefix(db) +
																"Database is an RO database.");
			}
			// The database is a writable WR database.
			
			final Table_ table = (Table_) db.getTable(tableName);
			// The database has a table with that name.
			
			final Column_<?> col0 = (Column_<?>) table.getColumn(colName);
			// The table has a column with that name.
			
			final Type t0 = col0.type();
			final Type t1 = ((Column_<?>) col).type();
			// col is not null.
			
			final TypeHelper t = new TypeHelper();
			
			if (t.isRT(t0) || t.isRT(t1) || t.isAofRT(t0) && !t.isAofRT(t1) ||
															!t.isAofRT(t0) && t.isAofRT(t1)) {
				throw new UnsupportedOperationException(ACDPException.prefix(table,
							colName) + "Modifications where reference columns are " +
							"involved are limited to changing the scheme or the " +
							"maximum size of an array of references.");
			}
			// No reference columns are involved or modification consists of just
			// changing scheme or maxSize of an array of references.
			
			// Types are one of ST, A[ST], A[RT]. Moreover, t0 is A[RT] if and
			// only if t1 is A[RT].
			
			final boolean isEmpty = table.numberOfRows() == 0;
			if (isEmpty || t.isAofRT(t0))
				valueChanger = null;
			else {
				// !isEmpty && types are one of ST, A[ST]
				if (valueChanger == null && !(t.isA(t0) && t.isA(t1) &&
													t.vt(t.et(t0)) == t.vt(t.et(t1)) ||
									            !t.isA(t0) && !t.isA(t1) &&
						                     t.vt(t0) == t.vt(t1))) {
					// valueChanger == null AND (one type is ST and other is A[ST] OR
					//                        the value types  are different)
					throw new IllegalArgumentException(ACDPException.prefix(table,
												colName) + "Converter not allowed to be " +
												"null in this case.");
				}
				// valueChanger != null OR (both types are A[ST] OR both types are
				//                           ST) AND both value types are identical  
			}
			// isEmpty OR types are both A[RT] IMPLIES valueChanger == null
			// valueChanger != null IMPLIES !isEmpty AND none of the types is A[RT]
			// valueChanger == null IMPLIES isEmpty OR both types are A[RT] OR
			//                     (both types are A[ST] OR both types are ST) AND
			//                     both value types are identical
			
			final WRStore store = (WRStore) table.store();
			
			if (t0.typeDesc().equals(t1.typeDesc())) {
				// Column type remains unchanged.
				if (valueChanger != null) {
					@SuppressWarnings("unchecked")
					final Column<T> col0_ = (Column<T>) col0;
					table.updateAllChangeValues(col0_, valueChanger);
				}
			}
			else {
				// Column type changes.
				if (!isEmpty) {
					final int tc = valueChanger == null ? trivialCase(t0, t1, t) : 0;
					if (tc > 0) {
						// This is a trivial case.
						if (tc == 2) {
							// Types are both OUTROW ST. No properties change except
							// the nullable property changes from NULL allowed to NULL
							// not allowed. Ensure that none of the stored values is
							// NULL.
							for (Iterator<Row> it = table.iterator(col0);
																					it.hasNext(); ) {
								if (it.next().get(0) == null) {
									throw new NullPointerException(ACDPException.prefix(
											table, colName) + "The column contains a " +
											"null value but new column type does not " +
											"allow null values.");
								}
							}
						}
					}
					else {
						// Table is not empty and type descriptors are different AND
						// (valueChanger != null OR no trivial modification).
						store.modifyCol(col0, (Column_<T>) col, valueChanger, 
																		layoutFile.getParent());
					}
				}
				// Change column layout and save the database layout.
				table.changeColLayout(col0, (Column_<T>) col);
				db.saveLayout();
			}
			return store.computeGapsRatio();
		} catch(FileIOException e) {
			throw new IOFailureException(e);
		}
	}
	
	/**
	 * See {@linkplain Refactor#nobsRowRef}.
	 */
	public final void nobsRowRef(Path layoutFile, String tableName,
																					int value) throws
									UnsupportedOperationException, NullPointerException, 
									IllegalArgumentException, MaximumException,
									ImplementationRestrictionException,
									OverlappingFileLockException, CreationException,
																				IOFailureException {
		try (Database_ db = new Database_(layoutFile, -1, false, -1, null)) {
			if (!db.isWritable()) {
				throw new UnsupportedOperationException(ACDPException.prefix(db) +
																"Database is an RO database.");
			}
			// The database is a writable WR database.
			
			final Table_ table = (Table_) db.getTable(tableName);
			// The database has a table with that name.
			
			final WRStore store = (WRStore) table.store();
			
			// Check new value of nobsRowRef.
			if (store.checkNobsRowRef(value)) {
				// Get the stores that reference store.
				final Set<WRStore> refdByStores = store.refdBy(getStores(db));
			
				// Change the length of the references stored in the reference
				// columns of all stores that reference store.
				for (WRStore s : refdByStores) {
					s.changeRowRefLen(store, value);
				}
			
				// Change store layout and save the database layout.
				store.changeNobsRowRefInLayout(value);
				db.saveLayout();
			}
		} catch(FileIOException e) {
			throw new IOFailureException(e);
		}
	}
	
	/**
	 * See {@linkplain Refactor#nobsOutrowPtr}.
	 */
	public final void nobsOutrowPtr(Path layoutFile, String tableName,
																					int value) throws
									UnsupportedOperationException, NullPointerException, 
									IllegalArgumentException,
									ImplementationRestrictionException,
									OverlappingFileLockException, CreationException,
																				IOFailureException {
		try (Database_ db = new Database_(layoutFile, -1, false, -1, null)) {
			if (!db.isWritable()) {
				throw new UnsupportedOperationException(ACDPException.prefix(db) +
																"Database is an RO database.");
			}
			// The database is a writable WR database.

			final Table_ table = (Table_) db.getTable(tableName);
			// The database has a table with that name.

			final WRStore store = (WRStore) table.store();

			// Check new value of nobsOutrowPtr.
			if (store.checkNobsOutrowPtr(value)) {
				// Table has a VL file space, hence, table has at least one VL
				// column.
			
				// Change the length of the pointers in the VL columns.
				store.changeOutrowPtrLen(value);
			
				// Change store layout and save the database layout.
				store.changeNobsOutrowPtrInLayout(value);
				db.saveLayout();
			}
		} catch(FileIOException e) {
			throw new IOFailureException(e);
		}
	}
	
	/**
	 * See {@linkplain Refactor#nobsRefCount}.
	 */
	public final void nobsRefCount(Path layoutFile, String tableName,
																					int value) throws
									UnsupportedOperationException, NullPointerException, 
									IllegalArgumentException,
									ImplementationRestrictionException,
									OverlappingFileLockException, CreationException,
																				IOFailureException {
		try (Database_ db = new Database_(layoutFile, -1, false, -1, null)) {
			if (!db.isWritable()) {
				throw new UnsupportedOperationException(ACDPException.prefix(db) +
																"Database is an RO database.");
			}
			// The database is a writable WR database.

			final Table_ table = (Table_) db.getTable(tableName);
			// The database has a table with that name.

			final WRStore store = (WRStore) table.store();

			// Check new value of nobsRefCount.
			if (store.checkNobsRefCount(value)) {
				// Table is referenced by at least one table in the database.
			
				// Change the length of the reference counter.
				store.changeRefCountLen(value);
			
				// Change store layout and save the database layout.
				store.changeNobsRefCountInLayout(value);
				db.saveLayout();
			}
		} catch(FileIOException e) {
			throw new IOFailureException(e);
		}
	}
}