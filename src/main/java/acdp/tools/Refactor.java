/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.tools;

import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;

import acdp.Column;
import acdp.Table;
import acdp.Table.ValueChanger;
import acdp.design.CustomDatabase;
import acdp.design.CustomTable;
import acdp.design.SimpleType;
import acdp.exceptions.CreationException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.MaximumException;
import acdp.internal.Refactor_;
import acdp.internal.Refactor_.Names_;
import acdp.types.Type;

/**
 * Provides methods for changing the structure of a WR database as well as
 * methods for changing some fundamental table parameters which involve the
 * modification of the internal format of one or more backing files.
 * <p>
 * The methods assume that the database is closed.
 * They can be applied no matter if the database is empty or houses some
 * persisted data.
 * The methods change the database layout.
 * <p>
 * Check the {@link Settings} class in case you miss a method.
 *
 * @author Beat Hoermann
 */
public final class Refactor {
	/**
	 * Declares methods for fluently creating a list where each element is equal
	 * to a column name and, provided that the column references a table, the
	 * name of the referenced table.
	 * <p>
	 * Instances of this interface, which are returned by the {@link
	 * Refactor#names} method, are used in the {@link Refactor#addTable} method.
	 * The methods declared in this interface must be invoked in the same order
	 * in which the columns appear in the {@linkplain Table table definition} of
	 * the newly added table.
	 * 
	 * @author Beat Hoermann
	 */
	public interface Names {
		/**
		 * Adds to the list of column and table names the specified column name.
		 * <p>
		 * Invoke this method if this column does not reference a table.
		 * 
		 * @param  columnName The name of this column, not allowed to be {@code
		 *         null} and not allowed to be an empty string.
		 * 
		 * @return This instance.
		 * 
		 * @throws IllegalArgumentException If {@code columnName} is {@code null}
		 *         or an empty string or if this method was invoked before with
		 *         the same column name.
		 */
		Names add(String columnName) throws IllegalArgumentException;
		
		/**
		 * Adds to the list of column and table names the specified column name
		 * and the specified name of the table that is referenced by this column.
		 * <p>
		 * Invoke this method if this column references the specified table.
		 * 
		 * @param  columnName The name of this column, not allowed to be {@code
		 *         null} and not allowed to be an empty string.
		 * @param  tableName The name of the referenced table, not allowed to be
		 *         {@code null} and not allowed to be an empty string.
		 *         The table must exist in the database.
		 * 
		 * @return This instance.
		 * 
		 * @throws IllegalArgumentException If {@code columnName} or {@code
		 *         tableName} is {@code null} or an empty string or if this method
		 *         was invoked before with the same column name.
		 */
		Names add(String columnName, String tableName) throws
																		IllegalArgumentException;
	}
	
	/**
	 * Creates an instance of a class implementing the {@link Names} interface.
	 * 
	 * @return The created instance, never {@code null}.
	 */
	public static final Names names() {
		return new Names_();
	}
	
	/**
	 * Adds the specified table to the specified WR database.
	 * <p>
	 * The {@linkplain Table table definition} of the newly added table is given
	 * by the specified {@linkplain CustomTable custom table}.
	 * To provide names for the columns and the referenced tables for the first,
	 * second, etc. column in the table definition apply method chaining like
	 * this:
	 * 
	 * <pre>
	 * Refactor.names().add("c1").add("c2", "t").add("c3")</pre>
	 * 
	 * where "c1", "c2" and "c3" are the desired names of the first, second and
	 * third column and "t" is the name of the table referenced by column "c2".
	 * (It is assumed that only the second column references a table.)
	 * <p>
	 * Adding a table includes adding a new table layout to the database layout.
	 * Moreover, if the new table references some other tables which are not yet
	 * referenced then the {@code nobsRefCount} field is added to the table
	 * layouts of these newly referenced tables and the tables are refactored
	 * accordingly.
	 * As described in the {@linkplain acdp.tools.Setup Setup Tool}, you may
	 * want to review the new fields in the database layout and customize them
	 * to suit your personal needs.
	 * <p>
	 * Don't forget to insert the custom table in your {@linkplain
	 * CustomDatabase custom database} at the <em>end</em> of the list containing
	 * the custom tables.
	 * <p>
	 * This method fails if the database is not a WR database or if the database
	 * is currently open.
	 * <p>
	 * If the specified file does not contain a valid layout for a WR database
	 * then this method may throw an exception that is different from the listed
	 * exceptions.
	 * <p>
	 * Caveat: If this method throws an exception then there is a risk that the
	 * database is broken.
	 * You may want to {@linkplain acdp.Database#zip backup the
	 * database} before executing this method.
	 * 
	 * @param  layoutFile The WR database layout file, not allowed to be {@code
	 *         null}.
	 * @param  tableName The name of the table to be added to the database,
	 *         not allowed to be {@code null} and not allowed to be an empty
	 *         string and not allowed to start with the number sign ('#')
	 *         character.
	 *         This method fails if a table with this name already exists in the
	 *         database.
	 * @param  table The custom table, not allowed to be {@code null}.
	 * @param  names The column names and the names of the referenced tables, not
	 *         allowed to be {@code null}.
	 * 
	 * @throws UnsupportedOperationException If the database is an RO database.
	 * @throws NullPointerException If at least one of {@code layoutFile},
	 *         {@code table}, and {@code names} is {@code null}.
	 * @throws IllegalArgumentException If {@code tableName} is null or an empty
	 *         string or starts with the number sign character ('#') or if a
	 *         table with this name already exists in the database or if the
	 *         table definition of the specified custom table is {@code null} or
	 *         if {@code names} is {@code null} or returns an empty list or
	 *         contains a name of a referenced table that is not equal to the
	 *         {@code tableName} argument and that is not the name of a table in
	 *         the database or if the table definition and the list of names do
	 *         not correspond to each other.
	 * @throws OverlappingFileLockException If the WR database is currently open.
	 * @throws CreationException If the database can't be opened due to any other
	 *         reason.
	 * @throws IOFailureException If the layout file does not exist or if one of
	 *         the backing files of the new table already exists or if another
	 *         I/O error occurs.
	 */
	public static final void addTable(Path layoutFile, String tableName,
														CustomTable table, Names names) throws
							UnsupportedOperationException,  NullPointerException,
							IllegalArgumentException, OverlappingFileLockException,
													CreationException, IOFailureException {
		new Refactor_().addTable(layoutFile, tableName, table, names);
	}
	
	/**
	 * Removes the specified table from the specified WR database.
	 * <p>
	 * Removing a table includes removing the table layout of the table from the
	 * database layout.
	 * Moreover, if the table references some other tables and no other tables
	 * reference these tables then the {@code nobsRefCount} field is removed
	 * from the table layouts of these tables and the tables are refactored
	 * accordingly.
	 * <p>
	 * Don't forget to remove the corresponding {@linkplain CustomTable custom
	 * table} from your {@linkplain CustomDatabase custom database}.
	 * <p>
	 * This method fails if the specified table is the only table in the database
	 * or if the table is referenced by another table or if the database is not
	 * a WR database or if the database is currently open.
	 * <p>
	 * If the specified file does not contain a valid layout for a WR database
	 * then this method may throw an exception that is different from the listed
	 * exceptions.
	 * <p>
	 * Caveat: If this method throws an exception then there is a risk that the
	 * database is broken.
	 * You may want to {@linkplain acdp.Database#zip backup the
	 * database} before executing this method.
	 * 
	 * 
	 * @param  layoutFile The WR database layout file, not allowed to be {@code
	 *         null}.
	 * @param  tableName The name of the table to be dropped from the database,
	 *         not allowed to be {@code null}.
	 * 
	 * @throws UnsupportedOperationException If the database is an RO database.
	 * @throws NullPointerException If {@code layoutFile} is {@code null}.
	 * @throws IllegalArgumentException If {@code tableName} is {@code null} or
	 *         if the database has no table with this name or if this table is
	 *         the only table in the database or if there exists another table
	 *         that references this table.
	 * @throws OverlappingFileLockException If the WR database is currently open.
	 * @throws CreationException If the database can't be opened due to any other
	 *         reason.
	 * @throws IOFailureException If the layout file does not exist or if another
	 *         I/O error occurs.
	 */
	public static final void dropTable(Path layoutFile, String tableName) throws
								UnsupportedOperationException, NullPointerException,
								IllegalArgumentException, OverlappingFileLockException,
													CreationException, IOFailureException {
		new Refactor_().dropTable(layoutFile, tableName);
	}
	
	/**
	 * Inserts the specified column into the {@linkplain Table table definition}
	 * of the specified table at the position given by the {@code index}
	 * argument.
	 * The new column will have an index equal to the value of {@code index}
	 * while columns with an index greater than or equal to the value of {@code
	 * index} will have their index incremented by one.
	 * <p>
	 * Inserting a new column includes inserting a new column layout into the
	 * table layout.
	 * Moreover, if the column is a column storing its values in the VL file
	 * space and no other column of the table stores its values in the VL file
	 * space yet then this method creates the VL data file and adds the fields
	 * {@code vlDataFile} and {@code nobsOutrowPtr} to the layout of the table.
	 * Furthermore, if the column references a table that is not yet referenced,
	 * then the {@code nobsRefCount} field is added to the layout of the newly
	 * referenced table and the table is refactored accordingly.
	 * As described in the {@linkplain acdp.tools.Setup Setup Tool}, you may
	 * want to review these new fields in the database layout and customize them
	 * to suit your personal needs.
	 * (Use the {@link #nobsOutrowPtr} and {@link #nobsRefCount} methods if you
	 * want to reduce the values of the {@code nobsOutrowPtr} and {@code
	 * nobsRefCount} fields, respectively.)
	 * <p>
	 * Don't forget to create and insert the new column within your corresponding
	 * {@linkplain CustomTable custom table}.
	 * <p>
	 * This method fails if the database is not a WR database or if the database
	 * is currently open.
	 * <p>
	 * If the specified file does not contain a valid layout for a WR database
	 * then this method may throw an exception that is different from the listed
	 * exceptions.
	 * <p>
	 * Caveat: If this method throws an exception then there is a risk that the
	 * database is broken.
	 * You may want to {@linkplain acdp.Database#zip backup the
	 * database} before executing this method.
	 * 
	 * @param  layoutFile The WR database layout file, not allowed to be {@code
	 *         null}.
	 * @param  tableName The name of the table, not allowed to be {@code null}.
	 * @param  columnName The name of the column to be inserted into the table,
	 *         not allowed to be {@code null}.
	 * @param  column The column to be inserted, not allowed to be {@code null}.
	 * @param  refdTableName The name of the referenced table.
	 *         This value is not allowed to be {@code null} if the column is
	 *         a reference column.
	 *         (A reference column is a column with a type equal to {@link
	 *         acdp.types.RefType RefType} or {@link
	 *         acdp.types.ArrayOfRefType ArrayOfRefType}.)
	 *         This value is ignored if the column is not a reference column.
	 * @param  index The index within the table definition where the column is
	 *         to be inserted, must satisfy 0 &le; {@code index} &le; {@code n},
	 *         where {@code n} denotes the number of columns in the table.
	 * @param  initialValue The initial value.
	 *         Existing rows of the table are filled with this value in the new
	 *         column.
	 *         If the new column is a reference column then this value must be
	 *         {@code null}.
	 *         Otherwise, this value must be {@linkplain
	 *         acdp.types.Type#isCompatible compatible} with the type of the
	 *         new column.
	 *         
	 * @throws UnsupportedOperationException If the database is an RO database.
	 * @throws NullPointerException If {@code layoutFile} or {@code column} is
	 *         {@code null}.
	 * @throws IllegalArgumentException If the database has no table equal to
	 *         {@code tableName} or if {@code columnName} is {@code null} or an
	 *         empty string or if the table already has a column with such a
	 *         name.
	 *         This exception also happens if the insertion index is negative
	 *         or greater than the number of columns in the table or if the
	 *         column to be inserted is a reference column but the database has
	 *         no table equal to the {@code refdTableName} argument or the
	 *         column to be inserted is a reference column but the initial value
	 *         is not {@code null} or if the initial value is not compatible
	 *         with the type of the column to be inserted.
	 *         The exception also happens if the length of the byte
	 *         representation of the initial value (or one of the elements if
	 *         the initial value is an array value) exceeds the maximum length
	 *         allowed by the {@linkplain acdp.design.SimpleType simple
	 *         column type}.
	 * @throws MaximumException If a new memory block in the VL file space must
	 *         be allocated and its file position exceeds the maximum allowed
	 *         position
	 * @throws CryptoException If encryption fails.
	 *         This exception never happens if the database does not apply
	 *         encryption.
	 * @throws ImplementationRestrictionException If the Null-info must be
	 *         expanded by a single bit but the bitmap is too large for being
	 *         expanded or if the new size of the FL data blocks exceeds {@code
	 *         Integer.MAX_VALUE}.
	 * @throws OverlappingFileLockException If the WR database is currently open.
	 * @throws CreationException If the database can't be opened due to any other
	 *         reason.
	 * @throws IOFailureException	If an I/O error occurs.
	 */
	public static final void insertColumn(Path layoutFile, String tableName,
					String columnName, Column<?> column, String refdTableName,
													int index, Object initialValue)  throws
					UnsupportedOperationException,  NullPointerException,
					IllegalArgumentException, MaximumException, CryptoException,
					ImplementationRestrictionException, OverlappingFileLockException,
													CreationException, IOFailureException {
		new Refactor_().insertColumn(layoutFile, tableName, columnName, column,
														refdTableName, index, initialValue);
	}
	
	/**
	 * Removes the specified column from the specified table.
	 * <p>
	 * Removing a column includes removing the column layout of the column from
	 * the table layout.
	 * Moreover, if the column is a column storing its values in the VL file
	 * space and no other column of the table stores its values in the VL file
	 * space then this method deletes the VL data file and removes the fields
	 * {@code vlDataFile} and {@code nobsOutrowPtr} from the layout of the table.
	 * Furthermore, if the column references a table and no other column of any
	 * table references that table then the {@code nobsRefCount} field is removed
	 * from the layout of that table and the table is refactored accordingly.
	 * <p>
	 * Don't forget to remove the column from your corresponding {@linkplain
	 * CustomTable custom table}.
	 * <p>
	 * This method fails if the specified clolumn is the only column of the
	 * table or if the database is not a WR database or if the database is
	 * currently open.
	 * <p>
	 * If the specified file does not contain a valid layout for a WR database
	 * then this method may throw an exception that is different from the listed
	 * exceptions.
	 * <p>
	 * Caveat: If this method throws an exception then there is a risk that the
	 * database is broken.
	 * You may want to {@linkplain acdp.Database#zip backup the
	 * database} before executing this method.
	 * 
	 * @param  layoutFile The WR database layout file, not allowed to be {@code
	 *         null}.
	 * @param  tableName The name of the table, not allowed to be {@code null}.
	 * @param  columnName The name of the column to be dropped from the table,
	 *         not allowed to be {@code null}.
	 *         
	 * @return The size of the {@linkplain
	 *         acdp.Information.WRStoreInfo#vlUnused() unused VL file
	 *         space} in relation to its total size.
	 *         For instance, a value equal to 0.9 means that 9 out of 10 bytes
	 *         of the VL file space are unused.
	 *         If there is no unused VL file space then this method returns 0.
	 *         If the VL file space was removed by this method or if this store
	 *         never had a VL file space than this method returns -1.0.
	 *         The returned value may be used to decide whether it is appropriate
	 *         to {@linkplain Table#compactVL() compact the VL file space} or
	 *         not.
	 *         (Removing a VL column may significantly increase the amount of
	 *         unused VL file space.)
	 * 
	 * @throws UnsupportedOperationException If the database is an RO database.
	 * @throws NullPointerException If {@code layoutFile} is {@code null}.
	 * @throws IllegalArgumentException If the database has no table equal to
	 *         {@code tableName} or if the table has no column equal to {@code
	 *         columnName} or if the specified column is the only column of the
	 *         table.
	 * @throws OverlappingFileLockException If the WR database is currently open.
	 * @throws CreationException If the database can't be opened due to any other
	 *         reason.
	 * @throws IOFailureException	If an I/O error occurs.
	 */
	public static final double dropColumn(Path layoutFile, String tableName,
																	String columnName) throws
								UnsupportedOperationException,  NullPointerException,
								IllegalArgumentException, OverlappingFileLockException,
													CreationException, IOFailureException {
		return new Refactor_().dropColumn(layoutFile, tableName, columnName);
	}
	
	/**
	 * Modifies the specified column of the specified table by replacing that
	 * column with the specified new column.
	 * <p>
	 * All kinds of modifications are allowed with the following two exceptions:
	 * 
	 * <ul>
	 *    <li>Changing the name of a column.
	 *        To change the name of a column just change the database layout
	 *        accordingly or invoke the {@link Settings#setColName} method.</li>
	 *    <li>Modifying a reference column or changing a column to a reference
	 *        column.
	 *        (A reference column is a column with a type equal to {@link
	 *        acdp.types.RefType RefType} or {@link
	 *        acdp.types.ArrayOfRefType ArrayOfRefType}.)
	 *        Modifications where reference columns are involved are limited to
	 *        changes regarding the storage scheme and the maximum size of an
	 *        {@code ArrayOfRefType}.
	 *        In any other case, use the {@link #dropColumn} and {@link
	 *        #insertColumn} methods if a reference column is involved.</li>
	 * </ul>
	 * <p>
	 * Modifying the {@linkplain SimpleType#valueType() value type} of a column
	 * type, be it the value type of a {@link SimpleType} or the value type of
	 * the component type of an {@link acdp.types.ArrayType ArrayType}, requires
	 * that the user provides a {@linkplain ValueChanger value changer} which
	 * converts the stored values into new values that must be {@linkplain
	 * Type#isCompatible compatible} with the new value type.
	 * (If the value changer returns a value that is not compatible with the new
	 * value type then this method throws an exception, however, this may be an
	 * exception of a type not listed below.)
	 * If the value type remains unchanged then the {@code valueChanger} argument
	 * may be equal to {@code null} which means that the values remain unchanged
	 * (identity conversion).
	 * You may even decide to invoke this method such that the column type
	 * remains unchanged and with a non-null value changer.
	 * In such a case this method internally executes the {@link
	 * Table#updateAllChangeValues} method.
	 * This method has no effect if the column type remains unchanged
	 * <em>and</em> the {@code valueChanger} argument is set equal {@code null}.
	 * <p>
	 * Modifying a column includes changing the column layout of the column to be
	 * modified.
	 * Moreover, if the column replacing the column to be modified is a column
	 * storing its values in the VL file space and no column of the table stores
	 * its values in the VL file space yet then this method creates the VL data
	 * file and adds the fields {@code vlDataFile} and {@code nobsOutrowPtr} to
	 * the layout of the table.
	 * As described in the {@linkplain acdp.tools.Setup Setup Tool}, you may
	 * want to review these new fields in the database layout and customize them
	 * to suit your personal needs.
	 * (Use the {@link #nobsOutrowPtr} method if you want to reduce the value of
	 * the {@code nobsOutrowPtr} field.)
	 * Conversely, if the column to be modified is a column storing its values
	 * in the VL file space and no other column of the table stores its values
	 * in the VL file space and the column replacing the column to be modified
	 * stores its values in the FL file space then this method deletes the VL
	 * data file and removes the {@code vlDataFile} and {@code nobsOutrowPtr}
	 * fields from the layout of the table.
	 * <p>
	 * Don't forget to modify the column accordingly within your corresponding
	 * {@linkplain CustomTable custom table}.
	 * <p>
	 * This method fails if the database is not a WR database or if the database
	 * is currently open.
	 * <p>
	 * If the specified file does not contain a valid layout for a WR database
	 * then this method may throw an exception that is different from the listed
	 * exceptions.
	 * <p>
	 * Caveat: If this method throws an exception then there is a risk that the
	 * database is broken.
	 * You may want to {@linkplain acdp.Database#zip backup the
	 * database} before executing this method.
	 * 
	 * @param  <T> The value type of the column.
	 * 
	 * @param  layoutFile The WR database layout file, not allowed to be {@code
	 *         null}.
	 * @param  tableName The name of the table, not allowed to be {@code null}.
	 * @param  columnName The name of the column to be modified, not allowed to
	 *         be {@code null}.
	 * @param  column The column replacing the column to be modified, not allowed
	 *         to be {@code null}.
	 * @param  valueChanger The value changer, may be {@code null} in some cases.
	 *         This value is ignored if the table is an empty table or if the
	 *         type of the column replacing the column to be modified is an array
	 *         of references.
	 *         
	 * @return The size of the {@linkplain
	 *         acdp.Information.WRStoreInfo#vlUnused() unused VL file
	 *         space} in relation to its total size.
	 *         For instance, a value equal to 0.9 means that 9 out of 10 bytes
	 *         of the VL file space are unused.
	 *         If there is no unused VL file space then this method returns 0.
	 *         If the VL file space was removed by this method or if this store
	 *         never had a VL file space than this method returns -1.0.
	 *         The returned value may be used to decide whether it is appropriate
	 *         to {@linkplain Table#compactVL() compact the VL file space} or
	 *         not.
	 *         (Modifying a VL column may significantly increase the amount of
	 *         unused VL file space.)
	 *         
	 * @throws UnsupportedOperationException If the database is an RO database
	 *         or if the modification requires an unsupported modification
	 *         involving reference columns.
	 * @throws NullPointerException If one of the arguments not allowed to be
	 *         {@code null} is {@code null} or if a value of a simple column
	 *         type is set equal to {@code null} but the column type forbids the
	 *         {@code null} value or if the value is an array value and this
	 *         condition is satisfied for at least one element contained in the
	 *         array.
	 * @throws IllegalArgumentException If the database has no table equal to
	 *         {@code tableName} or if the table has no column equal to {@code
	 *         columnName}.
	 *         This exception also happens if the length of the byte
	 *         representation of a value (or one of the elements if the value is
	 *         an array value) exceeds the maximum length allowed by the
	 *         {@linkplain acdp.design.SimpleType simple column type}.
	 *         Furthermore, this exception happens if the value is an array
	 *         value and the size of the array exceeds the maximum length
	 *         allowed by the array column type.
	 * @throws MaximumException If a new memory block in the VL file space must
	 *         be allocated and its file position exceeds the maximum allowed
	 *         position.
	 * @throws CryptoException If encryption or decryption fails.
	 *         This exception never happens if the WR database does not apply
	 *         encryption or if the column type is an array of references.
	 * @throws ImplementationRestrictionException If the Null-info must be
	 *         expanded by a single bit but the bitmap is too large for being
	 *         expanded <em>or</em> if the byte representation stored in the VL
	 *         file space is too large to fit into the inrow column section
	 *         <em>or</em> if the number of bytes needed to persist a value of
	 *         an array type exceeds Java's maximum array size <em>or</em>,
	 *         provided that this method invokes the {@link
	 *         Table#updateAllChangeValues} method, it turns out that the number
	 *         of row gaps in the table is greater than {@code Integer.MAX_VALUE}
	 *         <em>or</em> if the new size of the FL data blocks exceeds {@code
	 *         Integer.MAX_VALUE}.
	 * @throws OverlappingFileLockException If the WR database is currently open.
	 * @throws CreationException If the database can't be opened due to any other
	 *         reason.
	 * @throws IOFailureException	If an I/O error occurs.
	 */
	public static final <T> double modifyColumn(Path layoutFile,
							String tableName, String columnName, Column<T> column,
														ValueChanger<T> valueChanger) throws
							UnsupportedOperationException, NullPointerException, 
							IllegalArgumentException, MaximumException,
							CryptoException, ImplementationRestrictionException,
							OverlappingFileLockException, CreationException,
																				IOFailureException {
		return new Refactor_().modifyColumn(layoutFile, tableName, columnName, column,
																						valueChanger);
	}
	
	/**
	 * Changes the value of the {@code nobsRowRef} property of the specified
	 * table to the specified value.
	 * (See the section "Explanation of the {@code nobsRowRef}, {@code
	 * nobsOutrowPtr} and {@code nobsRefCount} Settings" in the description of
	 * the {@linkplain acdp.tools.Setup Setup Tool} to learn about this
	 * property.)
	 * <p>
	 * The amount of unused VL file space may significantly increase in tables
	 * with columns that have an outrow storage scheme and store values being
	 * array of references to rows of the specified table.
	 * (For a particular table {@code t} of a WR database you can get the amount
	 * of unused VL file space by executing {@link
	 * acdp.Information.WRStoreInfo#vlUnused() ((WRStoreInfo)
	 * t.info().storeInfo()).vlUnused()}.)
	 * <p>
	 * This method fails if the database is not a WR database or if the database
	 * is currently open.
	 * <p>
	 * If the specified file does not contain a valid layout for a WR database
	 * then this method may throw an exception that is different from the listed
	 * exceptions.
	 * <p>
	 * Caveat: If this method throws an exception then there is a risk that the
	 * database is broken.
	 * You may want to {@linkplain acdp.Database#zip backup the
	 * database} before executing this method.
	 * 
	 * @param  layoutFile The WR database layout file, not allowed to be {@code
	 *         null}.
	 * @param  tableName The name of the table, not allowed to be {@code null}.
	 * @param  value The new value of the {@code nobsRowRef} table property,
	 *         must be greater than or equal to 1 and less than or equal to 8.
	 *         The new value is allowed to be equal to the current value.
	 *         
	 * @throws UnsupportedOperationException If the database is an RO database.
	 * @throws NullPointerException If {@code layoutFile} or {@code tableName}
	 *         is {@code null}.
	 * @throws IllegalArgumentException If the database has no table equal to
	 *         {@code tableName} or if the specified value is less than 1 or
	 *         greater than 8 or if the specified value is too small with respect
	 *         to the number of rows and row gaps in the table.
	 * @throws MaximumException If a new memory block in the VL file space
	 *         must be allocated and its file position exceeds the maximum
	 *         allowed position.
	 * @throws ImplementationRestrictionException If the number of bytes needed
	 *         to persist an array of references in an inrow column exceeds
	 *         Java's maximum array size.
	 * @throws OverlappingFileLockException If the WR database is currently open.
	 * @throws CreationException If the database can't be opened due to any other
	 *         reason.
	 * @throws IOFailureException	If an I/O error occurs.
	 */
	public static final void nobsRowRef(Path layoutFile, String tableName,
																					int value) throws
									UnsupportedOperationException, NullPointerException, 
									IllegalArgumentException, MaximumException,
									ImplementationRestrictionException,
									OverlappingFileLockException, CreationException,
																				IOFailureException {
		new Refactor_().nobsRowRef(layoutFile, tableName, value);
	}
	
	/**
	 * Changes the value of the {@code nobsOutrowPtr} property of the specified
	 * table to the specified value.
	 * (See the section "Explanation of the {@code nobsRowRef}, {@code
	 * nobsOutrowPtr} and {@code nobsRefCount} Setinngs" in the description of
	 * the {@linkplain acdp.tools.Setup Setup Tool} to learn about this
	 * property.)
	 * <p>
	 * This method fails if the database is not a WR database or if the database
	 * is currently open.
	 * <p>
	 * If the specified file does not contain a valid layout for a WR database
	 * then this method may throw an exception that is different from the listed
	 * exceptions.
	 * <p>
	 * Caveat: If this method throws an exception then there is a risk that the
	 * database is broken.
	 * You may want to {@linkplain acdp.Database#zip backup the
	 * database} before executing this method.
	 * 
	 * @param  layoutFile The WR database layout file, not allowed to be {@code
	 *         null}.
	 * @param  tableName The name of the table, not allowed to be {@code null}.
	 * @param  value The new value of the {@code nobsOutrowPtr} table property,
	 *         must be greater than or equal to 1 and less than or equal to 8.
	 *         The new value is allowed to be equal to the current value.
	 *         
	 * @throws UnsupportedOperationException If the database is an RO database.
	 * @throws NullPointerException If {@code layoutFile} or {@code tableName}
	 *         is {@code null}.
	 * @throws IllegalArgumentException If the database has no table equal to
	 *         {@code tableName} or if the table has no VL file space or if
	 *         the specified value is less than 1 or greater than 8 or if the
	 *         specified value is too small with respect to the size of the VL
	 *         file space.
	 * @throws ImplementationRestrictionException If the new size of the FL data
	 *         blocks exceeds {@code Integer.MAX_VALUE}.
	 * @throws OverlappingFileLockException If the WR database is currently open.
	 * @throws CreationException If the database can't be opened due to any other
	 *         reason.
	 * @throws IOFailureException	If an I/O error occurs.
	 */
	public static final void nobsOutrowPtr(Path layoutFile, String tableName,
																					int value) throws
									UnsupportedOperationException, NullPointerException, 
									IllegalArgumentException,
									ImplementationRestrictionException,
									OverlappingFileLockException, CreationException,
																				IOFailureException {
		new Refactor_().nobsOutrowPtr(layoutFile, tableName, value);
	}
	
	/**
	 * Changes the value of the {@code nobsRefCount} property of the specified
	 * table to the specified value.
	 * (See the section "Explanation of the {@code nobsRowRef}, {@code
	 * nobsOutrowPtr} and {@code nobsRefCount} Settings" in the description of
	 * the {@linkplain acdp.tools.Setup Setup Tool} to learn about this
	 * property.)
	 * <p>
	 * This method fails if the database is not a WR database or if the database
	 * is currently open.
	 * <p>
	 * If the specified file does not contain a valid layout for a WR database
	 * then this method may throw an exception that is different from the listed
	 * exceptions.
	 * <p>
	 * Caveat: If this method throws an exception then there is a risk that the
	 * database is broken.
	 * You may want to {@linkplain acdp.Database#zip backup the
	 * database} before executing this method.
	 * 
	 * @param  layoutFile The WR database layout file, not allowed to be {@code
	 *         null}.
	 * @param  tableName The name of the table, not allowed to be {@code null}.
	 * @param  value The new value of the {@code nobsRefCount} table property,
	 *         must be greater than or equal to 1 and less than or equal to 8..
	 *         The new value is allowed to be equal to the current value.
	 *         
	 * @throws UnsupportedOperationException If the database is an RO database.
	 * @throws NullPointerException If {@code layoutFile} or {@code tableName}
	 *         is {@code null}.
	 * @throws IllegalArgumentException If the database has no table equal to
	 *         {@code tableName} or if the table is not referenced by any table
	 *         of the database or if the specified value is less than 1 or
	 *         greater than 8 or if the specified value is too small with
	 *         respect to the row having the highest value of the reference
	 *         counter.
	 * @throws ImplementationRestrictionException If the new size of the FL data
	 *         blocks exceeds {@code Integer.MAX_VALUE}.
	 * @throws OverlappingFileLockException If the WR database is currently open.
	 * @throws CreationException If the database can't be opened due to any other
	 *         reason.
	 * @throws IOFailureException	If an I/O error occurs.
	 */
	public static final void nobsRefCount(Path layoutFile, String tableName,
																					int value) throws
									UnsupportedOperationException, NullPointerException, 
									IllegalArgumentException,
									ImplementationRestrictionException,
									OverlappingFileLockException, CreationException,
																				IOFailureException {
		new Refactor_().nobsRefCount(layoutFile, tableName, value);
	}
	
	/**
	 * Prevent object construction.
	 */
	private Refactor() {
	}
}
