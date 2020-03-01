/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.design;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import acdp.Column;
import acdp.ColVal;
import acdp.Ref;
import acdp.Row;
import acdp.Table;
import acdp.Information.TableInfo;
import acdp.Table.TableIterator;
import acdp.Table.ValueChanger;
import acdp.Table.ValueSupplier;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.DeleteConstraintException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.IllegalReferenceException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.MaximumException;
import acdp.exceptions.ShutdownException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.Column_;
import acdp.internal.Table_;
import acdp.internal.Database_.Friend;
import acdp.tools.Setup;

/**
 * Defines the super class of a <em>custom table class</em>.
 * (See the section "Weakly and Strongly Typed" in the description of the
 * {@link Table} interface to learn about the motivation of a custom table
 * class.)
 * <p>
 * At a minimum, a concrete custom table class declares for each column an
 * instance variable (typically with the {@code final} modifier) referencing
 * that column, for example,
 * 
 * <pre>
 * final {@literal Column<String>} MY_STRING_COLUMN = {@link CL}.typeString();
 * final {@literal Column<String>} MY_STRING_ARRAY_COLUMN =
 *                          CL.typeArray(Scheme.OUTROW, 58, {@link ST}.beString());
 * final {@literal Column<Ref>} MY_REF_COLUMN = CL.typeRef()</pre>
 * 
 * In addition, the table must provide the <em>table definition</em> at the
 * time of object creation by invoking the {@link #initialize(Column...)
 * initialize} method, see example below.
 * (The table definition is the characteristic sequence of columns of a table,
 * see the description of the {@code Table} interface.)
 * <p>
 * If you plan to use the {@linkplain Setup Setup Tool} for creating the
 * database layout and all backing files of the database then make sure that
 * the table class is declared with a {@code public} access level modifier and
 * that it is annotated with the {@link Setup.Setup_Table @Setup_Table}
 * annotation.
 * Furthermore, the table class must have a no-arg {@code public} constructor
 * and all column declarations in the table class need to have a {@code public}
 * access level modifier and must be annotated with the
 * {@link Setup.Setup_Column @Setup_Column} annotation, for example,
 * 
 * <pre>
 * {@literal @Setup_Table({ "My String Column, My String Array Column, My Ref Column" })}
 * public MyTable {
 *    {@literal @Setup_Column("My String Column")}
 *    public final {@literal Column<String>} MY_STRING_COLUMN = CL.typeString();
 *    {@literal @Setup_Column("My String Array Column")}
 *    public final {@literal Column<String>} MY_STRING_ARRAY_COLUMN =
 *                          CL.typeArray(Scheme.OUTROW, 58, ST.beString());
 *    {@literal @Setup_Column(value = "My Ref Column", refdTable = "My Referenced Table")}
 *    public final {@literal Column<Ref>} MY_REF_COLUMN = CL.typeRef()
 *    
 *    public MyTable() {
 *    	initialize(MY_STRING_COLUMN, MY_STRING_ARRAY_COLUMN, MY_REF_COLUMN);
 *    }
 * }</pre>
 *
 * @author Beat Hoermann
 */
public abstract class CustomTable {
	/**
	 * The table object to which almost all of the methods provided by this
	 * class are delegated, never {@code null}.
	 */
	private Table table = null;
	/**
	 * The table's database.
	 */
	private CustomDatabase customDatabase = null;
	
	/**
	 * Initializes this custom table.
	 * <p>
	 * Invoke this method in the constructor of the concrete custom table class.
	 * If you do not call this method, almost all methods of this class will
	 * throw a {@code NullPointerException}.
	 * <p>
	 * The order of the columns in the specified array of columns must be the
	 * same as the order in which the corresponding column layouts appear in
	 * the database layout.
	 * 
	 * @param  columns The array of columns of the table, not allowed to be
	 *         {@code null} and not allowed to be empty.
	 *         The <em>table definition</em> is set equal to a copy of this
	 *         array.
	 *         
	 * @throws IllegalArgumentException If {@code columns} is {@code null} or
	 *         empty or if it contains a {@code null} value.
	 */
	protected void initialize(Column<?>... columns) throws
																		IllegalArgumentException {
		table = new Table_(Arrays.copyOf(columns, columns.length,
																				Column_[].class));
	}
	
	/**
	 * Sets the custom table's database.
	 * 
	 * @param customDatabase The custom table's database.
	 */
	final void setDatabase(CustomDatabase customDatabase) {
		this.customDatabase = customDatabase;
	}
	
	/**
	 * Friend-only access to the internal backing table object.
	 * (Since the friend resides in a different package, this method has to be
	 * public.)
	 * 
	 * @param  friend The friend, not allowed to be {@code null}.
	 * 
	 * @return The backing table, never {@code null}.
	 * 
	 * @throws NullPointerException If {@code friend} is {@code null}.
	 */
	public final Table_ getBackingTable(Friend friend) throws
																			NullPointerException {
		Objects.requireNonNull(friend);
		return (Table_) table;
	}
	
	/**
	 * See {@linkplain Table#name()}
	 * 
	 * @return The name of the table, never {@code null} and never an empty
	 *         string.
	 */
	public final String name() {
		return table.name();
	}
	
	/**
	 * See {@linkplain Table#info()}
	 * 
	 * @return The information object of the table, never {@code null}.
	 */
	protected TableInfo getInfo() {
		return table.info();
	}
	
	/**
	 * Returns the database.
	 * 
	 * @return The database.
	 */
	protected CustomDatabase getDatabase() {
		return customDatabase;
	}
	
	/**
	 * See {@linkplain Table#getColumns()}
	 * 
	 * @return An array of the columns of the table, never {@code null}.
	 *         The columns appear in the same order in which they appear in the
	 *         <em>table definition</em>.
	 */
	protected Column<?>[] getColumns() {
		return table.getColumns();
	}
	
	/**
	 * See {@linkplain Table#numberOfRows()}
	 * 
	 * @return The number of rows.
	 */
	protected long numberOfRows() {
		return table.numberOfRows();
	}
	
	/**
	 * See {@linkplain Table#insert(Object[])}
	 * 
	 * @param  values The values to insert into the row.
	 * 
	 * @return The reference to the inserted row.
	 * 
	 * @throws UnsupportedOperationException If the database is read-only.
	 * @throws NullPointerException If {@code values} is {@code null}.
	 * @throws IllegalArgumentException If the number of values is not equal to
	 *         the number of columns of the table.
	 *         This exception also happens if for at least one value the length
	 *         of the byte representation of the value (or one of the elements if
	 *         the value is an array value) exceeds the maximum length allowed
	 *         by the simple column type.
	 *         Furthermore, this exception happens if for at least one value the
	 *         value is a reference and the reference points to a row that does
	 *         not exist within the referenced table or if the reference points
	 *         to a row gap or if the value is an array of references and this
	 *         condition is satisfied for at least one of the references
	 *         contained in the array.
	 * @throws MaximumException If a new memory block in the VL file space must
	 *         be allocated and its file position exceeds the maximum allowed
	 *         position or if the maximum value of the reference counter of a
	 *         referenced row is exceeded or if the maximum number of rows for
	 *         this table is exceeded.
	 * @throws CryptoException If encryption fails.
	 *         This exception never happens if the WR database does not apply
	 *         encryption.
	 * @throws ShutdownException If this insert operation is a Kamikaze write
	 *         and the synchronization manager is shut down due to a closed
	 *         database.
	 * @throws ACDPException If this insert operation is called within a read
	 *         zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	protected Ref insert(Object... values) throws
								UnsupportedOperationException, NullPointerException,
								IllegalArgumentException, MaximumException,
								CryptoException, ShutdownException,
								ACDPException, UnitBrokenException, IOFailureException {
		return table.insert(values);
	}
	
	/**
	 * See {@linkplain Table#delete(Ref)}
	 * 
	 * @param  ref The reference to the row to delete from the table.
	 * 
	 * @throws UnsupportedOperationException If the database is read-only.
	 * @throws NullPointerException If {@code ref} is {@code null}.
	 * @throws DeleteConstraintException If the row to delete is referenced by at
	 *         least one foreign row.
	 * @throws IllegalReferenceException If the reference points to a row that
	 *         does not exist within the table.
	 *         This exception never occurs if the reference is a {@linkplain
	 *         Ref valid} reference.
	 * @throws ShutdownException If this delete operation is a Kamikaze write
	 *         and the synchronization manager is shut down due to a closed
	 *         database.
	 * @throws ACDPException If this delete operation is called within a read
	 *         zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	protected void delete(Ref ref) throws UnsupportedOperationException,
								NullPointerException, DeleteConstraintException,
								IllegalReferenceException, ShutdownException,
								ACDPException, UnitBrokenException, IOFailureException {
		table.delete(ref);
	}
	
	/**
	 * See {@linkplain Table#update(Ref, ColVal[])}
	 * 
	 * @param  ref The reference to the row to update.
	 * @param  colVals The array of column values.
	 * 
	 * @throws UnsupportedOperationException If the database is read-only.
	 * @throws NullPointerException If one of the arguments is equal to {@code
	 *         null} or if the array of column values contains an element equal
	 *         to {@code null}.
	 * @throws IllegalArgumentException If the array of column values contains
	 *         at least one column that is not a column of the table.
	 *         This exception also happens if for at least one value the length
	 *         of the byte representation of the value (or one of the elements
	 *         if the value is an array value) exceeds the maximum length
	 *         allowed by this type.
	 *         Furthermore, this exception happens if for at least one value the
	 *         value is a reference and the reference points to a row that does
	 *         not exist within the referenced table or if the reference points
	 *         to a row gap or if the value is an array of references and this
	 *         condition is satisfied for at least one of the references
	 *         contained in the array.
	 * @throws IllegalReferenceException If the specified reference points to
	 *         a row that does not exist within the table or if the reference
	 *         points to a row gap.
	 *         Such a situation cannot occur if {@code ref} is a {@linkplain Ref
	 *         valid} reference.
	 * @throws MaximumException If a new memory block in the VL file space must
	 *         be allocated and its file position exceeds the maximum allowed
	 *         position or if the maximum value of the reference counter of a
	 *         referenced row is exceeded.
	 * @throws CryptoException If encryption fails.
	 *         This exception never happens if encryption is not applied.
	 * @throws ShutdownException If this update operation is a Kamikaze write
	 *         and the synchronization manager is shut down due to a closed
	 *         database.
	 * @throws ACDPException If this update operation is called within a read
	 *         zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	protected void update(Ref ref, ColVal<?>... colVals) throws
								UnsupportedOperationException, NullPointerException,
								IllegalArgumentException, IllegalReferenceException,
								MaximumException, CryptoException, ShutdownException,
								ACDPException, UnitBrokenException, IOFailureException {
		table.update(ref, colVals);
	}
	
	/**
	 * See {@linkplain Table#updateAll(ColVal[])}
	 * 
	 * @param  colVals The array of column values.
	 * 
	 * @throws UnsupportedOperationException If the database is read-only.
	 * @throws NullPointerException If the array of column values is equal to
	 *         {@code null} or contains an element equal to {@code null}.
	 * @throws IllegalArgumentException If the array of column values contains
	 *         at least one column that is not a column of the table.
	 *         This exception also happens if for at least one value the length
	 *         of the byte representation of the value (or one of the elements
	 *         if the value is an array value) exceeds the maximum length
	 *         allowed by this type.
	 *         Furthermore, this exception happens if for at least one value the
	 *         value is a reference and the reference points to a row that does
	 *         not exist within the referenced table or if the reference points
	 *         to a row gap or if the value is an array of references and this
	 *         condition is satisfied for at least one of the references
	 *         contained in the array.
	 * @throws MaximumException If a new memory block in the VL file space must
	 *         be allocated and its file position exceeds the maximum allowed
	 *         position or if the maximum value of the reference counter of a
	 *         referenced row is exceeded.
	 * @throws CryptoException If encryption fails.
	 *         This exception never happens if encryption is not applied.
	 * @throws ShutdownException If this update operation is a Kamikaze write and
	 *         the synchronization manager is shut down due to a closed database.
	 * @throws ACDPException If this update operation is called within a read
	 *         zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	protected void updateAll(ColVal<?>... colVals) throws
								UnsupportedOperationException, NullPointerException,
								IllegalArgumentException, MaximumException,
								CryptoException, ShutdownException, ACDPException,
								UnitBrokenException, IOFailureException {
		table.updateAll(colVals);
	}
	
	/**
	 * See {@linkplain Table#updateAllSupplyValues(Column, ValueSupplier)}
	 * 
	 * @param  <T> The type of the column's values.
	 * 
	 * @param  column The column to be updated.
	 * @param  valueSupplier The value supplier.
	 * 
	 * @throws UnsupportedOperationException If the database is read-only.
	 * @throws NullPointerException If one of the arguments is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If the column is not a column of the
	 *         table.
	 *         This exception also happens if for a value returned by the value
	 *         supplier the length of the byte representation of the value (or
	 *         one of the elements if the value is an array value) exceeds the
	 *         maximum length allowed by this type.
	 *         Furthermore, this exception happens if the value is a reference
	 *         and the reference points to a row that does not exist within the
	 *         referenced table or if the reference points to a row gap or if
	 *         the value is an array of references and this condition is
	 *         satisfied for at least one of the references contained in the
	 *         array.
	 * @throws ImplementationRestrictionException If the number of row gaps is
	 *         greater than {@code Integer.MAX_VALUE}.
	 * @throws MaximumException If a new memory block in the VL file space must
	 *         be allocated and its file position exceeds the maximum allowed
	 *         position or if the maximum value of the reference counter of a
	 *         referenced row is exceeded.
	 * @throws CryptoException If encryption fails.
	 *         This exception never happens if encryption is not applied.
	 * @throws ShutdownException If this update operation is a Kamikaze write and
	 *         the synchronization manager is shut down due to a closed database.
	 * @throws ACDPException If this update operation is called within a read
	 *         zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	protected <T> void updateAllSupplyValues(Column<T> column,
													ValueSupplier<T> valueSupplier) throws
						UnsupportedOperationException, NullPointerException,
						IllegalArgumentException, ImplementationRestrictionException,
						MaximumException, CryptoException, ShutdownException,
						ACDPException, UnitBrokenException, IOFailureException {
		table.updateAllSupplyValues(column, valueSupplier);
	}
	
	/**
	 * See {@linkplain Table#updateAllChangeValues(Column, ValueChanger)}
	 * 
	 * @param  <T> The type of the column's values.
	 * 
	 * @param  column The column to be updated.
	 * @param  valueChanger The value changer.
	 * 
	 * @throws UnsupportedOperationException If the database is read-only.
	 * @throws NullPointerException If one of the arguments is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If the column is not a column of the
	 *         table.
	 *         This exception also happens if for a value returned by the value
	 *         changer the length of the byte representation of the value (or
	 *         one of the elements if the value is an array value) exceeds the
	 *         maximum length allowed by this type.
	 *         Furthermore, this exception happens if the value is a reference
	 *         and the reference points to a row that does not exist within the
	 *         referenced table or if the reference points to a row gap or if
	 *         the value is an array of references and this condition is
	 *         satisfied for at least one of the references contained in the
	 *         array.
	 * @throws ImplementationRestrictionException If the number of row gaps is
	 *         greater than {@code Integer.MAX_VALUE}.
	 * @throws MaximumException If a new memory block in the VL file space must
	 *         be allocated and its file position exceeds the maximum allowed
	 *         position or if the maximum value of the reference counter of a
	 *         referenced row is exceeded.
	 * @throws CryptoException If encryption or decryption fails.
	 *         This exception never happens if encryption is not applied.
	 * @throws ShutdownException If this update operation is a Kamikaze write and
	 *         the synchronization manager is shut down due to a closed database.
	 * @throws ACDPException If this update operation is called within a read
	 *         zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	protected <T> void updateAllChangeValues(Column<T> column,
													ValueChanger<T> valueChanger) throws
						UnsupportedOperationException, NullPointerException,
						IllegalArgumentException, ImplementationRestrictionException,
						MaximumException, CryptoException, ShutdownException,
						ACDPException, UnitBrokenException, IOFailureException {
		table.updateAllChangeValues(column, valueChanger);
	}
	
	/**
	 * See {@linkplain Table#get(Ref, Column[])}
	 * 
	 * @param  ref The reference to the row, not allowed to be {@code null}.
	 * @param  columns The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of this table.
	 *         If the array of columns is empty then this method behaves as if
	 *         the array of columns is identical to the value returned by
	 *         the {@link #getColumns} method.
	 * 
	 * @return The row, never {@code null}.
	 * 
	 * @throws NullPointerException If one of the arguments is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If at least one column in the specified
	 *         array of columns is not a column of the table.
	 * @throws IllegalReferenceException If the specified reference points to
	 *         a row that does not exist within the table or if the reference
	 *         points to a row gap.
	 *         Such a situation cannot occur if {@code ref} is a {@linkplain Ref
	 *         valid} reference.
    * @throws CryptoException If decryption fails.
	 *         This exception never happens if the database does not apply
	 *         encryption or if the database is an RO database.
	 * @throws ShutdownException If the file channel provider is shut down due
	 *         to a closed database.
	 *         This exception never happens if this get operation is executed
	 *         within a read zone or a unit or if the database is an RO database
	 *         and the operating mode is "memory packed" (-2) or "memory
	 *         unpacked" (-3).
	 * @throws IOFailureException If an I/O error occurs.
	 *         This exception never happens if the database is an RO database
	 *         and the operating mode is "memory unpacked" (-3).
	 */
	protected Row get(Ref ref, Column<?>... columns) throws
										NullPointerException, IllegalArgumentException,
										IllegalReferenceException, CryptoException,
										ShutdownException, IOFailureException {
		return table.get(ref, columns);
	}
	
	/**
	 * See {@linkplain Table#iterator(Column[])}
    * 
	 * @param  columns The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of this table.
	 *         If the array of columns is empty then the iterator behaves as if
	 *         the array of columns is identical to the value returned by
	 *         the {@link #getColumns} method.
	 *         
	 * @return The iterator, never {@code null}.
	 * 
	 * @throws NullPointerException If the array of columns is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If at least one column in the specified
	 *         array of columns is not a column of the table.
	 */
	protected TableIterator iterator(Column<?>... columns) throws
										NullPointerException, IllegalArgumentException {
		return table.iterator(columns);
	}
	
	/**
	 * See {@linkplain Table#iterator(Ref, Column[])}
	 * 
	 * @param  ref The reference, may be {@code null}.
	 *         The reference must be a {@linkplain Ref valid} reference.
	 * @param  columns The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of this table.
	 *         If the array of columns is empty then the iterator behaves as if
	 *         the array of columns is identical to the value returned by
	 *         the {@link #getColumns} method.
	 *         
	 * @return The iterator, never {@code null}.
	 * 
	 * @throws NullPointerException If one of the arguments is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If at least one column in the specified
	 *         array of columns is not a column of the table.
	 * @throws IllegalReferenceException If the specified reference points to
	 *         a row that does not exist within the table.
	 *         (The reference does not point to a row gap.)
	 *         Such a situation cannot occur if {@code ref} is a {@linkplain
	 *         Ref valid} reference.
	 */
	protected TableIterator iterator(Ref ref, Column<?>... columns) throws
										NullPointerException, IllegalArgumentException,
										IllegalReferenceException {
		return table.iterator(ref, columns);
	}
	
	/**
	 * See {@linkplain Table#rows(Column[])}
	 * 
	 * @param  columns The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of this table.
	 *         If the array of columns is empty then the stream behaves as if
	 *         the array of columns is identical to the value returned by
	 *         the {@link #getColumns} method.
	 *         
	 * @return The stream of the table's rows, never {@code null}.
	 *         All elements of the stream are non-null.
	 * 
	 * @throws NullPointerException If the array of columns is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If at least one column in the specified
	 *         array of columns is not a column of the table.
	 */
	protected Stream<Row> rows(Column<?>... columns) throws NullPointerException,
																		IllegalArgumentException {
		return table.rows(columns);
	}
	
	/**
	 * See {@linkplain Table#compactVL()}
	 * 
	 * @throws UnsupportedOperationException If this table has an RO store.
	 *         This can only be the case if the database is an RO database.
	 * @throws ACDPException If the WR database is read-only or if this method is
	 *         invoked within a read zone or a unit.
	 * @throws ShutdownException If the database is closed.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	protected void compactVL() throws UnsupportedOperationException,
								ACDPException, ShutdownException, IOFailureException {
		table.compactVL();
	}
	
	/**
	 * See {@linkplain Table#compactFL()}
	 * 
	 * @throws UnsupportedOperationException If this table has an RO store.
	 *         This can only be the case if the database is an RO database.
	 * @throws ImplementationRestrictionException If the number of row gaps in
	 *         this table or in at least one of the tables referencing that
	 *         table is greater than {@code Integer.MAX_VALUE}.
	 * @throws ACDPException If the WR database is read-only or if this method is
	 *         invoked within a read zone or a unit.
	 * @throws ShutdownException If the database is closed.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	protected void compactFL() throws UnsupportedOperationException,
								ImplementationRestrictionException,
								ACDPException, ShutdownException, IOFailureException {
		table.compactFL();
	}
	
	/**
	 * See {@linkplain Table#truncate()}
	 * 
	 * @throws UnsupportedOperationException If this table has an RO store.
	 *         This can only be the case if the database is an RO database.
	 * @throws DeleteConstraintException If the table contains at least one row
	 *         which is referenced by a foreign row.
	 *         This exception never happens if none of the tables in the database
	 *         reference this table.
	 * @throws ACDPException If the WR database is read-only or if this method is
	 *         invoked within a read zone or a unit.
	 * @throws ShutdownException If the database is closed.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	protected void truncate() throws UnsupportedOperationException,
													DeleteConstraintException, ACDPException,
													ShutdownException, IOFailureException {
		table.truncate();
	}
	
	/**
	 * See {@linkplain Table#zip(Path, int)}
	 * 
	 * @param  zipArchive The file path of the zip archive, not allowed to be
	 *         {@code null}.
	 * @param  level the compression level or -1 for the default compression
	 *         level.
	 *         Valid compression levels are greater than or equal to zero and
	 *         less than or equal to nine.
	 *         
	 * @throws UnsupportedOperationException If this table has an RO store.
	 *         This can only be the case if the database is an RO database.
	 * @throws NullPointerException If {@code zipArchive} is {@code null}.
	 * @throws IllegalArgumentException If the compression level is invalid.
	 * @throws ShutdownException If the database is closed.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	protected void zip(Path zipArchive, int level) throws
											UnsupportedOperationException,
											NullPointerException, IllegalArgumentException,
											ShutdownException, IOFailureException {
		table.zip(zipArchive, level);
	}
	
	/**
	 * See {@linkplain Table#toString()}
	 * 
	 * @return The name of the table, never {@code null} and never an empty
	 *         string.
	 */
	@Override
	public String toString() {
		return table.toString();
	}
}
