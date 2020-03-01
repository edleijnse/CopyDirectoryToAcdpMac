/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.stream.Stream;

import acdp.Information.TableInfo;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.DeleteConstraintException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.IllegalReferenceException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.MaximumException;
import acdp.exceptions.ShutdownException;
import acdp.exceptions.UnitBrokenException;
import acdp.types.Type;
import acdp.types.Type.Scheme;
import acdp.tools.Refactor;

/**
 * The table of a {@linkplain Database database}.
 * <p>
 * A table has columns and rows.
 * A column defines the type of the values a row can store in that column.
 * The configuration of a table is contained in the <em>table {@linkplain
 * acdp.misc.Layout layout}</em> which is part of the database layout.
 * <p>
 * Some important differences between an ACDP table and a relation of a
 * relational database are
 * 
 * <ul>
 *    <li>The collection of rows in a table is a sequence rather than a set.
 *        The same holds for the columns of a table.</li>
 * 	<li>ACDP tables natively persist arrays of values, rendering extra joins
 *        over join relations obsolete.</li>
 * 	<li>A row of an ACDP table can reference a row of the same or another
 *        ACDP table in direct analogy to an object referencing another object
 *        in main memory.</li>
 * </ul>
 * <p>
 * A row of a table can be viewed as a container housing the persisted data of
 * a particular object.
 * 
 * <h1>Weakly and Strongly Typed</h1>
 * An instance of this class is called a <em>weakly typed table</em> because
 * the first access to a specific column of the table can only be done by the
 * name of the column or by its position within the table definition, see the
 * {@link #getColumn(String)} and {@link #getColumns()} methods.
 * (The expression "table definition" is explained in section "Structure"
 * below.)
 * Changing the name of the column or changing the position of the column
 * within the table definition, without adapting the code on the client side,
 * breaks the client code.
 * Even worse, the compiler has no chance to detect such inconsistencies.
 * Another drawback is that there is no type safety at compile time regarding
 * the type of the values stored in a particular column of the table's rows.
 * <p>
 * In contrast to this, a <em>strongly typed table</em> is an instance of a
 * class extending the {@link acdp.design.CustomTable} class.
 * An elaborated custom table class can provide tailor-made methods for dealing
 * with all kinds of aspects of entity-specific persistence management.
 * A strongly typed table requires its columns to be instances of a class
 * implementing the {@link Column} interface which is parametrized with the type
 * of the column values.
 * Doing something with a particular column is simply doing it with the
 * appropriate column instance.
 * <p>
 * A weakly typed table is created from the information saved in the table
 * layout only.
 * Therefore, ACDP does not need to know of a custom table class in order to
 * create a weakly typed table.
 * Typically, a database is {@linkplain Database#open opened as a weakly typed
 * database}, that is, all tables of the database are created as weakly
 * typed tables, if there is no need to make a reference to the concrete
 * entities the tables represent, for example, in a generic tool which displays
 * the content of an arbitrary table.
 * 
 * <h1>Structure</h1>
 * The structure of a table comprises the names and types of its columns as well
 * as the characteristic sequence of its columns.
 * The characteristic sequence of columns is sometimes called the <em>table
 * definition</em>.
 * 
 * <h1>Name</h1>
 * The name of a table is saved in the table layout and is therefore not part
 * of the table structure.
 * Thus, it is possible to reuse the table's structure for a different content.
 * For instance, think of the entity "screw" and assume that we want to store
 * the diameter and length of all screws in stock.
 * Suppose we want to store this information separately for metal and wood
 * screws.
 * For this purpose we just define the table structure once and reuse it for
 * both screw types.
 * 
 * <h1>Store</h1>
 * Each table has a <em>store</em>.
 * A store implements various read/write database operations for reading data
 * from and writing data to the backing files of the store.
 * There are two types of stores: a writable <em>WR store</em> and a read-only
 * <em>RO store</em>.
 * The last one does not support any operations that modify the content of the
 * table.
 * Furthermore, data in the RO stores reside in a single compressed file and
 * can be loaded into main memory, provided that there is enough main memory
 * available.
 * All tables of the database have the same type of store.
 * Store specific parameters are saved in the <em>store layout</em> which is
 * part of the table layout.
 * 
 * <h1>Compacting the WR Store</h1>
 * Inserting, deleting and updating table data sooner or later fragments the
 * file spaces of the WR store.
 * A WR store has two file spaces: An FL file space for the fixed length data
 * of the rows and a VL file space for the {@linkplain Scheme#OUTROW outrow}
 * data of the rows.
 * Typically, compacting the FL file space, which can be done by invoking the
 * {@link #compactFL} method, is not necessary because unused memory blocks of
 * the FL file space, or short "row gaps", can be reused by the ACDP file space
 * management.
 * See the description of the {@link Ref} interface for further explanations.
 * In contrast to this, deallocated outrow data can't be reused and the total
 * size of unused memory blocks within the VL file space typically grows fast if
 * there are many write operations on outrow values.
 * This is why it makes sense to invoke the {@link #compactVL} method from time
 * to time.
 * To find out if it's worth doing so, consult the values returned by the {@link
 * Information.WRStoreInfo#vlUnused} and the {@link
 * Information.WRStoreInfo#vlUsed} methods.
 * 
 * <h1>References</h1>
 * Let {@code u} and {@code v} be two rows of the tables {@code U} and {@code
 * V}, respectively.
 * Furthermore, assume that {@code u} references {@code v} by storing in column
 * {@code c} of table {@code U} a {@linkplain Ref reference}.
 * The type of column {@code c} declares a reference to be a legal value for
 * this column.
 * However, the type does not specify the referenced table ({@code V} in our
 * example).
 * This kind of information is stored in the layout of table {@code U}, more
 * precisely, in the layout of column {@code c}.
 * (The layouts of the columns are part of the table layout.)
 * Note that <em>cyclic references</em> and even <em>self references</em> are
 * allowed: If table {@code T1} references table {@code T2}, that is, there is
 * a column of table {@code T1} referencing a column of table {@code T2}, and
 * table {@code T2} references table {@code T3} then {@code T3} is perfectly
 * allowed to reference {@code T1} or {@code T2} or even itself.
 * 
 * <h1>Exporting and Importing</h1>
 * Exporting the data of a table with a WR store can be done by invoking the
 * {@link #zip} method which adds the backing files of the table's store to a
 * zip archive.
 * To import the data of a table with a WR store from a zip archive, close the
 * database, unzip the zip archive and copy the data files to the right
 * location.
 * <p>
 * There should be no need for clients to implement this interface.
 *
 * @author Beat Hoermann
 */
public interface Table extends Iterable<Row> {
	/**
	 * Defines an iterator over the rows of a given table.
	 *
	 * @author Beat Hoermann
	 */
	public interface TableIterator extends Iterator<Row> {
		/**
	    * Returns {@code true} if and only if the iteration has more rows.
	    * (In other words, returns {@code true} if the {@link #next} method
	    * would return a row rather than throwing a {@code
	    * NoSuchElementException}.)
	    *
	    * @return The boolean value {@code true} if the iteration has more rows,
	    *         {@code false} if the iteration has no more rows.
	    *         
		 * @throws CryptoException If decryption fails.
		 *         This exception never happens if the database does not apply
		 *         encryption or if the database is an RO database.
		 * @throws ShutdownException If the database is closed.
		 *         This exception never happens if this iterator is executed
		 *         within a read zone or unit or if the database is an RO
		 *         database.
	    * @throws IOFailureException	If an I/O error occurs.
		 *         This exception never happens if the database is an RO database
		 *         and the operating mode is "memory unpacked" (-3).
	    */
		@Override
		boolean hasNext() throws CryptoException, ShutdownException,
																				IOFailureException;
		
		/**
	    * Returns the next row of the table.
		 * <p>
	    * In case of table data corruption this method may throw an exception of
		 * a type not listed below.
		 * If the database is a writable database then temporary table data
		 * corruption may be due to concurrent writes.
	    * Execute this method inside a <em>read zone</em> or a <em>unit</em> to
	    * ensure that no concurrent writes are taken place in the database while
	    * this method is being executed.
	    *
	    * @return The next row in the table.
	    * 
	    * @throws NoSuchElementException If the iteration has no more elements.
		 * @throws CryptoException If decryption fails.
		 *         This exception never happens if the database does not apply
		 *         encryption or if the database is an RO database.
		 * @throws ShutdownException If the database is closed.
		 *         This exception never happens if this iterator is executed
		 *         within a read zone or a unit or if the database is an RO
		 *         database and the operating mode is "memory packed" (-2) or
		 *         "memory unpacked" (-3).
	    * @throws IOFailureException	If an I/O error occurs.
		 *         This exception never happens if the database is an RO database
		 *         and the operating mode is "memory unpacked" (-3).
	    */
		@Override
	   Row next() throws NoSuchElementException, CryptoException,
														ShutdownException, IOFailureException;
		
	   /**
	    * The {@code remove} method is not supported by this iterator.
	    *
	    * @throws UnsupportedOperationException Always.
	    */
		@Override
	   void remove() throws UnsupportedOperationException;
	}
	
	/**
	 * Provides a new value that depends on the stored value.
	 * <p>
	 * The {@link #changeValue} method of a value changer is invoked by the
	 * {@link Table#updateAllChangeValues} and the {@link Refactor#modifyColumn}
	 * methods for each row of the table.
	 * <p>
	 * If you do not need to know the stored values then implement the {@link
	 * ValueSupplier} interface rather than this interface.
	 * 
	 * @param <T> The type of the value.
	 *
	 * @author Beat Hoermann
	 */
	@FunctionalInterface
	public interface ValueChanger<T> {
		/**
		 * Returns the new value for a particular column and for the next row of
		 * the table.
		 * <p>
		 * Depending on the concrete implementation, this method may throw an
		 * exception not mentioned here.
		 * 
		 * @param  storedValue The stored value.
		 *         Implementers of this method can expect this value to be
		 *         {@linkplain Type#isCompatible compatible} with the type of the
		 *         column.
		 * 
		 * @return The new value.
		 *         Implementers are asked to return a value that is {@linkplain
		 *         Type#isCompatible compatible} with the type of the column.
		 */
		T changeValue(Object storedValue);
	}
	
	/**
	 * Provides a new value that does not depend on the stored value.
	 * <p>
	 * The {@link #supplyValue} method of a value supplier is invoked by the
	 * {@link Table#updateAllSupplyValues} method for each row of the table.
	 * <p>
	 * Implement this interface in favor of the {@link ValueChanger} interface
	 * if you do not need to know the stored values.
	 * 
	 * @param <T> The type of the value.
	 *
	 * @author Beat Hoermann
	 */
	@FunctionalInterface
	public interface ValueSupplier<T> {
		/**
		 * Returns the value for a particular column and for the next row of the
		 * table.
		 * <p>
		 * Depending on the concrete implementation, this method may throw an
		 * exception not mentioned here.
		 * 
		 * @return The value.
		 *         Implementers are asked to return a value that is {@linkplain
		 *         Type#isCompatible compatible} with the type of the column.
		 */
		T supplyValue();
	}
	
	/**
	 * Returns the name of this table. 
	 * 
	 * @return The name of this table, never {@code null} and never an empty
	 *         string.
	 */
	String name();
	
	/**
	 * Returns the information object of this table.
	 * 
	 * @return The information object of this table, never {@code null}.
	 */
	TableInfo info();
	
	/**
	 * Returns the database.
	 * 
	 * @return The database, never {@code null}.
	 */
	Database getDatabase();
	
	/**
	 * Tests if this table has a column with the specified name.
	 * 
	 * @param  name The name.
	 * 
	 * @return The boolean value {@code true} if and only if this table has a
	 *         column with the specified name.
	 */
	boolean hasColumn(String name);
	
	/**
	 * Returns the column with the specified name.
	 * 
	 * @param  name The name of the column as written in the table layout.
	 * 
	 * @return The column with the specified name, never {@code null}.
	 * 
	 * @throws IllegalArgumentException If this table has no column with the
	 *         specified name.
	 */
	Column<?> getColumn(String name) throws IllegalArgumentException;
	
	/**
	 * Returns all columns of this table.
	 * 
	 * @return An array of the columns of this table, never {@code null} and
	 *         never empty.
	 *         The columns appear in the same order in which they appear in the
	 *         <em>table definition</em>.
	 */
	Column<?>[] getColumns();
	
	/**
	 * Returns the number of rows in this table.
	 * <p>
    * In case of table data corruption this read-only operation may return a
    * wrong result.
    * If the database is a writable database then temporary table data
    * corruption may be due to concurrent writes.
    * Invoke this read-only operation inside a <em>read zone</em> or a
    * <em>unit</em> to ensure that no concurrent writes are taken place in the
    * database while this operation is being executed.
	 * 
	 * @return The number of rows in this table.
	 */
	long numberOfRows();
	
	/**
	 * Inserts the specified values into this table and returns the reference
	 * to the newly created row.
	 * <p>
	 * The argument must be non-{@code null} and must satisfy the following
	 * conditions:
	 * 
	 * <ul>
	 * 	<li>The number of values is equal to the number of columns of the
	 *        table.</li>
	 *    <li>If v<sub>i</sub> denotes the i-th value then v<sub>i</sub> is
	 *        {@linkplain acdp.types.Type#isCompatible compatible}
	 *        with the type of the i-th column of this table.</li>
	 * </ul>
	 * <p>
	 * This method does not explicitly check this precondition.
	 * In any case, if this precondition is not satisfied then this method
	 * throws an exception, however, this may be an exception of a type not
	 * listed below.
	 * 
	 * @param  values The values to insert into the row.
	 * 
	 * @return The reference to the inserted row.
	 * 
	 * @throws UnsupportedOperationException If the database is read-only.
	 * @throws NullPointerException If {@code values} is {@code null}.
	 * @throws IllegalArgumentException If the number of values is not equal to
	 *         the number of columns of this table.
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
	 *         and the database is closed.
	 * @throws ACDPException If this insert operation is called within a read
	 *         zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	Ref insert(Object... values) throws UnsupportedOperationException,
								NullPointerException, IllegalArgumentException,
								MaximumException, CryptoException, ShutdownException,
								ACDPException, UnitBrokenException, IOFailureException;
	
	/**
	 * Deletes the referenced row from this table.
	 * <p>
	 * Note that deleting a non-existing row or a row which is referenced by at
	 * least one foreign row raises an exception.
	 * However, if there is any uncertainty about whether the row has already
	 * been deleted, then this situation can be solved as follows:
	 * 
	 * <pre>
	 * try {
	 *    delete(ref);
	 * } catch (IllegalReferenceException e) {
	 *    if (!e.rowGap()) {
	 *       throw e;
	 *    }
	 * }</pre>
	 *    
	 * In contrast to the isolated invocation of the {@code delete} method this
	 * code snippet has the effect that it does not raise an exception if the
	 * reference points to a row gap.
	 * (Consult the description of the {@link Ref} interface to learn about row
	 * gaps.)
	 * 
	 * @param  ref The reference to the row to delete from this table.
	 *         The value must be a {@linkplain Ref valid} reference.
	 * 
	 * @throws UnsupportedOperationException If the database is read-only.
	 * @throws NullPointerException If {@code ref} is {@code null}.
	 * @throws DeleteConstraintException If the row to delete is referenced by at
	 *         least one foreign row.
	 * @throws IllegalReferenceException If the specified reference points to
	 *         a row that does not exist within this table or if the reference
	 *         points to a row gap.
	 *         This exception never occurs if the reference is a {@linkplain
	 *         Ref valid} reference.
	 * @throws ShutdownException If this delete operation is a Kamikaze write
	 *         and the database is closed.
	 * @throws ACDPException If this delete operation is called within a read
	 *         zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	void delete(Ref ref) throws UnsupportedOperationException,
								NullPointerException, DeleteConstraintException,
								IllegalReferenceException, ShutdownException,
								ACDPException, UnitBrokenException, IOFailureException;
	/**
	 * Updates the values of the specified columns of the referenced row with the
	 * specified new values.
	 * <p>
	 * The new values must be {@linkplain acdp.types.Type#isCompatible
	 * compatible} with the type of their columns.
	 * This method does not explicitly check this precondition.
	 * In any case, if this precondition is not satisfied then this method
	 * throws an exception, however, this may be an exception of a type not
	 * listed below.
	 * 
	 * @param  ref The reference to the row to update.
	 * @param  colVals The array of column values.
	 * 
	 * @throws UnsupportedOperationException If the database is read-only.
	 * @throws NullPointerException If one of the arguments is equal to {@code
	 *         null} or if the array of column values contains an element equal
	 *         to {@code null}.
	 * @throws IllegalArgumentException If the array of column values contains
	 *         at least one column that is not a column of this table.
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
	 *         a row that does not exist within this table or if the reference
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
	 *         and the database is closed.
	 * @throws ACDPException If this update operation is called within a read
	 *         zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	void update(Ref ref, ColVal<?>... colVals) throws
								UnsupportedOperationException, NullPointerException,
								IllegalArgumentException, IllegalReferenceException,
								MaximumException, CryptoException, ShutdownException,
								ACDPException, UnitBrokenException, IOFailureException;
	/**
	 * For each row of this table updates the values stored in the specified
	 * columns with the specified new values.
	 * <p>
	 * The new values must be {@linkplain Type#isCompatible compatible} with
	 * the type of their columns.
	 * This method does not explicitly check this precondition.
	 * In any case, if this precondition is not satisfied then this method
	 * throws an exception, however, this may be an exception of a type not
	 * listed below.
	 * <p>
	 * Within a column, all rows are updated with the same value.
	 * Use the {@link #updateAllSupplyValues} or the {@link
	 * #updateAllChangeValues} methods to update each row with a different value.
	 * 
	 * @param  colVals The array of column values.
	 * 
	 * @throws UnsupportedOperationException If the database is read-only.
	 * @throws NullPointerException If the array of column values is equal to
	 *         {@code null} or contains an element equal to {@code null}.
	 * @throws IllegalArgumentException If the array of column values contains
	 *         at least one column that is not a column of this table.
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
	 * @throws ShutdownException If this update operation is a Kamikaze write
	 *         and the database is closed.
	 * @throws ACDPException If this update operation is called within a read
	 *         zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	void updateAll(ColVal<?>... colVals) throws UnsupportedOperationException,
								NullPointerException, IllegalArgumentException,
								MaximumException, CryptoException, ShutdownException,
								ACDPException, UnitBrokenException, IOFailureException;
	/**
	 * For each row of this table updates the value stored in the specified
	 * column with a value obtained from the specified value supplier.
	 * <p>
	 * If a value returned by the value supplier is not {@linkplain
	 * Type#isCompatible compatible} with the type of the column then this
	 * method throws an exception, however, this may be an exception of a type
	 * not listed below.
	 * <p>
	 * Depending on the concrete implementation of the value supplier, this
	 * method may throw an exception that is different from the listed
	 * exceptions.
	 * <p>
	 * To update all rows of a column with the same value, the {@link #updateAll}
	 * method can be used instead of this method.
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
	 * @throws ShutdownException If this update operation is a Kamikaze write
	 *         and the database is closed.
	 * @throws ACDPException If this update operation is called within a read
	 *         zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	<T> void updateAllSupplyValues(Column<T> column,
														ValueSupplier<T> valueSupplier) throws 
						UnsupportedOperationException, NullPointerException,
						IllegalArgumentException, ImplementationRestrictionException,
						MaximumException, CryptoException, ShutdownException,
						ACDPException, UnitBrokenException, IOFailureException;
	/**
	 * For each row of this table updates the value stored in the specified
	 * column with a value obtained from the specified value changer.
	 * <p>
	 * If a value returned by the value changer is not {@linkplain
	 * Type#isCompatible compatible} with the type of the column then this
	 * method throws an exception, however, this may be an exception of a type
	 * not listed below.
	 * <p>
	 * Depending on the concrete implementation of the value changer, this
	 * method may throw an exception that is different from the listed
	 * exceptions.
	 * <p>
	 * To update all rows of a column with the same value, the {@link #updateAll}
	 * method can be used instead of this method.
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
	 * @throws ShutdownException If this update operation is a Kamikaze write
	 *         and the database is closed.
	 * @throws ACDPException If this update operation is called within a read
	 *         zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	<T> void updateAllChangeValues(Column<T> column,
														ValueChanger<T> valueChanger) throws 
						UnsupportedOperationException, NullPointerException,
						IllegalArgumentException, ImplementationRestrictionException,
						MaximumException, CryptoException, ShutdownException,
						ACDPException, UnitBrokenException, IOFailureException;
	/**
	 * Returns the referenced row of this table.
	 * <p>
    * In case of table data corruption this read-only operation may throw an
    * exception of a type not listed below.
    * If the database is a writable database then temporary table data
    * corruption may be due to concurrent writes.
    * Invoke this read-only operation inside a <em>read zone</em> or a
    * <em>unit</em> to ensure that no concurrent writes are taken place in the
    * database while this operation is being executed.
	 * 
	 * @param  ref The reference to the row, not allowed to be {@code null}.
	 *         The value must be a {@linkplain Ref valid} reference.
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
	 *         array of columns is not a column of this table.
	 * @throws IllegalReferenceException If the specified reference points to
	 *         a row that does not exist within this table or if the reference
	 *         points to a row gap.
	 *         This exception never occurs if the reference is a {@linkplain
	 *         Ref valid} reference.
    * @throws CryptoException If decryption fails.
	 *         This exception never happens if the database does not apply
	 *         encryption or if the database is an RO database.
	 * @throws ShutdownException If the database is closed.
	 *         This exception never happens if this get operation is executed
	 *         within a read zone or a unit or if the database is an RO database
	 *         and the operating mode is "memory packed" (-2) or "memory
	 *         unpacked" (-3).
	 * @throws IOFailureException If an I/O error occurs.
	 *         This exception never happens if the database is an RO database
	 *         and the operating mode is "memory unpacked" (-3).
	 */
	Row get(Ref ref, Column<?>... columns) throws NullPointerException,
								IllegalArgumentException, IllegalReferenceException,
								CryptoException, ShutdownException, IOFailureException;
	/**
	 * Returns an iterator over the rows of this table.
	 * <p>
    * In case of table data corruption this method may throw an exception of a
    * type not listed below.
    * If the database is a writable database then temporary table data
    * corruption may be due to concurrent writes.
    * Execute this method and the whole iteration inside a <em>read zone</em> or
    * a <em>unit</em> to ensure that no concurrent writes take place in the
    * database while the iteration is being executed.
    * <p>
    * Executing the {@link #insert}, the {@link #delete} and the {@link #update}
    * operations within the same thread can be done without damaging the
    * internal state of the iterator, although the effects of such write
    * operations may not be seen as the iteration continous due to caching
    * effects.
    * (Recall that a write operation cannot be executed within a read zone.)
    * <p>
    * Consider using one of the {@code updateAll} perations if you want to
    * update each row of this table in a single step.
    * Furthermore, if you want to delete all rows of a table in a single step,
    * consider using the {@link #truncate} operation.
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
	 *         array of columns is not a column of this table.
	 */
	TableIterator iterator(Column<?>... columns) throws NullPointerException,
																		IllegalArgumentException;
	/**
	 * Returns an iterator over the rows of this table starting with the first
	 * row that comes <em>after</em> the referenced row.
	 * <p>
	 * If the reference is equal to {@code null} then the first row returned by
	 * the iterator will be equal to the first row of this table, provided that
	 * this table is not empty.
	 * Otherwise, the first row returned by the iterator will be equal to the row
	 * that immediately follows the referenced row, provided that the reference
	 * does not point to the last row of this table.
	 * (If the reference is {@code null} and this table is empty or if the
	 * reference points to the last row of this table then invoking the
	 * iterator's {@code hasNext} method returns {@code false}.)
	 * <p>
	 * This method can be used to split an iteration from one large iteration
	 * into several smaller ones.
	 * <p>
	 * The remarks in the {@linkplain #iterator(Column...)
	 * iterator(Column&lt;T>...) method description} also apply for this iterator
	 * method.
	 * 
	 * @param  ref The row reference, may be {@code null}.
	 *         The value must be a {@linkplain Ref valid} reference.
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
	 *         array of columns is not a column of this table.
	 * @throws IllegalReferenceException If the specified reference points to
	 *         a row that does not exist within this table.
	 *         (The reference does not point to a row gap.)
	 *         Such a situation cannot occur if {@code ref} is a {@linkplain
	 *         Ref valid} reference.
	 */
	TableIterator iterator(Ref ref, Column<?>... columns) throws
											NullPointerException, IllegalArgumentException,
											IllegalReferenceException;
	/**
	 * Returns an iterator over the rows of this table, semantically identical to
	 * the {@link #iterator(Column...) iterator(Column&lt;?>...)} method invoked
	 * with no arguments.
	 * <p>
	 * The sequence of columns of the rows returned by this iterator is identical
	 * to the <em>table definition</em>.
	 * <p>
	 * The remarks in the {@linkplain #iterator(Column...)
	 * iterator(Column&lt;T>...) method description} also apply for this iterator
	 * method.
	 *         
	 * @return The iterator, never {@code null}.
	 */
	@Override
	TableIterator iterator();
	
	/**
	 * Returns a {@link Stream} of this table's rows.
	 * <p>
	 * The returned stream is a sequential, ordered stream which may be turned
	 * into a parallel, unordered stream by invoking the stream's {@link
	 * Stream#parallel() parallel()} and/or {@link Stream#unordered()
	 * unordered()} methods, as explained in the description of the {@code
	 * Stream} class.
	 * Furthermore, the stream's underlying {@link Spliterator} is
	 * <em>late-binding</em>.
	 * <p>
	 * For a writable database that is operated in an environment that allows
	 * concurrent writes, at least the <em>terminal operation</em> of the
	 * <em>stream pipeline</em> must be executed inside a <em>read zone</em> or
	 * a <em>unit</em>.
	 * 
	 * @param  columns The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of this table.
	 *         If the array of columns is empty then the stream behaves as if
	 *         the array of columns is identical to the value returned by
	 *         the {@link #getColumns} method.
	 *         
	 * @return The stream of this table's rows, never {@code null}.
	 *         All elements of the stream are non-null.
	 * 
	 * @throws NullPointerException If the array of columns is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If at least one column in the specified
	 *         array of columns is not a column of this table.
	 */
	Stream<Row> rows(Column<?>... columns) throws NullPointerException,
																		IllegalArgumentException;
	/**
	 * Eliminates the unused memory blocks within the VL file space of
	 * this table and truncates the corresponding data file.
	 * <p>
	 * Note that the database must be a writable database.
	 * Note also that this method has no effect if this table's WR store has no
	 * VL file space.
	 * <p>
	 * Since the effects of this method cannot be undone, you may want to
	 * {@linkplain #zip backup this table} before executing this method.
	 * <p>
	 * The implementation of this method is such that any access to table data is
	 * blocked while this method is running.
	 * It is therefore safe to invoke this service operation anytime during
	 * a session.
	 * (Service operations are described in section "Service Operations" of
	 * the {@link Database} interface description.)
	 * 
	 * @throws UnsupportedOperationException If this table has an RO store.
	 *         This can only be the case if the database is an RO database.
	 * @throws ACDPException If the WR database is read-only or if this method is
	 *         invoked within a read zone or a unit.
	 * @throws ShutdownException If the database is closed.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	void compactVL() throws UnsupportedOperationException, ACDPException,
														ShutdownException, IOFailureException;
	/**
	 * Eliminates the unused memory blocks within the FL file space of this table
	 * and truncates the corresponding data file.
	 * <p>
	 * Note that the database must be a writable WR database.
	 * <p>
	 * Since the effects of this method on this table and on any other tables
	 * referencing this table cannot be undone, you may want to {@linkplain #zip
	 * backup those tables} or even {@linkplain Database#zip backup the whole
	 * database} before executing this method.
	 * <p>
	 * The implementation of this method is such that any access to table data is
	 * blocked while this method is running.
	 * It is therefore safe to invoke this service operation anytime during
	 * a session.
	 * (Service operations are described in section "Service Operations" of
	 * the {@link Database} interface description.)
	 * <p>
	 * As mentioned under "Compacting the WR Store" in the {@linkplain Table
	 * interface description} compacting the FL file space is usually not
	 * necessary.
	 * <p>
	 * <em>Note that existing references may be invalidated by compacting the FL
	 * file space of a table.</em>
	 * 
	 * @throws UnsupportedOperationException If this table has an RO store.
	 *         This can only be the case if the database is an RO database.
	 * @throws ImplementationRestrictionException If the number of unused memory
	 *         blocks in this table or in at least one of the tables referencing
	 *         that table is greater than {@code Integer.MAX_VALUE}.
	 * @throws ACDPException If the WR database is read-only or if this method is
	 *         invoked within a read zone or a unit.
	 * @throws ShutdownException If the database is closed.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	void compactFL() throws UnsupportedOperationException,
										ImplementationRestrictionException, ACDPException,
														ShutdownException, IOFailureException;
	/**
	 * Deletes all rows of this table and truncates the corresponding data files.
	 * <p>
	 * Note that if this table contains at least one row which is referenced by a
	 * foreign row then this method throws a {@code DeleteConstraintException}.
	 * No changes are made to the database in such a situation.
	 * <p>
	 * Note also that the database must be a writable database.
	 * <p>
	 * Since the effects of this method on this table and on any other tables
	 * referenced by this table cannot be undone, you may want to {@linkplain
	 * #zip backup those tables} or even {@linkplain Database#zip backup the
	 * whole database} before executing this method.
	 * <p>
	 * The implementation of this method is such that any access to table data is
	 * blocked while this method is running.
	 * It is therefore safe to invoke this service operation anytime during
	 * a session.
	 * (Service operations are described in section "Service Operations" of
	 * the {@link Database} interface description.)
	 * 
	 * @throws UnsupportedOperationException If this table has an RO store.
	 *         This can only be the case if the database is an RO database.
	 * @throws DeleteConstraintException If this table contains at least one row
	 *         which is referenced by a foreign row.
	 *         This exception never happens if none of the tables in the database
	 *         reference this table.
	 * @throws ACDPException If the WR database is read-only or if this method is
	 *         invoked within a read zone or a unit.
	 * @throws ShutdownException If the database is closed.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	void truncate() throws UnsupportedOperationException,
													DeleteConstraintException, ACDPException,
														ShutdownException, IOFailureException;
	/**
	 * Creates a zip archive with the specified path and adds the data files of
	 * this table to that zip archive.
	 * <p>
	 * The files appear in the resulting zip archive without any path
	 * information, just with their file names.
	 * <p>
	 * Note that this table must have a WR store.
	 * <p>
	 * The implementation of this method is such that concurrent writes can not
	 * take place while this method is running.
	 * It is therefore safe to invoke this service operation anytime during a
	 * session.
	 * (Service operations are described in section "Service Operations" of
	 * the {@link Database} interface description.)
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
	void zip(Path zipArchive, int level) throws UnsupportedOperationException,
											NullPointerException, IllegalArgumentException,
														ShutdownException, IOFailureException;
	
	/**
	 * As the {@link #name} method, returns the name of this table. 
	 * 
	 * @return The name of this table, never {@code null} and never an empty
	 *         string.
	 */
	@Override
	String toString();
}
