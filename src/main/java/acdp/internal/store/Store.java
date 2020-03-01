/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store;

import java.util.Spliterator;
import java.util.stream.Stream;

import acdp.Column;
import acdp.ColVal;
import acdp.Ref;
import acdp.Row;
import acdp.Table.TableIterator;
import acdp.Table.ValueChanger;
import acdp.Table.ValueSupplier;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.IllegalReferenceException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.MaximumException;
import acdp.exceptions.ShutdownException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.Column_;
import acdp.internal.Ref_;
import acdp.internal.Row_;
import acdp.internal.Table_;
import acdp.internal.store.ro.ROStore;
import acdp.internal.store.wr.WRStore;
import acdp.internal.types.Type_;
import acdp.types.Type;

/**
 * A concrete store implements the various read/write database operations
 * operating on the data saved in the table that is associated with the store.
 * Since the read/write database operations assume a particular storage layout
 * for reading and writing values of a certain {@linkplain Type_ column type},
 * the store implicitly specifies the storage layout.
 * <p>
 * This class is the super class of two concrete stores, namely the writable
 * {@link WRStore} and the read-only {@link ROStore}.
 *
 * @author Beat Hoermann
 */
public abstract class Store {
	/**
	 * The information whether this store can insert, delete and update data,
	 * not just read data.
	 */
	protected final boolean writable;
	/**
	 * The table associated with this store.
	 */
	public final Table_ table;
	
	/**
	 * The read-only operation of this store.
	 */
	private ReadOp readOp;
	
	/**
	 * The constructor.
	 * 
	 * @param  table The table associated with this store.
	 * 
	 * @throws NullPointerException if {@code table} is {@code null}.
	 */
	protected Store(Table_ table) throws NullPointerException {
		this.writable = table.db().isWritable();
		this.table = table;
		this.readOp = null;
	}
	
	/**
	 * Sets the read-only operation.
	 * This method must be invoked by a concrete store implementation before a
	 * client invokes the {@link #get}, {@link #iterator} or {@link #rows}
	 * methods.
	 * 
	 * @param readOp The read-only operation, not allowed to be {@code null}.
	 */
	protected final void setReadOp(ReadOp readOp) {
		this.readOp = readOp;
	}
	
	/**
	 * Returns the number of row gaps.
	 * <p>
    * In case of table data corruption this read-only operation may return a
    * wrong result.
    * If the database is a writable database then temporary table data
    * corruption may be due to concurrent writes.
    * Execute this read-only operation inside a <em>read zone</em> or a
    * <em>unit</em> to ensure that no concurrent writes are taken place in the
    * database while this operation is being executed.
	 * 
	 * @return The number of row gaps.
	 */
	protected abstract long numberOfRowGaps();
	
	/**
	 * Converts the specified reference to a row index.
	 * 
	 * @param  ref The reference, not allowed to be {@code null}.
	 * 
	 * @return The row index.
	 *         The row index of the first row is equal to 1.
	 * 
	 * @throws IllegalReferenceException If the specified reference points to a
	 *         row that does not exist within the table.
	 *         (The reference does not point to a row gap.)
	 *         Such a situation cannot occur if {@code ref} is a {@linkplain
	 *         Ref valid} reference.
	 */
	public abstract long refToRi(Ref_ ref) throws IllegalReferenceException;
	
	/**
	 * Returns the number of rows in the table.
	 * <p>
    * In case of table data corruption this read-only operation may return a
    * wrong result.
    * If the database is a writable database then temporary table data
    * corruption may be due to concurrent writes.
    * Execute this read-only operation inside a <em>read zone</em> or a
    * <em>unit</em> to ensure that no concurrent writes are taken place in the
    * database while this operation is being executed.
	 * 
	 * @return The number of rows.
	 */
	public abstract long numberOfRows();
	
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
	 *         zone or an ACDP zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	public abstract Ref_ insert(Object[] values) throws
								UnsupportedOperationException, NullPointerException,
								IllegalArgumentException, MaximumException,
								CryptoException, ShutdownException, ACDPException,
								UnitBrokenException, IOFailureException;
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
	 * 
	 * @param  ref The reference to the row to delete from the table.
	 * 
	 * @throws UnsupportedOperationException If the database is read-only.
	 * @throws NullPointerException If {@code ref} is {@code null}.
	 * @throws IllegalArgumentException If the row to delete is referenced by at
	 *         least one foreign row.
	 * @throws IllegalReferenceException  If the reference points to a row that
	 *         does not exist within the table or if the reference points to a
	 *         row gap.
	 *         This exception never occurs if the reference is a {@linkplain Ref
	 *         valid} reference.
	 * @throws ShutdownException If this delete operation is a Kamikaze write
	 *         and the synchronization manager is shut down due to a closed
	 *         database.
	 * @throws ACDPException If this delete operation is called within a read
	 *         zone or an ACDP zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	public abstract void delete(Ref_ ref) throws
								UnsupportedOperationException, NullPointerException,
								IllegalArgumentException, IllegalReferenceException,
								ShutdownException, ACDPException, UnitBrokenException,
								IOFailureException;
	/**
	 * Updates the values of the specified columns of the referenced row with the
	 * specified new values.
	 * <p>
	 * The new values must be {@linkplain Type#isCompatible compatible} with
	 * the type of their columns.
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
	 *         zone or an ACDP zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	public abstract void update(Ref_ ref, ColVal<?>[] colVals) throws
								UnsupportedOperationException, NullPointerException,
								IllegalArgumentException, IllegalReferenceException,
								MaximumException, CryptoException, ShutdownException,
								ACDPException, UnitBrokenException, IOFailureException;
	/**
	 * For each row of the table this method updates the values stored in the
	 * specified columns with the specified new values.
	 * <p>
	 * The new values must be {@linkplain Type#isCompatible compatible} with
	 * the type of their columns.
	 * This method does not explicitly check this precondition.
	 * In any case, if this precondition is not satisfied then this method
	 * throws an exception, however, this may be an exception of a type not
	 * listed below.
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
	 *         zone or an ACDP zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	public abstract void updateAll(ColVal<?>[] colVals) throws
								UnsupportedOperationException, NullPointerException,
								IllegalArgumentException, MaximumException,
								CryptoException, ShutdownException,
								ACDPException, UnitBrokenException, IOFailureException;
	/**
	 * For each row of the table this method updates the value stored in the
	 * specified column with a value obtained from the specified value supplier.
	 * <p>
	 * If a value returned by the value supplier is not {@linkplain
	 * Type#isCompatible compatible} with the type of the column then this
	 * method throws an exception, however, this may be an exception of a type
	 * not listed below.
	 * <p>
	 * Depending on the concrete implementation of the value supplier, this
	 * method may throw an exception that is different from the listed
	 * exceptions.
	 * 
	 * @param  col The column to be updated.
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
	 *         zone or an ACDP zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	public abstract void updateAllSupplyValues(Column_<?> col,
													ValueSupplier<?> valueSupplier) throws 
						UnsupportedOperationException, NullPointerException,
						IllegalArgumentException, ImplementationRestrictionException,
						MaximumException, CryptoException, ShutdownException,
						ACDPException, UnitBrokenException, IOFailureException;
	/**
	 * For each row of the table this method updates the value stored in the
	 * specified column with a value obtained from the specified value changer.
	 * <p>
	 * If a value returned by the value changer is not {@linkplain
	 * Type#isCompatible compatible} with the type of the column then this
	 * method throws an exception, however, this may be an exception of a type
	 * not listed below.
	 * <p>
	 * Depending on the concrete implementation of the value changer, this
	 * method may throw an exception that is different from the listed
	 * exceptions.
	 * 
	 * @param  col The column to be updated.
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
	 *         zone or an ACDP zone.
	 * @throws UnitBrokenException If recording before data fails or if the unit
	 *         was broken before.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	public abstract void updateAllChangeValues(Column_<?> col,
														ValueChanger<?> valueChanger) throws 
						UnsupportedOperationException, NullPointerException,
						IllegalArgumentException, ImplementationRestrictionException,
						MaximumException, CryptoException, ShutdownException,
						ACDPException, UnitBrokenException, IOFailureException;
	
	/**
	 * Returns the referenced row.
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
	 * @param  cols The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of the store's table.
	 *         If the array of columns is empty then this method behaves as if
	 *         the array of columns is identical to the table definition.
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
	 *         This exception never happens if this get operation is
	 *         synchronized or if the database is an RO database and the
	 *         operating mode is "memory packed" or "memory unpacked".
	 * @throws IOFailureException If an I/O error occurs.
	 *         This exception never happens if the database is an RO database
	 *         and the operating mode is "memory unpacked".
	 */
	public final Row_ get(Ref_ ref, Column<?>[] cols) throws
											NullPointerException, IllegalArgumentException,
											IllegalReferenceException, CryptoException,
											ShutdownException, IOFailureException {
		return readOp.get(ref, cols);
	}
	
	/**
	 * Returns an iterator over the rows of the table.
	 * <p>
    * In case of table data corruption this method may throw an exception of a
    * type not listed below.
    * If the database is a writable database then temporary table data
    * corruption may be due to concurrent writes.
    * Execute this method and the whole iteration inside a <em>read zone</em> or
    * a <em>unit</em> to ensure that no concurrent writes take place in the
    * database while the iteration is being executed.
	 * 
	 * @param  cols The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of the store's table.
	 *         If the array of columns is empty then the iterator behaves as if
	 *         the array of columns is identical to the table definition.
	 *         
	 * @return The iterator, never {@code null}.
	 * 
	 * @throws NullPointerException If the array of columns is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If at least one column in the specified
	 *         array of columns is not a column of the table.
	 */
	public final TableIterator iterator(Column<?>[] cols) throws
										NullPointerException, IllegalArgumentException {
		return readOp.iterator(0, cols);
	}
	
	/**
	 * Returns an iterator over the rows of this table starting with the first
	 * row that comes immediately <em>after</em> the referenced row.
	 * <p>
	 * If the reference is equal to {@code null} then the first row returned by
	 * the iterator will be equal to the first row of the table, provided that
	 * the table is not empty.
	 * Otherwise, the first row returned by the iterator will be equal to the row
	 * that immediately follows the referenced row, provided that the reference
	 * does not point to the last row of this table.
	 * (If the reference is {@code null} and the table is empty or if the
	 * reference points to the last row of the table then invoking the iterator's
	 * {@code hasNext} method returns {@code false}.)
	 * <p>
	 * This method can be used to split an iteration from one large iteration
	 * into several smaller ones.
	 * <p>
	 * Take note of the second paragraph of the {@link #iterator(Column[])
	 * iterator(Col[])} method description which also apply for this iterator
	 * method.
	 * 
	 * @param  ref The reference, may be {@code null}.
	 *         The reference must be a {@linkplain Ref valid} reference.
	 * @param  cols The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of the store's table.
	 *         If the array of columns is empty then the iterator behaves as if
	 *         the array of columns is identical to the table definition.
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
	public final TableIterator iterator(Ref_ ref, Column<?>[] cols) throws
											NullPointerException, IllegalArgumentException,
											IllegalReferenceException {
		if (ref == null)
			return readOp.iterator(0, cols);
		else {
			// Note that the row index of the first row is equal to 1.
			return readOp.iterator(refToRi(ref), cols);
		}
	}
	
	/**
	 * Returns a {@link Stream} of the table's rows.
	 * <p>
	 * The returned stream is a sequential, ordered stream which may be turned
	 * into a parallel, unordered stream by invoking the stream's {@link
	 * Stream#parallel() parallel()} and/or {@link Stream#unordered()
	 * unordered()} methods, as explained in the description of the {@code
	 * Stream} class.
	 * <p>
	 * For a writable database that is operated in an environment that allows
	 * concurrent writes, at least the <em>terminal operation</em> of the
	 * <em>stream pipeline</em> must be executed inside a <em>read zone</em> or
	 * a <em>unit</em>.
	 * (The stream's underlying {@link Spliterator} is <em>late-binding</em>.)
	 * 
	 * @param  cols The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of the store's table.
	 *         If the array of columns is empty then the stream behaves as if
	 *         the array of columns is identical to the table definition.
	 *         
	 * @return The stream of the table's rows, never {@code null}.
	 *         All elements of the stream are non-null.
	 * 
	 * @throws NullPointerException If the array of columns is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If at least one column in the specified
	 *         array of columns is not a column of the table.
	 */
	public final Stream<Row> rows(Column<?>[] cols) throws NullPointerException,
																		IllegalArgumentException {
		return readOp.rows(cols);
	}
}
