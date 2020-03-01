/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp;

import java.util.Iterator;

/**
 * Defines a data structure housing some or all values stored in a particular
 * row of a {@link Table}.
 * <p>
 * An instance of {@code Row} has a sequence of columns {@code c}<sub>1</sub>,
 * {@code c}<sub>2</sub>, ... , {@code c}<sub>n</sub> with
 * {@code n} &ge; 1 and consists of a sequence of values {@code v}<sub>1</sub>,
 * {@code v}<sub>2</sub>, ... , {@code v}<sub>n</sub> such that
 * {@code v}<sub>i</sub> is the value stored in the table's row in the column
 * {@code c}<sub>i</sub>.
 * <p>
 * The sequence of columns is given by the {@code columns} argument of those
 * methods that return rows such as the {@link Table#get(Ref, Column...)
 * Table.get(Ref ref, Column&lt;?>... columns)} method, where an empty column
 * array means the {@linkplain Table table definition}, hence, the table's
 * characteristic sequence of columns.
 * <p>
 * It can be assumed that {@code v}<sub>i</sub> is {@linkplain
 * acdp.types.Type#isCompatible compatible} with the type of column {@code
 * c}<sub>i</sub>.
 * <p>
 * Note that a row is {@linkplain Iterable iterable}.
 * The order in which the values of an iterated row are returned is naturally
 * defined by its sequence of columns.
 * <p>
 * There should be no need for clients to implement this interface.
 *
 * @author Beat Hoermann
 */
public interface Row extends Iterable<Object> {
	/**
	 * Returns the table this row belongs to.
	 * 
	 * @return The table, never {@code null}.
	 */
	Table getTable();
	
	/**
	 * The sequence of columns of this row.
	 * 
	 * @return The columns, never {@code null} and never an empty array.
	 */
	Column<?>[] getColumns();
	
	/**
	 * Returns the value for the specified column.
	 * 
	 * @param  <T> The value type of the column.
	 * 
	 * @param  column The column, not allowed to be {@code null}.
	 *         The column must be a column contained in the column array returned
	 *         by the {@link #getColumns} method.
	 * 
	 * @return The value of this row for the specified column.
	 * 
	 * @throws IllegalArgumentException If {@code column} is {@code null} or if
	 *         the row has no value for the specified column.
	 */
	<T> T get(Column<T> column) throws IllegalArgumentException;
	
	/**
	 * Returns the value with the specified index.
	 * 
	 * @param  index The index, must be greater than or equal to zero and less
	 *         than the length of the column array returned by the {@link
	 *         #getColumns} method.
	 *         
	 * @return The value with the specified index.
	 * 
	 * @throws IndexOutOfBoundsException If the specified index is less than
	 *         zero or greater than the row's number of columns.
	 */
	Object get(int index) throws IndexOutOfBoundsException;
	
	/**
	 * The reference to this row within the table.
	 * 
	 * @return The reference, never {@code null}.
	 */
	Ref getRef();
	
	/**
	 * Returns an iterator over the values of this row in proper sequence.
	 * 
	 * @return The iterator over the values of this row, never {@code null}.
	 */
	@Override
	Iterator<Object> iterator();
}
