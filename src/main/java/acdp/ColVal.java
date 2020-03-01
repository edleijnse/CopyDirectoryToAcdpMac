/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp;

/**
 * Defines a column value pair.
 * Column value pairs are used when updating a value in a table.
 * A column value pair can be obtained by invoking the {@link Column#value}
 * method.
 * <p>
 * There should be no need for clients to implement this interface.
 *
 * @author Beat Hoermann
 * 
 * @param <T> The type of the values of the column.
 */
public interface ColVal<T> {
	/**
	 * Returns the column of this column value pair.
	 * 
	 * @return The column, never {@code null}.
	 */
	Column<T> column();
	
	/**
	 * Returns the value of this column value pair.
	 * 
	 * @return The value.
	 */
	T value();
}
