/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp;

import acdp.Information.ColumnInfo;
import acdp.types.Type;

/**
 * The column of a {@linkplain Table table}.
 * A column defines the type of values that a {@linkplain Row row} can store
 * in a particular column.
 * Columns are created with the factory methods of the {@link acdp.design.CL}
 * class.
 * <p>
 * There should be no need for clients to implement this interface.
 *
 * @author Beat Hoermann
 * 
 * @param <T> The type of the values of the column.
 */
public interface Column<T> {
	/**
	 * Returns the name of this column.
	 * 
	 * @return The name of this column, never {@code null} and never an empty
	 *         string.
	 */
	String name();
	
	/**
	 * Returns the information object of this column.
	 * 
	 * @return The information object of this column, never {@code null}.
	 */
	ColumnInfo info();
	
	/**
	 * Creates a column value pair from this column and the specified value.
	 * 
	 * @param  value The value.
	 *         The value must be {@linkplain Type#isCompatible compatible}
	 *         with the type of this column.
	 * 
	 * @return The column value pair.
	 */
	ColVal<T> value(T value);
	
	/**
	 * As the {@link #name} method, returns the name of this column.
	 * 
	 * @return The name of this column, never {@code null} and never an empty
	 *         string.
	 */
	@Override
	String toString();
}
