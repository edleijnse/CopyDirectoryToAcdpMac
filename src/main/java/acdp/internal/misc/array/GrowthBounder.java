/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.misc.array;

/**
 * Defines the interface of a class which computes an upper bound on the
 * number of elements left to be added to the array.
 * <p>
 * Let's give a possible use case for a growth bounder, assuming a program
 * querying the rows of a table.
 * Suppose that the program iterates the rows of the table.
 * Furthermore, suppose that it stores each row satisfying the criteria of the
 * query into the array.
 * At the beginning, a maximum of {@code n} rows may be added to the array,
 * {@code n} being the number of rows in the table.
 * After having processed {@code n-7} rows, no more than 7 rows are left that
 * must be added to the array at most.
 * <p>
 * Knowing this information, the array-logic may be able to limit its growth
 * next time it enlarges the capacity of the array.
 * <p>
 * Provide an implementation of this interface even if the upper bound is loose.
 *
 * @author Beat Hoermann
 */
public interface GrowthBounder {
	
	/**
	 * Returns an upper bound on the number of elements left to be added to the
	 * array.
	 * 
	 * @return An upper bound on the number of elements left to be added to the
	 *         array, always greater than zero.
	 *         A value equal to {@link Integer#MAX_VALUE} signals the
	 *         unavailability of such a bound.
	 */
	int bound();
}
