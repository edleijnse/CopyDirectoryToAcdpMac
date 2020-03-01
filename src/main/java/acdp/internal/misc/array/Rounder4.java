/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.misc.array;


/**
 * A rounder that rounds a given decimal value greater than or equal to zero
 * to the nearest integer {@code n} such that 12 + {@code n} is divisible by 8
 * without remainder.
 *
 * @author Beat Hoermann
 */
final class Rounder4 implements Rounder {
	private static final int MAX_VALUE = (Integer.MAX_VALUE - 4) / 8 * 8 + 4;
	
	/**
	 * Rounds the specified non-negative decimal value to the nearest integer
	 * {@code n} such that 12 + {@code n} is divisible by 8 without remainder.
	 * This implies that {@code n} is divisible by 4 without remainder.
	 * 
	 * @param  d The decimal value to round.
	 *         Must be greater than or equal to zero.
	 * @return The nearest integer {@code n} to {@code d} such that 12 +
	 *         {@code n} is divisible by 8 without remainder.
	 *         This value is always greater than or equal to 4 and less than
	 *         or equal to {@code MAX}, where {@code MAX} is the largest integer
	 *         less than or equal to the value of the {@link Integer#MAX_VALUE}
	 *         constant having this property.
	 */
	@Override
	public int round(double d) {
		double val = Math.round((d - 4.0) / 8.0) * 8.0 + 4.0;
		return val > MAX_VALUE ? MAX_VALUE : (int) val;
	}
}
