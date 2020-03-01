/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.misc.array;


/**
 * A rounder that rounds a given decimal value greater than or equal to zero
 * to the nearest integer {@code n} such that {@code n} is greater than or equal
 * to {@code len}, passed via the constructor, and divisible by {@code len}
 * without remainder.
 *
 * @author Beat Hoermann
 */
final class RounderLen implements Rounder {
	private final int len;
	private final int MAX_VALUE;
	
	/**
	 * Constructs the rounder.
	 * 
	 * @param len The rounding parameter, always greater than zero.
	 *        See the description of the {@link #round} method to learn more
	 *        details about the nature of this parameter.
	 */
	RounderLen(int len) {
		this.len = len;
		this.MAX_VALUE = Integer.MAX_VALUE  / len * len;
	}
	
	/**
	 * Rounds the specified non-negative decimal value to the nearest integer
	 * {@code n} such that {@code n} is greater than or equal to {@code len} and
	 * {@code n} is divisible by {@code len} without remainder, {@code len}
	 * denoting the value passed via the constructor.
	 * 
	 * @param  d The decimal value to round.
	 *         Must be greater than or equal to zero.
	 * @return The nearest integer {@code n} to {@code d} such that {@code n} is
	 *         divisible by {@code len} without remainder.
	 *         This value is always greater than or equal to {@code len} and
	 *         less than or equal to {@code MAX}, where {@code MAX} is the
	 *         largest integer less than or equal to the value of the
	 *         {@link Integer#MAX_VALUE} constant having this property.
	 */
	@Override
	public int round(double d) {
		if (d < len)
			return len;
		else {
			double val = Math.round(d / len) * len;
			return val > MAX_VALUE  ? MAX_VALUE : (int) val;
		}
	}
}
