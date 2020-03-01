/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.misc.array;

/**
 * Defines the interface of a class which is able to round a non-negative
 * decimal decimal value to a non-negative integer value in a way which depends
 * on the implementing class.
 *
 * @author Beat Hoermann
 */
interface Rounder {
	/**
	 * Rounds the specified non-negative decimal value to a non-negative integer
	 * value.
	 * 
	 * @param  d The non-negative decimal value to round.
	 * @return The non-negative rounded integer value.
	 */
	int round(double d);
}
