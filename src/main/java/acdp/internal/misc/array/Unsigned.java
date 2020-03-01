/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.misc.array;

/**
 * Utility class to convert an unsigned integer to a byte-array and vice versa,
 * inspired by the source code of the {@link java.io.DataOutputStream#writeLong}
 * and the {@link java.io.DataInputStream#readLong} methods.
 *
 * @author Beat Hoermann
 */
public final class Unsigned {
	
	/**
	 * Prevent object construction.
	 */
	private Unsigned() {
	}
	
	/**
	 * Converts the specified unsigned integer value of the specified length to
	 * a byte array of at least the same length.
	 * 
	 * @param val The value of the unsigned integer.
	 *        The value must be greater than or equal to zero.
	 * @param len The length of the unsigned integer value in bytes.
	 *        The value must be greater than 0 and less than or equal to 8.
	 * @param bytes The byte array which houses the byte representation of the
	 *        unsigned integer, not allowed to be <code>null</code>.
	 *        The length of the array must be greater than or equal to the value
	 *        of {@code len}.
	 *        This method sets the first {@code len} elements of the array.
	 *        The other elements of the array remain untouched.
	 */
	static final void toBytes(long val, int len, byte[] bytes) {
		int k = 0;
		for (int i = len - 1; i > 0; i--) {
			bytes[k++] = (byte) (val >>> (i << 3));
		}
		bytes[k] = (byte) val;
	}
	
	/**
	 * Converts the specified byte array to an unsigned integer value.
	 * The byte array is typically the result of a call to the {@link #toBytes}
	 * method.
	 * 
	 * @param  bytes The byte array representing the unsigned integer value, not
	 *         allowed to be <code>null</code>.
	 *         The length of the array must be greater than or equal to the value
	 *         of {@code len}.
	 *         This method reads the first {@code len} elements of the array.
	 *         The other elements of the array remain untouched.
	 * @param  len The length of the unsigned integer in bytes.
	 *         The value must be greater than 0 and less than or equal to 8.
	 * @return The unsigned integer value.
	 */
	static final long fromBytes(byte[] bytes, int len) {
		int k = 0;
		long val = 0;
		for (int i = len - 1; i > 0; i--) {
			val += (long) (bytes[k++] & 255) << (i << 3);
		}
		val += bytes[k] & 255;
		
		return val;
	}
}
