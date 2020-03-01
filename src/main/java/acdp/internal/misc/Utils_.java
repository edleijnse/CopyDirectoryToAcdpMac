/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.misc;

import java.util.Arrays;

/**
 * Defines some useful constants.
 * The same constants appear in the {@link acdp.misc.Utils Utils} class that is
 * part of the API.
 * However, there is a danger that the elements of the declared arrays are
 * changed by client code.
 * This is why these constants are repeated here to be accessed inside ACDP
 * only.
 *
 * @author Beat Hoermann
 */
public final class Utils_ {
	/**
	 * The value of {@code bnd8}[x] for 0 &le; {@literal x < 8} is equal to
	 * 256<sup>x</sup> - 1 and the value of {@code bnd8}[8] is equal to the value
	 * of the {@link Long#MAX_VALUE} constant.
	 * <p>
	 * The value of {@code bnd8}[x] is equal to the largest unsigned integer
	 * with a width (or precision) equal to x bytes using the binary numeral
	 * system.
	 * The value of {@code bnd8}[8] is equal to Java's largest positive built-in
	 * integer number.
	 * <p>
	 * Treat this array as if it were immutable: Don't change any of its
	 * elements.
	 */
	public static final long[] bnd8 = new long[9];
	
	/**
	 * The value of {@code bnd4}[x] for 0 &le; {@literal x < 4} is equal to
	 * 256<sup>x</sup> - 1 and the value of {@code bnd4}[4] is equal to the value
	 * of the {@link Integer#MAX_VALUE} constant.
	 * <p>
	 * Treat this array as if it were immutable: Don't change any of its
	 * elements.
	 */
	public static final int[] bnd4 = new int[5];
	
	/**
	 * The value of {@code zeros}[x] for 0 &le; x &le; 8 is equal to a byte
	 * array of length x filled with zeros.
	 * Note that {@code zeros}[0] returns an empty byte array.
	 * <p>
	 * Treat this array as if it were immutable: Don't change any of its
	 * elements.
	 */
	public static final byte[][] zeros = new byte[9][];
	
	static {
		long factL = 1L;
		for (int i = 0; i < 8; i++) {
			bnd8[i] = factL - 1L;
			factL *= 256L;
		}
		bnd8[8] = Long.MAX_VALUE;
		
		int factInt = 1;
		for (int i = 0; i < 4; i++) {
			bnd4[i] = factInt - 1;
			factInt *= 256;
		}
		bnd4[4] = Integer.MAX_VALUE;
		
		for (int i = 0; i < 9; i++) {
			final byte[] bytes = new byte[i];
			Arrays.fill(bytes, (byte) 0);
			zeros[i] = bytes;
		}
	}
	
	/**
	 * Prevent object construction.
	 */
	private Utils_() {
	}
}
