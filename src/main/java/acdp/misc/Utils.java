/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.misc;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Defines some useful constants and methods that are heavily used inside ACDP
 * but may be useful to custom database designers as well.
 *
 * @author Beat Hoermann
 */
public class Utils {
	/**
	 * One mebibyte has 1024 * 1024 = 1048576 bytes.
	 */
	public static final int oneMiB = 1048576; 
	
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
	 * (This variable is not used inside ACDP.)
	 */
	public static final long[] bnd8 = new long[9];
	
	/**
	 * The value of {@code bnd4}[x] for 0 &le; {@literal x < 4} is equal to
	 * 256<sup>x</sup> - 1 and the value of {@code bnd4}[4] is equal to the value
	 * of the {@link Integer#MAX_VALUE} constant.
	 * <p>
	 * Treat this array as if it were immutable: Don't change any of its
	 * elements.
	 * (This variable is not used inside ACDP.)
	 */
	public static final int[] bnd4 = new int[5];
	
	/**
	 * The value of {@code zeros}[x] for 0 &le; x &le; 8 is equal to a byte
	 * array of length x filled with zeros.
	 * Note that {@code zeros}[0] returns an empty byte array.
	 * <p>
	 * Treat this array as if it were immutable: Don't change any of its
	 * elements.
	 * (This variable is not used inside ACDP.)
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
	 * Computes &lfloor;{@code log}<sub>256</sub>(x)&rfloor; + 1 for x &ge; 1.
	 * <p>
	 * The result of this method is equal to the minimum number of bytes
	 * necessary to represent the argument as an unsigend integer using the
	 * binary numeral system.
	 * <p>
	 * Note that {@code lor}({@code bnd8}[x]) equals {@code x} for 1  &le; x
	 * &le; 8.
	 * 
	 * @param  x The argument.
	 * @return The value of the described function or 1 if {@literal x < 1}.
	 */
	public static final int lor(long x) {
		int lor = 1;
		if (x > 255) lor++; else return lor;
		if (x > 65535) lor++; else return lor;
		if (x > 16777215) lor++; else return lor;
		if (x > 4294967295L) lor++; else return lor;
		if (x > 1099511627775L) lor++; else return lor;
		if (x > 281474976710655L) lor++; else return lor;
		if (x > 72057594037927900L) lor++; else return lor;
		return lor;
	}
	
	/**
	 * Returns the number of bytes of a bitmap with {@code n} bits represented
	 * as an array of bytes.
	 * 
	 * @param  n The number of bits of the bitmap.
	 * @return The length of the bitmap or 0 if {@code n} is less than 1.
	 */
	public static final int bmLength(int n) {
		return n < 1 ? 0 : (n - 1) / 8 + 1;
	}
	
	/**
	 * Builds a path from the specified path string and the optional directory
	 * path.
	 * If the specified path string turns out to denote a relative path then
	 * it is resolved against the specified directory path.
	 * 
	 * @param  pathStr The path string, not allowed to be {@code null} or an
	 *         empty string.
	 * @param  dirPath The directory path.
	 *         This value is allowed to be {@code null} if and only if {@code
	 *         pathStr} denotes an absolute path.
	 * 
	 * @return The resulting path.
	 * 
	 * @throws NullPointerException If the path string denotes a relative path
	 *         and the directory path is {@code null}.
	 * @throws IllegalArgumentException If the path string is {@code null} or an
	 *         empty string.
	 * @throws InvalidPathException If the path string is invalid.
	 */
	public static final Path buildPath(String pathStr, Path dirPath) throws
			NullPointerException, IllegalArgumentException, InvalidPathException {
		if (pathStr == null || pathStr.isEmpty()) {
			throw new IllegalArgumentException("Path string is null or an empty " +
																							"string.");
		}
		final Path filePath = Paths.get(pathStr);
		return filePath.isAbsolute() ? filePath : dirPath.resolve(filePath);
	}
	
	/**
	 * Finds out if all the bytes of the specified subarray of bytes are zero.
	 * 
	 * @param  bytes The byte array to test, not allowed to be {@code null}.
	 *         The length of the array must be greater than or equal to the value
	 *         of {@code offset + len}.
	 *         This method reads the elements with indices {@code offset},
	 *         {@code offset + 1}, ..., {@code offset + len - 1} of the array.
	 *         The other elements of the array remain untouched.
	 * @param  offset The index within {@code bytes} where to start testing the
	 *         bytes.
	 *         The value must be greater than or equal to zero.
	 * @param  len The number of bytes to test.
	 *         The value must be greater than or equal to zero.
	 *         
	 * @return The boolean value {@code true} if the byte array has at least one
	 *         byte and all bytes are equal 0, {@code false} otherwise.
	 *         
	 * @throws NullPointerException If {@code bytes} is {@code null}.
	 * @throws IndexOutOfBoundsException If the condition described in the
	 *         {@code bytes} parameter description is not met.
	 */
	public static final boolean isZero(byte[] bytes, int offset, int len) throws
										NullPointerException, IndexOutOfBoundsException {
		final int end = offset + len;
		boolean allZero = true;
		int i = offset;
		while (i < end && allZero) {
			allZero = bytes[i++] == 0;
		}
		return allZero;
	}
	
	/**
	 * Returns the number of one-bits in the specified subarray of bytes.
	 * 
	 * @param  bytes The byte array, not allowed to be {@code null}.
	 *         The length of the array must be greater than or equal to the value
	 *         of {@code offset + len}.
	 *         This method reads the elements with indices {@code offset},
	 *         {@code offset + 1}, ..., {@code offset + len - 1} of the array.
	 *         The other elements of the array remain untouched.
	 * @param  offset The index within {@code bytes} where to start counting the
	 *         one-bits.
	 *         The value must be greater than or equal to zero.
	 * @param  len The number of bytes.
	 *         The value must be greater than or equal to zero.
	 *         
	 * @return The number of one-bits in the specified subarray of bytes.
	 *         
	 * @throws NullPointerException If {@code bytes} is {@code null}.
	 * @throws IndexOutOfBoundsException If the condition described in the
	 *         {@code bytes} parameter description is not met.
	 */
	public static final int bitCount(byte[] bytes, int offset, int len) throws
										NullPointerException, IndexOutOfBoundsException {
		int n = 0;
		for (int i = offset; i < offset + len; i++) {
			n += Integer.bitCount(bytes[i] & 0xFF);
		}
		return n;
	}
	
	
	/**
	 * Finds out if at least one element of the specified array is equal to
	 * {@code null}.
	 * 
	 * @param   arr The array to test, not allowed to be {@code null}.
	 * 
	 * @return  The boolean value {@code true} if at least one element of the
	 *          specified array is equal to {@code null}, {@code false}
	 *          otherwise.
	 *         
	 * @throws NullPointerException If {@code arr} is {@code null}.
	 */
	public static final boolean hasNull(Object[] arr) throws
																			NullPointerException {
		boolean hasNull = false;
		int i = 0;
		while (i < arr.length && !hasNull) {
			hasNull = arr[i++] == null;
		}
		return hasNull;
	}
	
	/**
	 * Finds out if {@code arr1[k]} == {@code arr2[k]} for {@code offset} &le;
	 * {@code k} &lt; {@code offset} + {@code n}.
	 * 
	 * @param  arr1 The first byte array, not allowed to be {@code null}.
	 *         The array must contain at least {@code offset} + {@code n} bytes.
	 * @param  arr2 The second byte array, not allowed to be {@code null}.
	 *         The array must contain at least {@code offset} + {@code n} bytes.
	 * @param  offset The index where to start the comparison.
	 * @param  n The number of bytes to compare.
	 *         Must be greater than or equal to zero.
	 *         
	 * @return The boolean value {@code true} if the specified arrays have equal
	 *         byte values for all indices in the specified range, {@code false}
	 *         otherwise.
	 *         
	 * @throws NullPointerException If {@code arr1} or {@code arr2} is {@code
	 *         null}.
	 * @throws IndexOutOfBoundsException If the condition described in the
	 *         {@code arr1} or {@code arr2} parameter description is not met.
	 */
	public static final boolean equals(byte[] arr1, byte[] arr2, int offset,
																						int n) throws
										NullPointerException, IndexOutOfBoundsException {
		boolean equals = true;
		final int end = offset + n;
		int i = offset;
		while (i < end && equals) {
			equals = arr1[i] == arr2[i];
			i++;
		}
		return equals;
	}
	
	/**
	 * Converts the specified unsigned integer value of the specified length to
	 * a byte array of the same length.
	 * 
	 * @param  val The value of the unsigned integer.
	 *         The value must be greater than or equal to zero.
	 * @param  len The length of the unsigned integer value in bytes.
	 *         The value must be greater than 0 and less than or equal to 8.
	 *         
	 * @return The byte array of length {@code len} housing the converted
	 *         unsigned integer.
	 *         
	 * @throws IndexOutOfBoundsException If the condition described in the
	 *         {@code len} parameter description is not met.
	 */
	public static final byte[] unsToBytes(long val, int len) {
		byte[] bytes = new byte[len];
		unsToBytes(val, len, bytes, 0);
		return bytes;
	}
	
	/**
	 * Converts the specified unsigned integer value of the specified length to
	 * a byte array and stores it into the specified byte array.
	 * <p>
	 * Internally this method just calls the {@link #unsToBytes(long, int,
	 * byte[], int)} method with the last parameter set to zero.
	 * 
	 * @param  val The value of the unsigned integer.
	 *         The value must be greater than or equal to zero.
	 * @param  len The length of the unsigned integer value in bytes.
	 *         The value must be greater than 0 and less than or equal to 8.
	 * @param  bytes The byte array which houses the byte representation of the
	 *         unsigned integer, not allowed to be <code>null</code>.
	 *         The length of the array must be greater than or equal to the value
	 *         of {@code len}.
	 *         This method sets the first {@code len} elements of the array.
	 *         The other elements of the array remain untouched.
	 *         
	 * @throws NullPointerException If {@code bytes} is {@code null}.
	 * @throws IndexOutOfBoundsException If the condition described in the
	 *         {@code bytes} parameter description is not met.
	 */
	public static final void unsToBytes(long val, int len, byte[] bytes) throws
										NullPointerException, IndexOutOfBoundsException {
		unsToBytes(val, len, bytes, 0);
	}
	
	/**
	 * Converts the specified unsigned integer value of the specified length to
	 * a byte array and stores it into the specified byte array starting at
	 * the specified offset.
	 * 
	 * @param  val The value of the unsigned integer.
	 *         The value must be greater than or equal to zero.
	 * @param  len The length of the unsigned integer value in bytes.
	 *         The value must be greater than 0 and less than or equal to 8.
	 * @param  bytes The byte array which houses the byte representation of the
	 *         unsigned integer, not allowed to be <code>null</code>.
	 *         The length of the array must be greater than or equal to the value
	 *         of {@code offset + len}.
	 *         This method sets the elements with indices {@code offset},
	 *         {@code offset + 1}, ..., {@code offset + len - 1} of the array.
	 *         The other elements of the array remain untouched.
	 * @param  offset The index within {@code bytes} where to start saving the
	 *         result.
	 *         The value must be greater than or equal to zero.
	 *         
	 * @throws NullPointerException If {@code bytes} is {@code null}.
	 * @throws IndexOutOfBoundsException If the condition described in the
	 *         {@code bytes} parameter description is not met.
	 */
	public static final void unsToBytes(long val, int len, byte[] bytes,
																				int offset) throws
										NullPointerException, IndexOutOfBoundsException {
		for (int i = len - 1; i > 0; i--) {
			bytes[offset++] = (byte) (val >>> (i << 3));
		}
		bytes[offset] = (byte) val;
	}
	
	/**
	 * Converts the specified byte array of the specified length to an unsigned
	 * integer value.
	 * <p>
	 * Internally this method just calls the {@link #unsFromBytes(byte[], int,
	 * int)} method with the second parameter set to zero.
	 * 
	 * @param  bytes The byte array representing the unsigned integer value, not
	 *         allowed to be <code>null</code>.
	 *         The length of the array must be greater than or equal to the value
	 *         of {@code len}.
	 *         This method reads the first {@code len} elements of the array.
	 *         The other elements of the array remain untouched.
	 * @param  len The length of the unsigned integer in bytes.
	 *         The value must be greater than 0 and less than or equal to 8.
	 *         
	 * @return The unsigned integer value.
	 *         
	 * @throws NullPointerException If {@code bytes} is {@code null}.
	 * @throws IndexOutOfBoundsException If the condition described in the
	 *         {@code bytes} parameter description is not met.
	 */
	public static final long unsFromBytes(byte[] bytes, int len) throws
										NullPointerException, IndexOutOfBoundsException {
		return unsFromBytes(bytes, 0, len);
	}
	
	/**
	 * Converts the specified byte subarray to an unsigned integer value.
	 * 
	 * @param  bytes The byte array representing the unsigned integer value, not
	 *         allowed to be <code>null</code>.
	 *         The length of the array must be greater than or equal to the value
	 *         of {@code offset + len}.
	 *         This method reads the elements with indices {@code offset},
	 *         {@code offset + 1}, ..., {@code offset + len - 1} of the array.
	 *         The other elements of the array remain untouched.
	 * @param  offset The index within {@code bytes} where to start reading the
	 *         bytes of the unsigned integer.
	 *         The value must be greater than or equal to zero.
	 * @param  len The length of the unsigned integer in bytes.
	 *         The value must be greater than 0 and less than or equal to 8.
	 *         
	 * @return The unsigned integer value.
	 *         
	 * @throws NullPointerException If {@code bytes} is {@code null}.
	 * @throws IndexOutOfBoundsException If the condition described in the
	 *         {@code bytes} parameter description is not met.
	 */
	public static final long unsFromBytes(byte[] bytes, int offset,
																					int len) throws
										NullPointerException, IndexOutOfBoundsException {
		long val = 0;
		for (int i = len - 1; i > 0; i--) {
			val += (long) (bytes[offset++] & 255) << (i << 3);
		}
		val += bytes[offset] & 255;
		
		return val;
	}
}
