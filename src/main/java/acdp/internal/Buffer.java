/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import java.nio.ByteBuffer;

import acdp.misc.Utils;

/**
 * Provides a {@link ByteBuffer} with a <em>maximum capacity</em> equal to the
 * value passed via the constructor of this class.
 * <p>
 * Let {@code max} be the maximum value of all values passed to the {@link
 * #buf(long)} method so far and assume that the {@code buf(long)} method is
 * now invoked with a limit equal to the value of {@code n}.
 * Then the {@code buf(long)} method returns the same instance of a {@code
 * ByteBuffer} that was returned when this method was invoked the last time
 * if and only if {@code n} is less than or equal to {@code max} or if {@code
 * max} is greater than or equal to the maximum capacity.
 * <p>
 * Due to this behaviour, it is a good idea to reuse an instance of this class
 * as often as possible so as to reduce the frequency of requesting memory from
 * the JVM.
 * <p>
 * Note that the buffer always grows and may finally reach its maximum capacity.
 * This is why the maximum capacity should be chosen such that it is small
 * compared to the size of available main memory, especially if this buffer
 * has a long lifetime.
 * (I don't feel that it's worth dealing with a {@linkplain
 * java.lang.ref.SoftReference soft reference}.)
 *
 * @author Beat Hoermann
 */
public final class Buffer {
	/**
	 * The maximum capacity of the buffer returned by the {@link #buf}  method.
	 */
	private final int maxCap;
	
	/**
	 * The buffer.
	 * <p>
	 * Access to this property is reserved to the {@link #buf} method.
	 */
	private ByteBuffer buf;
	
	/**
	 * Returns the maximum capacity of the byte buffer returned by the {@link
	 * #buf} method.
	 * 
	 * @return The maximum size.
	 *         This value is equal to the value passed via the constructor.
	 */
	public final int maxCap() {
		return maxCap;
	}
	
	/**
	 * Returns the rewinded byte buffer with a limit equal to either the
	 * specified limit or the value returned by the {@link #maxCap()} method,
	 * whichever is less.
	 * <p>
	 * Note that the same reference returned by this method may be returned by
	 * a previous invocation of this method, hence, the buffers returned by this
	 * method may not be memory independent.
	 * 
	 * @param  limit The desired limit of the byte buffer.
	 * 
	 * @return The rewinded byte buffer, never {@code null}.
	 */
	public final ByteBuffer buf(long limit) {
		final int boundedLimit = limit > maxCap ? maxCap : (int) limit;
		if (buf == null || boundedLimit > buf.capacity())
			buf = ByteBuffer.allocate(boundedLimit);
		else {
			buf.limit(boundedLimit);
			buf.rewind();
		}
		return buf;
	}
	
	/**
	 * Constructs the buffer with the specified maximum capacity.
	 * <p>
	 * Some sources recommend using a multiple of 4 KiB = 4096 bytes to be a
	 * good value for reading some data from a disk into main memory.
	 * 
	 * @param maxCap The maximum capacity of the byte buffer returned by the
	 *        {@link #buf} method.
	 */
	public Buffer(int maxCap) {
		this.maxCap = maxCap;
		buf = null;
	}
	
	/**
	 * Creates the buffer with a maximum capacity of {@linkplain Utils#oneMiB
	 * one mebibyte}.
	 */
	public Buffer() {
		this(Utils.oneMiB);
	}
}
