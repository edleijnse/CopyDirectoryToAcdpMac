/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.nio.ByteBuffer;

import acdp.internal.Buffer;
import acdp.internal.FileIOException;

/**
 * This class buffers data of varying length received via the {@code write}
 * methods and invokes the {@link #save} method as soon as the buffer is full
 * and has reached its maximum size.
 * <p>
 * Subclasses typically implement the {@code save} method such that it writes
 * data to a file.
 * <p>
 * By transferring some few large blocks of data to the mass storage media
 * instead of many small pieces, the overall write performance may be improved.
 * Whether the write performance can actually be improved applying this
 * strategy, depends on the device controlling the mass storage media.
 * (The {@code java.io.BufferedOutputStream} class follows the same approach.)
 *
 * @author Beat Hoermann
 */
abstract class AbstractWriter {
	private final Buffer buffer;
	private final int maxCap;
	protected ByteBuffer buf;
	
	/**
	 * Saves the data in the specified buffer to a file.
	 * 
	 * @param  buf The buffer, never {@code null}.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	protected abstract void save(ByteBuffer buf) throws FileIOException;
	
	/**
	 * Creates a writer with an initial buffer limit of {@code min(limit,
	 * buffer.maxCap())} bytes.
	 * 
	 * @param limit The initial limit of the buffer, must be greater than zero.
	 * @param buffer The buffer to apply, not allowed to be {@code null}.
	 */
	protected AbstractWriter(long limit, Buffer buffer) {
		this.buffer = buffer;
		this.maxCap = buffer.maxCap();
		this.buf = buffer.buf(limit);
		// Set limit of buf equal to its capacity and thus take the opportunity
		// that buf's capacity may be greater than limit.
		buf.clear();
	}
	
	/**
	 * Increases the limit of the buffer to the specified new limit.
	 * 
	 * @param newLimit The new limit of the buffer.
	 */
	private final void increaseLimit(int newLimit) {
		// buffer.limit() == buffer.capacity() && newLimit > buffer.limit()
		int pos = buf.position();
		final ByteBuffer buf1 = buffer.buf(newLimit);
		if (buf == buf1)
			// Same ByteBuffer instance: Restore position.
			buf.position(pos);
		else {
			// New ByteBuffer instance: Copy contents of old instance.
			buf1.put(buf.array(), 0, pos);
			buf = buf1;
		}
	}
	
	/**
	 * Writes the specified byte to this writer.
	 * 
	 * @param  data The byte to be written.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	final void write(int data) throws FileIOException {
		if (buf.hasRemaining())
			buf.put((byte) data);
		else {
			write(new byte[] { (byte) data }, 0, 1);
		}
	}
	
   /**
    * Writes {@code len} bytes from the specified byte array to this writer
    * starting at offset {@code off}.
    * 
    * @param  data The byte array to be written.
    * @param  off The offset within the byte array of the first byte to be
    *         written.
     *        Must be greater than or equal to zero and no larger than the
     *        length of the byte array.
    * @param  len The number of bytes to be written.
    *         Must be greater than or equal to zero and no larger than the
    *         length of the byte array minus the offset.
    * 
    * @throws NullPointerException If {@code data} is {@code null}.
    * @throws IndexOutOfBoundsException If the preconditions on the offset and
    *         length parameters do not hold.
	 * @throws FileIOException If an I/O error occurs.
    */
   final void write(byte[] data, int off, int len) throws NullPointerException,
   											IndexOutOfBoundsException, FileIOException {
   	int newPos = buf.position() + len;
		if (newPos < 0) {
			// Overflow.
			newPos = Integer.MAX_VALUE;
		}
		
		final int limit = buf.limit();
   	if (newPos <= limit)
			// The buffer has enough free space to keep the data.
			// Copy data to buffer.
			buf.put(data, off, len);
		else if (limit < maxCap && newPos < maxCap) {
			// newPos > limit && limit < maxCap && newPos <= maxCap
			// The buffer has not enough free space to keep the data but the
			// buffer's limit can be increased such that the buffer can keep
			// the data.
			// Aggressively increase the buffer's limit without exceeding maxCap.
			increaseLimit(Math.min(Math.max(newPos, 10 * limit), maxCap));
			// Copy data to buffer.
			buf.put(data, off, len);
		}
		else {
			// newPos > limit && (limit == maxCap || newPos > maxCap)
			// It follows that newPos > maxCap.
			// The buffer has not enough free space to keep the data and the
			// buffer's limit is either already at its maximum size or it can't
			// keep the data even if it is increased to its maximum size.
			final int free = maxCap - buf.position();
			final int left = len - free;
			// left > 0 since newPos > maxCap.
			if (left > maxCap) {
				// left > maxCap
				// An empty buffer with a maximum size is not enough to hold the
				// portion of the data left after the first portion of the data is
				// copied to the buffer with maximum size.
				// Give up!
				flush();
				save(ByteBuffer.wrap(data, off, len));
			}
			else {
				// left <= maxCap
				// An empty buffer with a maximum size is enough to hold the
				// portion of the data left after the first portion of the data is
				// copied to the buffer with maximum capacity.
				if (limit < maxCap) {
					// Increase butter's limit to its maximum.
					increaseLimit(maxCap);
				}
				// Copy data to buffer
				buf.put(data, off, free);
				// Buffer is full! Write buffer.
				// buf.position() == buf.limit() == buf.capacity()
				buf.rewind();
				save(buf);
				buf.rewind();
				buf.put(data, free, left);
			}
		}
	}
   
	/**
	 * Invokes the {@link #save} method, provided that the buffer contains at
	 * least one byte.
	 * Invoke this method once when your are done to ensure that all data
	 * given to this writer via its {@code write} methods are actually written
	 * to the file.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	final void flush() throws FileIOException {
		if (buf.position() > 0) {
			buf.flip();
			save(buf);
			buf.clear();
		}
	}
}
