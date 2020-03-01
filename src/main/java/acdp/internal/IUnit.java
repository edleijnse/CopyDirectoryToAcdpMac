/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import acdp.exceptions.UnitBrokenException;

/**
 * Defines the interface of a unit from the perspective of a write operation.
 *
 * @author Beat Hoermann
 */
public interface IUnit {
	/**
	 * Writes the specified subarray of the specified before data located at the
	 * specified position within the specified file to the recorder file.
	 * <p>
	 * This method is supposed to be invoked by a write operation.
	 * This ensures that the unit was originally opened by the same thread.
	 * 
	 * @param  file The file, not allowed to be {@code null}.
	 * @param  pos The position within the file.
	 * @param  data The before data, not allowed to be {@code null}.
	 * @param  offset The offset within {@code data} of the first byte to be 
	 *         recorded; must be non-negative and no larger than {@code
	 *         data.length}.
	 * @param  length The number of bytes to be recorded from {@code data}; must
	 *          be non-negative and no larger than {@code data.length - offset}.
	 * 
	 * @throws IndexOutOfBoundsException If the preconditions on the {@code
	 *         offset} and {@code length} parameters do not hold.
	 * @throws UnitBrokenException If recording before data fails, including the
	 *         case that the unit was already broken before this method was
	 *         able to start doing its job.
	 */
	void record(FileIO file, long pos, byte[] data, int offset,
				int length) throws IndexOutOfBoundsException, UnitBrokenException;
	
	/**
	 * Writes the specified before data located at the specified position within
	 * the specified file to the recorder file.
	 * <p>
	 * This method is supposed to be invoked by a write operation.
	 * This ensures that the unit was originally opened by the same thread.
	 * <p>
	 * Invoking this method behaves exactly the same as invoking
	 * 
	 * <pre>
	 * record(file, pos, data, 0, data.length)</pre>
	 * 
	 * @param  file The file, not allowed to be {@code null}.
	 * @param  pos The position within the file.
	 * @param  data The before data, not allowed to be {@code null}.
	 * 
	 * @throws UnitBrokenException If recording before data fails, including the
	 *         case that the unit was already broken before this method was
	 *         able to start doing its job.
	 */
	void record(FileIO file, long pos, byte[] data) throws UnitBrokenException;
	
	/**
	 * Adds the specified file to the internal list of files that must be {@link
	 * java.nio.channels.FileChannel#force forced}.
	 * <p>
	 * Invoke this method if data in the file has changed.
	 * It is not necessary to invoke this method if there is an invocation of
	 * the {@link #record} method with the same file.
	 * 
	 * @param file The file to force.
	 */
	void addToForceList(FileIO file);
}
