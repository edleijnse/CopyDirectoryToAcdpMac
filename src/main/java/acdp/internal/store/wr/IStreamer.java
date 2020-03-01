/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import acdp.internal.FileIOException;
import acdp.internal.store.Bag;

/**
 * A streamer consecutively delivers byte arrays of variable lengths from some
 * data source that provides readable access to its data in the form of a
 * sequence of bytes.
 *
 * @author Beat Hoermann
 */
interface IStreamer {
	/**
	 * Pulls the specified number of bytes from the data source and saves the
	 * pulled byte array into the specified bag.
	 * Pulling beyond a predefined end results in an unchanged bag.
	 * <p>
	 * Ensure that the fiel is open, provided that this streamer is backed by
	 * a file.
	 * 
	 * @param  len The number of bytes to pull from the data source, must be
	 *         greater than or equal to zero.
	 * @param  bag The bag containing the pulled byte array of length {@code
	 *         len}, not allowed to be {@code null}.
	 *         
	 * @throws NullPointerException If {@code bag} is {@code null}.
	 * @throws FileIOException If the end of the file is reached before the byte
	 *         buffer is completely filled or if an I/O error occurs while
	 *         reading the file.
	 *         This exception never happens if the streamer is not backed by a
	 *         file.
	 */
	void pull(int len, Bag bag) throws NullPointerException, FileIOException;
}
