/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import acdp.internal.store.Bag;

/**
 * A streamer that uses a byte array as its source of data.
 *
 * @author Beat Hoermann
 */
final class ArrayStreamer implements IStreamer {
	/**
	 * The backing byte array.
	 */
	private final byte[] byteArr;
	/**
	 * The current position within the byte array.
	 */
	private int pos;
	
	/**
	 * The constructor.
	 * 
	 * @param byteArr The byte array, not allowed to be {@code null} and not
	 *        allowed to be empty.
	 * @param offset The position within the byte array where to start returning
	 *        the data from.
	 *        This value must be less than {@code byteArr.length}.
	 */
	ArrayStreamer(byte[] byteArr, int offset) {
		this.byteArr = byteArr;
		this.pos = offset;
	}

	@Override
	public final void pull(int len, Bag bag) throws NullPointerException {
		bag.bytes = byteArr;
		bag.offset = pos;
		pos += len;
	}
}
