/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store;

/**
 * A data structure keeping together a byte array and an offset.
 * 
 *  @author Beat Hoermann
 */
public final class Bag {
	/**
	 * The resulting byte array.
	 */
	public byte[] bytes;
	/**
	 * The position within the byte array where the relevant data starts.
	 */
	public int offset = 0;
	
	/**
	 * Constructs a bag with {@code Bag.bytes} equal to {@code null} and
	 * {@code Bag.offset} equal to 0.
	 */
	public Bag() {
		bytes = null;
	}
	
	/**
	 * Constructs a bag with {@code Bag.bytes} set equal to the specified byte
	 * array and with {@code Bag.offset} equal to 0.
	 * 
	 * @param byteArr The byte array.
	 */
	public Bag(byte[] byteArr) {
		bytes = byteArr;
	}
	
	/**
	 * Constructs a bag with {@code Bag.bytes} set to a newly created byte array
	 * of the specified size and with {@code Bag.offset} equal to 0.
	 * 
	 * @param n The size of the byte array, must be greater than or equal to 0.
	 */
	public Bag(int n) {
		bytes = new byte[n];
	}
}
