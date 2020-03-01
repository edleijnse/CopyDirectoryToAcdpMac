/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.types;

/**
 * Defines an array column type.
 * <p>
 * Values of this type are either {@code null} or they are array values having
 * at most {@code n} elements where {@code n} denotes the value of the
 * {@code maxSize} parameter given via the constructor.
 *
 * @author Beat Hoermann
 */
public abstract class AbstractArrayType extends Type_ {
	/**
	 * The maximum number of elements in an array value of this array type.
	 */
	protected final int maxSize;
	
	/**
	 * The constructor
	 * 
	 * @param  scheme The storage scheme of the type, not allowed to be
	 *         {@code null}.
	 * @param  maxSize The maximum number of elements in an array value of this
	 *         array type.
	 *         
	 * @throws NullPointerException If {@code scheme} is {@code null}.
	 * @throws IllegalArgumentException If {@code maxSize} is less than 1.
	 */
	protected AbstractArrayType(Scheme scheme, int maxSize) throws
										NullPointerException, IllegalArgumentException {
		super(scheme);
		if (maxSize < 1) {
			throw new IllegalArgumentException("The value of \"maxSize\" must " +
											"be greater than or equal to 1: " + maxSize);
		}
		this.maxSize = maxSize;
	}
	
	/**
	 * Returns the maximum size of this array type, hence, the maximum number of
	 * elements in an array value of this array type
	 * 
	 * @return The maximum size of this array type, greater than or equal to 1.
	 */
	public final int maxSize() {
		return maxSize;
	}
}
