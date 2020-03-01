/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.types;

import acdp.design.SimpleType;

/**
 * The array column type with elements of {@link SimpleType}.
 * <p>
 * There should be no need for clients to implement this interface.
 *
 * @author Beat Hoermann
 */
public interface ArrayType extends Type {
	/**
	 * Returns the maximum size of this array type, hence, the maximum number of
	 * elements in an array value of this array type
	 * 
	 * @return The maximum size of this array type, greater than or equal to 1.
	 */
	int maxSize();
	
	/**
	 * Returns the type of the elements of this array type.
	 * 
	 * @return The type of the elements of this array type, never {@code null}.
	 */
	SimpleType<?> elementType();
}
