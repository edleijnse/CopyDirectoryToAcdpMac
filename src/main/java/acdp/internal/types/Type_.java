/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.types;

import java.util.Objects;

import acdp.types.Type;

/**
 * The super class of all column types.
 *
 * @author Beat Hoermann
 */
public abstract class Type_ implements Type {
	/**
	 * Determines if the type described by the specified type descriptor is a
	 * built-in type.
	 * By convention, a type is a built-in type if and only if its type
	 * descriptor does not contain an upper case character.
	 * <p>
	 * Note that an array type with an element type being a built-in or custom
	 * column type is itself a built-in or custom column type, respectively.
	 * 
	 * @param  typeDesc The type descriptor, not allowed to be {@code null} and
	 *         not allowed to be an empty string.
	 * @return The boolean value {@code true} if the type descriptor describes
	 *         a built-in type, {@code false} otherwise.
	 */
	public static final boolean isBuiltInType(String typeDesc) {
		boolean foundUpperCase = false;
		int i = 0;
		while (i < typeDesc.length() && !foundUpperCase) {
			foundUpperCase = Character.isUpperCase(typeDesc.charAt(i++));
		}
		return !foundUpperCase;
	}
	
	/**
	 * The storage scheme, never {@code null}.
	 */
	protected final Scheme scheme;
	
	/**
	 * The constructor.
	 * 
	 * @param  scheme The storage scheme of the type, not allowed to be
	 *         {@code null}.
	 * @throws NullPointerException If {@code scheme} is {@code null}.
	 */
	protected Type_(Scheme scheme) throws NullPointerException {
		this.scheme = Objects.requireNonNull(scheme, "The value for the "+
														"storage is not allowed to be null.");
	}
	
	@Override
	public final Scheme scheme() {
		return scheme;
	}
}
