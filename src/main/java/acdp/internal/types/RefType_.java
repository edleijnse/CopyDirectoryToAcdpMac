/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.types;

import acdp.exceptions.CreationException;
import acdp.internal.Ref_;
import acdp.types.RefType;

/**
 * A value of an RT column is a {@linkplain acdp.Ref reference} to a
 * particular row of some table.
 * <p>
 * A reference may be {@code null} and the length of its byte representation is
 * equal to or less than 8.
 * (The exact number of bytes used to persist a reference depends on the
 * maximum number of rows allowed in the referenced table.)
 * <p>
 * The {@code RefType_} class does not extend {@code SimpleType} because of
 * the following three reasons:
 * 
 * <ol>
 *    <li>The length of a reference is not known at the time of column type
 *        creation.</li>
 *    <li>I want a reference to be stored similar to a value of {@code
 *        SimpleType} that does not allow the {@code null} value, but I want to
 *        allow the {@code null} value for references.</li>
 *    <li>Inserting, deleting and updating references require the correct
 *        handling of the reference counters of the referenced rows which
 *        make these operations different when applying them to references
 *        instead of applying them to values of {@code SimpleType}.</li>
 * </ol>
 *
 * @author Beat Hoermann
 */
public final class RefType_ extends Type_ implements RefType {
	/**
	 * The "kind" is the first character of a built-in column type.
	 * The value uniquely identifies this built-in column type.
	 */
	static final char KIND = 'r';
	
	/**
	 * Returns the {@linkplain acdp.types.Type#typeDesc() type descriptor}
	 * of a reference column type.
	 *         
	 * @return The type descriptor, never {@code null}.
	 */
	static final String refTypeDesc() {
		return "" + KIND;
	}
	
	/**      
	 * Creates a reference column type from the specified type descriptor.
	 * 
	 * @param  typeDesc The type descriptor.
	 * 
	 * @return The created reference column type, never {@code null}.
	 *         
	 * @throws CreationException If the type descriptor is invalid.
	 */
	static final RefType_ createType(String typeDesc) {
		if (typeDesc == null || typeDesc.length() != 1 || typeDesc.charAt(0) !=
																						RefType_.KIND)
			throw new CreationException("Invalid type descriptor: \"" + typeDesc +
																								"\".");
		else {
			return new RefType_();
		}
	}
	
	/**
	 * The constructor.
	 */
	RefType_() {
		super(Scheme.INROW);
	}
	
	@Override
	public final String typeDesc() {
		return refTypeDesc();
	}

	/**
	 * Tests if the specified value is compatible with this type.
	 * In accordance with the general definition of {@linkplain
	 * acdp.types.Type#isCompatible compatibility} the specified value is
	 * compatible with this type if and only if it is {@code null} <em>or</em>
	 * it is an instance of the {@code Ref_} class.
	 * <p>
	 * Note that even if this method returns {@code true} this it is not a
	 * guarantee that the specified value is indeed a valid reference to an
	 * existing row of the referenced table.
	 * 
	 * @param  val The value to test for compatibility.
	 * @return The boolean value {@code true} if the value is compatible with
	 *         this type, {@code false} otherwise.
	 */
	@Override
	public final boolean isCompatible(Object val) {
		return val == null || val instanceof Ref_;
	}
}
