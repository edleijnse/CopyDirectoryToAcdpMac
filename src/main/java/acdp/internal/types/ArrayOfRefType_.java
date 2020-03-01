/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.types;

import java.lang.reflect.Array;

import acdp.exceptions.CreationException;
import acdp.internal.Ref_;
import acdp.types.ArrayOfRefType;

/**
 * Implements an array column type with elements being {@linkplain acdp.Ref
 * references}.
 *
 * @author Beat Hoermann
 */
public final class ArrayOfRefType_ extends AbstractArrayType implements
																					ArrayOfRefType {
	/**
	 * The "kind" is the first character of a built-in column type.
	 * The value uniquely identifies this built-in column type.
	 */
	static final char KIND = 'y';
	
	/**
	 * Returns the {@linkplain acdp.types.Type#typeDesc() type descriptor}
	 * of an array of references column type constructed with the specified
	 * arguments.
	 * 
	 * @param  scheme The storage scheme of the type, not allowed to be
	 *         {@code null}.
	 * @param  maxSize The maximum number of elements in an array value of this
	 *         array type.
	 *         
	 * @return The type descriptor, never {@code null}.
	 */
	static final String typeDesc(Scheme scheme, int maxSize) {
		return KIND + (scheme == Scheme.INROW ? "i" : "o") + maxSize;
	}
	
	/**
	 * Creates an array of references column type from the specified type
	 * descriptor.
	 * 
	 * @param  typeDesc The type descriptor.
	 * 
	 * @return The created array of references column type, never {@code null}.
	 *         
	 * @throws CreationException If the type descriptor is invalid.
	 */
	static final ArrayOfRefType_ createType(String typeDesc) throws
																				CreationException {
		if (typeDesc == null || typeDesc.length() < 3 ||
												typeDesc.charAt(0) != ArrayOfRefType_.KIND)
			throw new CreationException("Invalid type descriptor: \"" + typeDesc +
																								"\".");
		else {
			final Scheme scheme;
			final char schemeChar = typeDesc.charAt(1);
			if (schemeChar == 'i')
				scheme = Scheme.INROW;
			else if (schemeChar == 'o')
				scheme = Scheme.OUTROW;
			else {
				throw new CreationException("Invalid type descriptor: \"" +
																				typeDesc + "\".");
			}
			
			final ArrayOfRefType_ type;
			try {
				final int maxSize = Integer.parseInt(typeDesc.substring(2));
				type = new ArrayOfRefType_(scheme, maxSize);
			} catch (Exception e) {
				throw new CreationException("Invalid type descriptor: \"" +
																				typeDesc + "\".");
			}
				
			return type;
		}
	}
	
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
	ArrayOfRefType_(Scheme scheme, int maxSize) throws
										NullPointerException, IllegalArgumentException {
		super(scheme, maxSize);
	}
	
	@Override
	public final String typeDesc() {
		return typeDesc(scheme, maxSize);
	}
	
	/**
	 * Tests if the specified value is compatible with this type.
	 * In accordance with the general definition of {@linkplain
	 * acdp.types.Type#isCompatible compatibility} the specified value is
	 * compatible with this type if and only if it is equal to {@code null} or,
	 * provided that the value is not equal to {@code null}, the following
	 * conditions are satisfied:
	 * 
	 * <ul>
	 * 	<li>The specified value is an array.</li>
	 * 	<li>The component type of the specified array value is equal to the
	 *        {@code Ref_} class.</li>
	 * 	<li>The number of elements of the specified array value is less than
	 *        or equal to the maximum size as returned by the {@link #maxSize()}
	 *        method.</li>
	 * </ul>
	 * <p>
	 * Note that even if this method returns {@code true} this is not a
	 * guarantee that the elements of the specified array value are indeed
	 * valid references to existing rows of the referenced table.
	 * 
	 * @param  val The value to test for compatibility.
	 * @return The boolean value {@code true} if the value is compatible with
	 *         this type, {@code false} otherwise.
	 */
	@Override
	public final boolean isCompatible(Object val) {
		return val == null || val.getClass().isArray() && val.getClass().
				getComponentType() == Ref_.class && Array.getLength(val) <= maxSize;
	}
}
