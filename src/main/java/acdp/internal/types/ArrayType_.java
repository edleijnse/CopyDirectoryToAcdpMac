/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.types;

import java.lang.reflect.Array;
import java.util.Objects;

import acdp.design.SimpleType;
import acdp.exceptions.CreationException;
import acdp.misc.Utils;
import acdp.types.ArrayType;

/**
 * Implements an array column type with elements being instances of the {@link
 * SimpleType} class.
 *
 * @author Beat Hoermann
 */
public final class ArrayType_ extends AbstractArrayType implements ArrayType {
	/**
	 * The "kind" is the first character of a built-in column type.
	 * The value uniquely identifies this built-in column type.
	 */
	static final char KIND = 'z';
	
	/**
	 * Returns the {@linkplain acdp.types.Type#typeDesc() type descriptor}
	 * of an array column type constructed with the specified arguments.
	 * 
	 * @param  scheme The storage scheme of the type, not allowed to be
	 *         {@code null}.
	 * @param  maxSize The maximum number of elements in an array value of this
	 *         array type.
	 * @param  elementType The type of the elements of the array, not allowed
	 *         to be {@code null}.
	 *         
	 * @return The type descriptor, never {@code null}.
	 */
	static final String typeDesc(Scheme scheme, int maxSize,
																	SimpleType<?> elementType) {
		return KIND + (scheme == Scheme.INROW ? "i" : "o") + maxSize +
																			elementType.typeDesc();
	}
	
	/**
	 * Compute the index where the type descriptor of the element type of the
	 * array column type is located.
	 * 
	 * @param  typeDesc The type descriptor of the array column type.
	 * 
	 * @return The index of the type descriptor of the element tpye or 0 if such
	 *         an index could not be found.
	 */
	private static final int getIdxET(String typeDesc) {
		int idxET = 0;
		int k = 3;
		while (k < typeDesc.length() && idxET == 0) {
			if (Character.isLetter(typeDesc.charAt(k))) {
				idxET = k;
			}
			k++;
		}
		return idxET;
	}
	
	/**
	 * Creates an array column type from the specified type descriptor and the
	 * specified element type.
	 * 
	 * @param  typeDesc The type descriptor.
	 * @param  elementType The type of the elements of the array.
	 * 
	 * @return The created array column type, never {@code null}.
	 *         
	 * @throws CreationException If the type descriptor is invalid.
	 */
	static final ArrayType_ createType(String typeDesc,
									SimpleType<?> elementType) throws CreationException {
		if (typeDesc == null || typeDesc.length() < 4 ||
						typeDesc.charAt(0) != ArrayType_.KIND || elementType == null)
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
			
			final ArrayType_ type;
			try {
				final int idxET = getIdxET(typeDesc);
				final int maxSize = Integer.parseInt(typeDesc.substring(2, idxET));
				type = new ArrayType_(scheme, maxSize, elementType);
			} catch (Exception e) {
				throw new CreationException("Invalid type descriptor: \"" +
																				typeDesc + "\".");
			}
				
			return type;
		}
	}
	
	/**
	 * Extracts the type descriptor of the element type from the specified
	 * array column type descriptor.
	 * 
	 * @param  typeDesc The type descriptor of the array column type.
	 * 
	 * @return The type descriptor of the element type or {@code null} if the
	 *         specified type descriptor is {@code null} or invalid.
	 *         Never an empty string.
	 */
	static final String getTypeDescET(String typeDesc) {
		if (typeDesc == null || typeDesc.length() < 4 ||
														typeDesc.charAt(0) != ArrayType_.KIND)
			return null;
		else {
			final int idxET = getIdxET(typeDesc);
			
			if (idxET == 0)
				return null;
			else {
				return typeDesc.substring(idxET);
			}
		}
	}
	
	/**
	 * The type of the array elements.
	 */
	private final SimpleType<?> elementType;
	
	/**
	 * The constructor
	 * 
	 * @param  scheme The storage scheme of the type, not allowed to be
	 *         {@code null}.
	 * @param  maxSize The maximum number of elements in an array value of this
	 *         array type.
	 * @param  elementType The type of the elements of the array, not allowed
	 *         to be {@code null}.
	 *         
	 * @throws NullPointerException If {@code scheme} or {@code elementType} is
	 *         {@code null}.
	 * @throws IllegalArgumentException If {@code maxSize} is less than 1.
	 */
	ArrayType_(Scheme scheme, int maxSize, SimpleType<?> elementType)
								throws NullPointerException, IllegalArgumentException {
		super(scheme, maxSize);
		this.elementType = Objects.requireNonNull(elementType, "The value for " +
											"the element type is not allowed to be null.");
	}
	
	@Override
	public final String typeDesc() {
		return typeDesc(scheme, maxSize, elementType);
	}
	
	/**
	 * Returns the type of the elements of this array.
	 * 
	 * @return The type of the elements of this array, never {@code null}.
	 */
	@Override
	public final SimpleType<?> elementType() {
		return elementType;
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
	 * 	<li>If at least one element of the array value is equal to {@code null}
	 *        then the {@code elementType().nullable()} method returns
	 *        {@code true}.</li>
	 * 	<li>The component type of the specified array value is equal to the
	 *        <em>value type</em> of the simple type returned by the {@code
	 *        elementType()} method.</li>
	 * 	<li>The number of elements of the specified array value is less than
	 *        or equal to the maximum size as returned by the {@link #maxSize()}
	 *        method.</li>
	 * </ul>
	 * 
	 * @param  val The value to test for compatibility.
	 * @return The boolean value {@code true} if the value is compatible with
	 *         this array type, {@code false} otherwise.
	 */
	@Override
	public final boolean isCompatible(Object val) {
		return val == null || val.getClass().isArray() &&
					val.getClass().getComponentType() == elementType.valueType() &&
					Array.getLength(val) <= maxSize && (elementType.nullable() ||
					!Utils.hasNull((Object[]) val));
	}
}
