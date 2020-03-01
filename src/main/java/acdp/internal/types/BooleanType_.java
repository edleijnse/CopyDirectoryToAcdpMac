/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.types;

import acdp.design.SimpleType;
import acdp.exceptions.CreationException;
import acdp.types.BooleanType;

/**
 * The column type analogon of a Java {@code boolean} or {@code Boolean} type.
 *
 * @author Beat Hoermann
 */
public final class BooleanType_ extends SimpleType<Boolean> implements
																						BooleanType {
	/**
	 * The "kind" is the first character of a built-in column type.
	 * The value uniquely identifies this built-in column type.
	 */
	static final char KIND = 'a';
	
	/**
	 * Returns the {@linkplain acdp.types.Type#typeDesc() type descriptor}
	 * of a Boolean column type constructed with the specified argument.
	 * 
	 * @param  nullable Must be equal to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         
	 * @return The type descriptor, never {@code null}.
	 */
	static final String typeDesc(boolean nullable) {
		return KIND + typeDescSuffix(Scheme.INROW, nullable, 1, false);
	}
	
	/**
	 * Creates a Boolean column type from the specified type descriptor.
	 * 
	 * @param  typeDesc The type descriptor.
	 * 
	 * @return The created Boolean column type, never {@code null}.
	 *         
	 * @throws CreationException If the type descriptor is invalid.
	 */
	static final BooleanType_ createType(String typeDesc) throws
																				CreationException {
		return new BooleanType_(new TypeDesc(typeDesc).nullable);
	}
	
	/**
	 * The constructor.
	 * A Java {@code Boolean} type is nullable whereas a Java {@code boolean} is
	 * not.
	 * 
	 * @param nullable Must be equal to {@code true} if the type allows the
	 *        {@code null} value, {@code false} if not.
	 */
	BooleanType_(boolean nullable) {
		super(Boolean.class, Scheme.INROW, nullable, 1, false);
	}
	
	@Override
	protected final String typeDescPrefix() {
		return "" + KIND;
	}

	@Override
	public final byte[] toBytes(Boolean val) {
		return val ? new byte[]{ 1 } : new byte[]{ 0 };
	}
	
	@Override
	protected final int toBytes(Boolean val, byte[] bytes, int offset) throws
										NullPointerException, IndexOutOfBoundsException {
		bytes[offset] = val ? (byte) 1 : (byte) 0;
		return length;
	}

	@Override
	public final Boolean fromBytes(byte[] bytes, int offset, int len) throws
																	IndexOutOfBoundsException {
		return bytes[offset] == 1 ? Boolean.TRUE : Boolean.FALSE;
	}
}
