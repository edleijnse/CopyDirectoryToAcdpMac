/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.types;

import java.nio.ByteBuffer;

import acdp.design.SimpleType;
import acdp.exceptions.CreationException;
import acdp.types.ShortType;


/**
 * The column type analogon of a Java {@code short} or {@code Short} type.
 *
 * @author Beat Hoermann
 */
public final class ShortType_ extends SimpleType<Short> implements ShortType {
	/**
	 * The "kind" is the first character of a built-in column type.
	 * The value uniquely identifies this built-in column type.
	 */
	static final char KIND = 'h';
	
	/**
	 * The size in bytes of this type.
	 */
	private static final int SIZE = (int) Math.ceil(Short.SIZE / 8.0);
	
	/**
	 * Returns the {@linkplain acdp.types.Type#typeDesc() type descriptor}
	 * of a Short column type constructed with the specified argument.
	 * 
	 * @param  nullable Must be equal to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         
	 * @return The type descriptor, never {@code null}.
	 */
	static final String typeDesc(boolean nullable) {
		return KIND + typeDescSuffix(Scheme.INROW, nullable, SIZE, false);
	}
	
	/**
	 * Creates a Short column type from the specified type descriptor.
	 * 
	 * @param  typeDesc The type descriptor.
	 * 
	 * @return The created Short column type, never {@code null}.
	 *         
	 * @throws CreationException If the type descriptor is invalid.
	 */
	static final ShortType_ createType(String typeDesc) throws CreationException {
		return new ShortType_(new TypeDesc(typeDesc).nullable);
	}
	
	/**
	 * The constructor.
	 * A Java {@code Short} type is nullable whereas a Java {@code short} is
	 * not.
	 * 
	 * @param nullable Must be equal to {@code true} if the type allows the
	 *        {@code null} value, {@code false} if not.
	 */
	ShortType_(boolean nullable) {
		super(Short.class, Scheme.INROW, nullable, SIZE, false);
	}
	
	@Override
	protected final String typeDescPrefix() {
		return "" + KIND;
	}
	
	@Override
	public final byte[] toBytes(Short val) {
		return ByteBuffer.allocate(length).putShort(val).array();
	}
	
	@Override
	protected final int toBytes(Short val, byte[] bytes, int offset) throws
										NullPointerException, IndexOutOfBoundsException {
		ByteBuffer.wrap(bytes, offset, length).putShort(val);
		return length;
	}
	
	@Override
	public final Short fromBytes(byte[] bytes, int offset, int len) throws
																	IndexOutOfBoundsException {
		return ByteBuffer.wrap(bytes, offset, length).getShort();
	}
}
