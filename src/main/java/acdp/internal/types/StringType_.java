/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.types;

import java.nio.charset.Charset;
import java.util.Objects;

import acdp.design.SimpleType;
import acdp.exceptions.CreationException;
import acdp.types.StringType;

/**
 * See {@link StringType}.
 *
 * @author Beat Hoermann
 */
public final class StringType_ extends SimpleType<String> implements
																						StringType {
	/**
	 * The "kind" is the first character of a built-in column type.
	 * The value uniquely identifies this built-in column type.
	 */
	static final char KIND = 's';
	
	/**
	 * Returns the first part of the type descriptor of a {@code StringType_}
	 * constructed with the specified character set.
	 * 
	 * @param  charset The {@link Charset} to be used to encode a string value,
	 *         not allowed to be {@code null}. 
	 * 
	 * @return The prefix of the type descriptor, never {@code null} and never
	 *         an empty string.
	 */
	private static final String typeDescPrefix(Charset charset) {
		return KIND + (charset.name().equals("UTF-8") ? "" : charset.name());
	}
	
	/**
	 * Returns the {@linkplain acdp.types.Type#typeDesc() type descriptor}
	 * of a String column type constructed with the specified arguments.
	 * 
	 * @param  charset The {@link Charset} to be used to encode a string value,
	 *         not allowed to be {@code null}.
	 * @param  nullable Must be equal to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 * @param  scheme The storage scheme of this type, not allowed to be {@code
	 *         null}.
	 * @param  limit The limit of the String, must be greater than or equal to 1.
	 *         Read the description of the {@link StringType} interface to
	 *         learn how the value of this parameter relates to the maximum
	 *         number of characters.
	 *         
	 * @return The type descriptor, never {@code null}.
	 */
	static final String typeDesc(Charset charset, boolean nullable,
																	Scheme scheme, int limit) {
		return typeDescPrefix(charset) + typeDescSuffix(scheme, nullable, limit,
																								true);
	}
	
	/**
	 * Creates a String column type from the specified type descriptor.
	 * 
	 * @param  typeDesc The type descriptor.
	 * 
	 * @return The created String column type, never {@code null}.
	 *         
	 * @throws CreationException If the type descriptor is invalid.
	 */
	static final StringType_ createType(String typeDesc) throws
																				CreationException {
		final TypeDesc td = new TypeDesc(typeDesc);
		
		final String prefix = td.prefix;
		
		if (prefix.charAt(0) != KIND) {
			throw new CreationException("Invalid type descriptor: \"" + typeDesc +
																								"\".");
		};
		
		StringType_ type;
		try {
			final Charset charset = prefix.length() <= 1 ?
					Charset.forName("UTF-8") : Charset.forName(prefix.substring(1));
			type = new StringType_(charset, td.nullable, td.scheme, td.limit);
		} catch (Exception e) {
			throw new CreationException("Invalid type descriptor: \"" + typeDesc +
																								"\".");
		}
				
		return type;
	}
	
	/**
	 * The charset which is used to encode a string value. 
	 */
	private final Charset charset;
	
	/**
	 * The limit of the string type.
	 */
	private final int limit;
	
	/**
	 * The constructor.
	 * 
	 * @param  charset The {@link Charset} to be used to encode a string value,
	 *         not allowed to be {@code null}.
	 * @param  nullable Must be equal to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 * @param  scheme The storage scheme of this type, not allowed to be {@code
	 *         null}.
	 * @param  limit The limit of the String, must be greater than or equal to 1.
	 *         Read the description of the {@link StringType} interface to
	 *         learn how the value of this parameter relates to the maximum
	 *         number of characters.
	 *         
	 * @throws NullPointerException If {@code scheme} or {@code charset} are
	 *         {@code null}.
	 * @throws IllegalArgumentException If {@code limit} is less than 1 or,
	 *         provided that {@code scheme} is equal to {@code Scheme.OUTROW},
	 *         {@code limit} is greater than 4.
	 *         Furthermore, this exception is thrown if {@code scheme} is equal
	 *         to {@code Scheme.INROW} and {@code limit} is greater than
	 *         {@code Integer.MAX_VALUE} - 4.
	 */
	StringType_(Charset charset, boolean nullable, Scheme scheme,
				int limit) throws NullPointerException, IllegalArgumentException {
		super(String.class, scheme, nullable, limit, true);
		this.charset = Objects.requireNonNull(charset,
															"Charset not allowed to be null.");
		this.limit = limit;
	}
	
	@Override
	protected final String typeDescPrefix() {
		return typeDescPrefix(charset);
	}
	
	@Override
	public final Charset charset() {
		return charset;
	}
	
	@Override
	public final int limit() {
		return limit;
	}
	
	@Override
	protected final byte[] toBytes(String val) throws NullPointerException {
		return val.getBytes(charset);
	}

	@Override
	protected final String fromBytes(byte[] bytes, int offset, int len) throws
																	IndexOutOfBoundsException {
		return new String(bytes, offset, len, charset);
	}
}
