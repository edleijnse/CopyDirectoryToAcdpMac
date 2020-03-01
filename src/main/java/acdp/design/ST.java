/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.design;

import java.nio.charset.Charset;

import acdp.internal.types.TypeFactory;
import acdp.types.StringType;
import acdp.types.Type.Scheme;

/**
 * Provides factory methods which return a built-in {@linkplain SimpleType
 * simple column type} that can be used as third argument of the {@link
 * CL#typeArray} method.
 *
 * @author Beat Hoermann
 */
public final class ST {
	/**
	 * Returns the column type analogon of a Java {@code boolean} or {@code
	 * Boolean} type.
	 * It is safe to cast the returned value to the {@link acdp.types.BooleanType
	 * BooleanType} interface.
	 * 
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         Typically, values of types forbidding the {@code null} value can
	 *         be persisted more efficiently than values of types allowing the
	 *         {@code null} value.
	 *         
	 * @return The Boolean column type.
	 */
	public static final SimpleType<Boolean> beBoolean(boolean nullable) {
		return TypeFactory.fetchBoolean(nullable);
	}
	
	/**
	 * Returns the column type analogon of a Java {@code byte} or {@code Byte}
	 * type.
	 * It is safe to cast the returned value to the {@link acdp.types.ByteType
	 * ByteType} interface.
	 * 
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         Typically, values of types forbidding the {@code null} value can
	 *         be persisted more efficiently than values of types allowing the
	 *         {@code null} value.
	 *         
	 * @return The Byte column type.
	 */
	public static final SimpleType<Byte> beByte(boolean nullable) {
		return TypeFactory.fetchByte(nullable);
	}
	
	/**
	 * Returns the column type analogon of a Java {@code short} or {@code Short}
	 * type.
	 * It is safe to cast the returned value to the {@link acdp.types.ShortType
	 * ShortType} interface.
	 * 
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         Typically, values of types forbidding the {@code null} value can
	 *         be persisted more efficiently than values of types allowing the
	 *         {@code null} value.
	 *         
	 * @return The Short column type.
	 */
	public static final SimpleType<Short> beShort(boolean nullable) {
		return TypeFactory.fetchShort(nullable);
	}
	
	/**
	 * Returns column type analogon of a Java {@code int} or {@code Integer}
	 * type.
	 * It is safe to cast the returned value to the {@link acdp.types.IntegerType
	 * IntegerType} interface.
	 * 
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         Typically, values of types forbidding the {@code null} value can
	 *         be persisted more efficiently than values of types allowing the
	 *         {@code null} value.
	 *         
	 * @return The Integer column type.
	 */
	public static final SimpleType<Integer> beInteger(boolean nullable) {
		return TypeFactory.fetchInteger(nullable);
	}
	
	/**
	 * Returns the column type analogon of a Java {@code long} or {@code Long}
	 * type.
	 * It is safe to cast the returned value to the {@link acdp.types.LongType
	 * LongType} interface.
	 * 
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         Typically, values of types forbidding the {@code null} value can
	 *         be persisted more efficiently than values of types allowing the
	 *         {@code null} value.
	 *         
	 * @return The Long column type.
	 */
	public static final SimpleType<Long> beLong(boolean nullable) {
		return TypeFactory.fetchLong(nullable);
	}
	
	/**
	 * Returns the column type analogon of a Java {@code float} or {@code Float}
	 * type.
	 * It is safe to cast the returned value to the {@link acdp.types.FloatType
	 * FloatType} interface.
	 * 
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         Typically, values of types forbidding the {@code null} value can
	 *         be persisted more efficiently than values of types allowing the
	 *         {@code null} value.
	 *         
	 * @return The Float column type.
	 */
	public static final SimpleType<Float> beFloat(boolean nullable) {
		return TypeFactory.fetchFloat(nullable);
	}
	
	/**
	 * Returns the column type analogon of a Java {@code double} or {@code
	 * Double} type.
	 * It is safe to cast the returned value to the {@link acdp.types.DoubleType
	 * DoubleType} interface.
	 * 
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         Typically, values of types forbidding the {@code null} value can
	 *         be persisted more efficiently than values of types allowing the
	 *         {@code null} value.
	 *         
	 * @return The Double column type.
	 */
	public static final SimpleType<Double> beDouble(boolean nullable) {
		return TypeFactory.fetchDouble(nullable);
	}
	
	/**
	 * Returns the column type analogon of a Java {@code String} type.
	 * In contrast to the Java {@code String} type this type may be specified
	 * such that the {@code null} value is forbidden.
	 * <p>
	 * Invoking this method has the same effect as invoking {@code beString(true,
	 * 4)}.
	 * 
	 * @return The String column type.
	 */
	public static final SimpleType<String> beString() {
		return beString(true, 4);
	}
	
	/**
	 * Returns the String column type with an {@linkplain Scheme#OUTROW outrow
	 * storage scheme} and applying the "UTF-8" charset for any byte conversions.
	 * <p>
	 * Invoking this method has the same effect as invoking {@code
	 * beString(Charset.forName("UTF-8"), nullable, Scheme.OUTROW, limit)}.
	 * 
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         Typically, values of types forbidding the {@code null} value can
	 *         be persisted more efficiently than values of types allowing the
	 *         {@code null} value.
	 * @param  limit The limit of the string, must be greater than or equal to 1
	 *         and less than or equal to 4.
	 *         Read the description of the {@link StringType} interface to
	 *         learn how the value of this parameter relates to the maximum
	 *         number of characters.
	 * 
	 * @return The String column type.
	 * 
	 * @throws IllegalArgumentException If {@code limit} is less than 1 or
	 *         greater than 4.
	 */
	public static final SimpleType<String> beString(boolean nullable,
												int limit) throws IllegalArgumentException {
		return beString(Charset.forName("UTF-8"), nullable, Scheme.OUTROW, limit);
	}
	
	/**
	 * Returns the String column type with the specified properties.
	 * It is safe to cast the returned value to the {@link StringType} interface.
	 * 
	 * @param  charset The {@link Charset} to be used to encode a string value,
	 *         not allowed to be {@code null}.
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         Typically, values of types forbidding the {@code null} value can
	 *         be persisted more efficiently than values of types allowing the
	 *         {@code null} value.
	 * @param  scheme The storage scheme of this type, not allowed to be {@code
	 *         null}.
	 * @param  limit The limit of the String, must be greater than or equal to 1.
	 *         Read the description of the {@link StringType} interface to
	 *         learn how the value of this parameter relates to the maximum
	 *         number of characters.
	 *         
	 * @return The String column type.
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
	public static final SimpleType<String> beString(Charset charset,
										boolean nullable, Scheme scheme, int limit) throws
										NullPointerException, IllegalArgumentException {
		return TypeFactory.fetchString(charset, nullable, scheme, limit);
	}
	
	/**
	 * Prevent object construction.
	 */
	private ST() {
	}
}
