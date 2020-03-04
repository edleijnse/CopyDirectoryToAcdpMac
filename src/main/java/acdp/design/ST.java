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
 * CL#ofArray} method.
 *
 * @author Beat Hoermann
 */
public final class ST {
	/**
	 * Defines the constants used to indicate whether a column allows for the
	 * {@code null} value or not.
	 * 
	 * @author Beat Hörmann
	 */
	public enum Nulls {
		/**
		 * The column does not allows for the {@code null} value.
		 */
		NO_NULL(false),
		/**
		 * The column allows for the {@code null} value.
		 */
		NULLABLE(true);
		
		private final boolean value;
		
		private Nulls(boolean value) {
			this.value = value;
		}
		
		/**
		 * Returns the information whether a column allows for the {@code null}
		 * value.
		 * 
		 * @return The boolean value {@code true} if and only if the column allows
		 *         for the {@code null} value.
		 */
		public final boolean value() {
			return value;
		}
	}
	
	/**
	 * Defines the constants used to indicate the maximum number of characters
	 * of an {@linkplain Scheme#OUTROW outrow} string.
	 * 
	 * <ul>
	 * 	<li>SMALL: 255</li>
	 * 	<li>MEDIUM: 65 535</li>
	 * 	<li>LARGE: 16 777 215</li>
	 * 	<li>GIANT: 4 294 967 295</li>
	 * </ul>
	 * 
	 * Depending on the character set of the string the maximum number of
	 * characters may be less than the numbers given above.
	 * 
	 * @author Beat Hörmann
	 */
	public enum OutrowStringLength {
		/**
		 * Maximum outrow string length: 255 characters.
		 * Depending on the character set of the string the maximum number of
		 * characters may be less.
		 */
		SMALL(1),
		/**
		 * Maximum outrow string length: 65 535 characters.
		 * Depending on the character set of the string the maximum number of
		 * characters may be less.
		 */
		MEDIUM(2),
		/**
		 * Maximum outrow string length: 16 777 215 characters.
		 * Depending on the character set of the string the maximum number of
		 * characters may be less.
		 */
		LARGE(3),
		/**
		 * Maximum outrow string length: 4 294 967 295 characters.
		 * Depending on the character set of the string the maximum number of
		 * characters may be less.
		 */
		GIANT(4);
		
		private final int limit;
		
		private OutrowStringLength(int limit) {
			this.limit = limit;
		}
		
		/**
		 * Returns the limit.
		 * The limit of a string type is connected to the maximum number of
		 * characters of a string value as explained in the description of the
		 * {@link StringType} interface.
		 *
		 * @return The limit.
		 */
		public final int limit() {
			return limit;
		}
	}
	
	/**
	 * Returns the column type analogon of a Java {@code boolean} or {@code
	 * Boolean} type.
	 * It is safe to cast the returned value to the {@link acdp.types.BooleanType
	 * BooleanType} interface.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 *         
	 * @return The Boolean column type.
	 */
	public static final SimpleType<Boolean> beBoolean(Nulls nulls) {
		return TypeFactory.fetchBoolean(nulls.value());
	}
	
	/**
	 * Returns the column type analogon of a Java {@code byte} or {@code Byte}
	 * type.
	 * It is safe to cast the returned value to the {@link acdp.types.ByteType
	 * ByteType} interface.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 *         
	 * @return The Byte column type.
	 */
	public static final SimpleType<Byte> beByte(Nulls nulls) {
		return TypeFactory.fetchByte(nulls.value());
	}
	
	/**
	 * Returns the column type analogon of a Java {@code short} or {@code Short}
	 * type.
	 * It is safe to cast the returned value to the {@link acdp.types.ShortType
	 * ShortType} interface.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 *         
	 * @return The Short column type.
	 */
	public static final SimpleType<Short> beShort(Nulls nulls) {
		return TypeFactory.fetchShort(nulls.value());
	}
	
	/**
	 * Returns column type analogon of a Java {@code int} or {@code Integer}
	 * type.
	 * It is safe to cast the returned value to the {@link acdp.types.IntegerType
	 * IntegerType} interface.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 *         
	 * @return The Integer column type.
	 */
	public static final SimpleType<Integer> beInteger(Nulls nulls) {
		return TypeFactory.fetchInteger(nulls.value());
	}
	
	/**
	 * Returns the column type analogon of a Java {@code long} or {@code Long}
	 * type.
	 * It is safe to cast the returned value to the {@link acdp.types.LongType
	 * LongType} interface.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 *         
	 * @return The Long column type.
	 */
	public static final SimpleType<Long> beLong(Nulls nulls) {
		return TypeFactory.fetchLong(nulls.value());
	}
	
	/**
	 * Returns the column type analogon of a Java {@code float} or {@code Float}
	 * type.
	 * It is safe to cast the returned value to the {@link acdp.types.FloatType
	 * FloatType} interface.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 *         
	 * @return The Float column type.
	 */
	public static final SimpleType<Float> beFloat(Nulls nulls) {
		return TypeFactory.fetchFloat(nulls.value());
	}
	
	/**
	 * Returns the column type analogon of a Java {@code double} or {@code
	 * Double} type.
	 * It is safe to cast the returned value to the {@link acdp.types.DoubleType
	 * DoubleType} interface.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 *         
	 * @return The Double column type.
	 */
	public static final SimpleType<Double> beDouble(Nulls nulls) {
		return TypeFactory.fetchDouble(nulls.value());
	}
	
	/**
	 * Returns the column type analogon of a Java {@code String} type.
	 * It is safe to cast the returned value to the {@link StringType} interface.
	 * <p>
	 * Invoking this method has the same effect as invoking {@code
	 * beString(Nulls.NULLABLE, OutrowStringLength.GIANT)}.
	 * 
	 * @return The String column type.
	 */
	public static final SimpleType<String> beString() {
		return beString(Nulls.NULLABLE, OutrowStringLength.GIANT);
	}
	
	/**
	 * Returns the String column type with an {@linkplain Scheme#OUTROW outrow
	 * storage scheme} and applying the "UTF-8" charset for any byte conversions.
	 * It is safe to cast the returned value to the {@link StringType} interface.
	 * <p>
	 * Invoking this method has the same effect as invoking {@code
	 * beString(Charset.forName("UTF-8"), nulls, length)}.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 * @param  length The length of the string.
	 * 
	 * @return The String column type.
	 */
	public static final SimpleType<String> beString(Nulls nulls,
																	OutrowStringLength length) {
		return beString(Charset.forName("UTF-8"), nulls, length);
	}
	
	/**
	 * Returns the String column type with an {@linkplain Scheme#OUTROW outrow
	 * storage scheme} and applying the specified charset for any byte
	 * conversions.
	 * It is safe to cast the returned value to the {@link StringType} interface.
	 * 
	 * @param  charset The {@link Charset} to be used to encode a string value,
	 *         not allowed to be {@code null}.
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 * @param  length The length of the string.
	 *         
	 * @return The String column type.
	 * 
	 * @throws NullPointerException If {@code charset} is {@code null}.
	 */
	public static final SimpleType<String> beString(Charset charset,
											Nulls nulls, OutrowStringLength length) throws
																			NullPointerException {
		return TypeFactory.fetchString(charset, nulls.value(), Scheme.OUTROW,
																					length.limit());
	}
	
	/**
	 * Returns the String column type with an {@linkplain Scheme#INROW inrow
	 * storage scheme} and applying the specified charset for any byte
	 * conversions.
	 * It is safe to cast the returned value to the {@link StringType} interface.
	 * 
	 * @param  charset The {@link Charset} to be used to encode a string value,
	 *         not allowed to be {@code null}.
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 * @param  length The maximum number of characters of the String, must be
	 *         greater than or equal to 1 and less than or equal to {@code
	 *         Integer.MAX_VALUE} - 4.
	 *         Depending on the character set the maximum number of characters
	 *         may be less than this value.
	 *         
	 * @return The String column type.
	 * 
	 * @throws NullPointerException If {@code charset} is {@code null}.
	 * @throws IllegalArgumentException If {@code length} is less than 1 or
	 *         greater than {@code Integer.MAX_VALUE} - 4.
	 */
	public static final SimpleType<String> beString(Charset charset,
										Nulls nulls, int length) throws
										NullPointerException, IllegalArgumentException {
		return TypeFactory.fetchString(charset, nulls.value(), Scheme.INROW,
																								length);
	}
	
	/**
	 * Prevent object construction.
	 */
	private ST() {
	}
}
