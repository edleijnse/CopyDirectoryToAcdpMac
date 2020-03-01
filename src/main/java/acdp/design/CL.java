/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.design;

import java.nio.charset.Charset;

import acdp.Column;
import acdp.Ref;
import acdp.internal.Column_;
import acdp.internal.types.TypeFactory;
import acdp.types.*;
import acdp.types.Type.Scheme;

/**
 * Provides factory methods that return columns of various types.
 *
 * @author Beat Hoermann
 */
public final class CL {
	/**
	 * Creates a new column of type {@linkplain BooleanType Boolean}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link BooleanType} interface.
	 * 
	 * @param  nullable Must be set to {@code true} if the column allows for
	 *         {@code null} values, {@code false} if not.
	 *         Typically, values in columns forbidding the {@code null} value
	 *         can be persisted more efficiently than values in columns allowing
	 *         the {@code null} value.
	 * 
	 * @return The created column of type Boolean.
	 */
	public static final Column<Boolean> typeBoolean(boolean nullable){
		return new Column_<>(TypeFactory.fetchBoolean(nullable));
	}
	
	/**
	 * Creates a new column of type {@linkplain ByteType Byte}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link ByteType} interface.
	 * 
	 * @param  nullable Must be set to {@code true} if the column allows for
	 *         {@code null} values, {@code false} if not.
	 *         Typically, values in columns forbidding the {@code null} value
	 *         can be persisted more efficiently than values in columns allowing
	 *         the {@code null} value.
	 * 
	 * @return The created column of type Byte.
	 */
	public static final Column<Byte> typeByte(boolean nullable) {
		return new Column_<>(TypeFactory.fetchByte(nullable));
	}
	
	/**
	 * Creates a new column of type {@linkplain ShortType Short}.
	 * It is safe to cast {@code column.getInfo().type()} of the returned {@code
	 * column} to the {@link ShortType} interface.
	 * 
	 * @param  nullable Must be set to {@code true} if the column allows for
	 *         {@code null} values, {@code false} if not.
	 *         Typically, values in columns forbidding the {@code null} value
	 *         can be persisted more efficiently than values in columns allowing
	 *         the {@code null} value.
	 * 
	 * @return The created column of type Short.
	 */
	public static final Column<Short> typeShort(boolean nullable) {
		return new Column_<>(TypeFactory.fetchShort(nullable));
	}
	
	/**
	 * Creates a new column of type {@linkplain IntegerType Integer}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link IntegerType} interface.
	 * 
	 * @param  nullable Must be set to {@code true} if the column allows for
	 *         {@code null} values, {@code false} if not.
	 *         Typically, values in columns forbidding the {@code null} value
	 *         can be persisted more efficiently than values in columns allowing
	 *         the {@code null} value.
	 * 
	 * @return The created column of type Integer.
	 */
	public static final Column<Integer> typeInteger(boolean nullable){
		return new Column_<>(TypeFactory.fetchInteger(nullable));
	}
	
	/**
	 * Creates a new column of type {@linkplain LongType Long}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link LongType} interface.
	 * 
	 * @param  nullable Must be set to {@code true} if the column allows for
	 *         {@code null} values, {@code false} if not.
	 *         Typically, values in columns forbidding the {@code null} value
	 *         can be persisted more efficiently than values in columns allowing
	 *         the {@code null} value.
	 * 
	 * @return The created column of type Long.
	 */
	public static final Column<Long> typeLong(boolean nullable) {
		return new Column_<>(TypeFactory.fetchLong(nullable));
	}
	
	/**
	 * Creates a new column of type {@linkplain FloatType Float}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link FloatType} interface.
	 * 
	 * @param  nullable Must be set to {@code true} if the column allows for
	 *         {@code null} values, {@code false} if not.
	 *         Typically, values in columns forbidding the {@code null} value
	 *         can be persisted more efficiently than values in columns allowing
	 *         the {@code null} value.
	 * 
	 * @return The created column of type Float.
	 */
	public static final Column<Float> typeFloat(boolean nullable) {
		return new Column_<>(TypeFactory.fetchFloat(nullable));
	}

	/**
	 * Creates a new column of type {@linkplain DoubleType Double}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link DoubleType} interface.
	 * 
	 * @param  nullable Must be set to {@code true} if the column allows for
	 *         {@code null} values, {@code false} if not.
	 *         Typically, values in columns forbidding the {@code null} value
	 *         can be persisted more efficiently than values in columns allowing
	 *         the {@code null} value.
	 * 
	 * @return The created column of type Double.
	 */
	public static final Column<Double> typeDouble(boolean nullable) {
		return new Column_<>(TypeFactory.fetchDouble(nullable));
	}
	
	/**
	 * Creates a new column of type "classic Java" {@linkplain StringType
	 * String}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link StringType} interface.
	 * <p>
	 * Invoking this method has the same effect as invoking {@code
	 * typeString(true, 4)}.
	 * 
	 * @return The created column of type String.
	 */
	public static final Column<String> typeString() {
		return typeString(true, 4);
	}
	
	/**
	 * Creates a new column of type {@linkplain StringType String} with an
	 * {@linkplain Scheme#OUTROW outrow storage scheme} and applying the "UTF-8"
	 * charset for any byte conversions.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link StringType} interface.
	 * <p>
	 * Invoking this method has the same effect as invoking {@code
	 * typeString(Charset.forName("UTF-8"), nullable, Scheme.OUTROW, limit)}.
	 * 
	 * @param  nullable Must be set to {@code true} if the column allows for
	 *         {@code null} values, {@code false} if not.
	 *         Typically, values in columns forbidding the {@code null} value
	 *         can be persisted more efficiently than values in columns allowing
	 *         the {@code null} value.
	 * @param  limit The limit of the String, must be greater than or equal to 1
	 *         and less than or equal to 4.
	 *         Read the description of the {@link StringType} interface to
	 *         learn how the value of this parameter relates to the maximum
	 *         number of characters.
	 * 
	 * @return The created column of type String.
	 * 
	 * @throws IllegalArgumentException If {@code limit} is less than 1 or
	 *         greater than 4.
	 */
	public static final Column<String> typeString(boolean nullable,
												int limit) throws IllegalArgumentException {
		return typeString(Charset.forName("UTF-8"), nullable,
																			Scheme.OUTROW, limit);
	}
	
	/**
	 * Creates a new column of type {@linkplain StringType String}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link StringType} interface.
	 * 
	 * @param  charset The {@link Charset} to be used to encode a string value,
	 *         not allowed to be {@code null}.
	 * @param  nullable Must be set to {@code true} if the column allows for
	 *         {@code null} values, {@code false} if not.
	 *         Typically, values in columns forbidding the {@code null} value
	 *         can be persisted more efficiently than values in columns allowing
	 *         the {@code null} value.
	 * @param  scheme The storage scheme of this type, not allowed to be {@code
	 *         null}.
	 * @param  limit The limit of the String, must be greater than or equal to 1.
	 *         Read the description of the {@link StringType} interface to
	 *         learn how the value of this parameter relates to the maximum
	 *         number of characters.
	 * 
	 * @return The created column of type String.
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
	public static final Column<String> typeString(Charset charset,
										boolean nullable, Scheme scheme, int limit) throws
										NullPointerException, IllegalArgumentException {
		return new Column_<>(TypeFactory.fetchString(charset, nullable, scheme,
																							limit));
	}
	
	/**
	 * Creates a new column having the specified simple column type.
	 * <p>
	 * This method is typically used if the column type is a custom column type,
	 * hence, if the column can't be created with one of the {@code type}
	 * methods of this class.
	 * <p>
	 * Note that the type of the returned column may not be identical
	 * ({@code ==}) to the specified column type but the type descriptors will
	 * be equal ({@code String.equals}).
	 * This is because ACDP internally treats column types with same type
	 * descriptors as singletons.
	 * 
	 * @param  <T> The type of the column's values.
	 * 
	 * @param  simpleType The simple column type, not allowed to be {@code null}.
	 * 
	 * @return The created column having the specified simple type.
	 * 
	 * @throws NullPointerException If {@code simpleType} is {@code null}.
	 */
	public static final <T> Column<T> create(SimpleType<T> simpleType) throws
																			NullPointerException {
		return new Column_<>(TypeFactory.getFromCache(simpleType));
	}
	
	/**
	 * Creates a new column of type {@linkplain ArrayType array} with elements
	 * of the specified element type.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link ArrayType} interface.
	 * <p>
	 * Consider using the {@link ST} simple column type factory if the element
	 * type should be a built-in simple column type.
	 * If you are not using the {@code ST} simple column type factory then note
	 * that the element type of the returned array column may not be identical
	 * ({@code ==}) to the specified element type but the type descriptors will
	 * be equal ({@code String.equals}).
	 * This is because ACDP internally handles column types with same type
	 * descriptors as singletons.
	 * 
	 * @param  <T> The type of the elements.
	 *
	 * @param  scheme The storage scheme of the type, not allowed to be
	 *         {@code null}.
	 * @param  maxSize The maximum number of elements in an array value of this
	 *         array type.
	 * @param  elementType The type of the elements of the array, not allowed
	 *         to be {@code null}.
	 * 
	 * @return The created array type column.
	 *
	 * @throws NullPointerException If {@code scheme} or {@code elementType} is
	 *         {@code null}.
	 * @throws IllegalArgumentException If {@code maxSize} is less than 1.
	 */
	public static final <T> Column<T[]> typeArray(Scheme scheme, int maxSize,
															SimpleType<T> elementType) throws
										NullPointerException, IllegalArgumentException {
		return new Column_<>(TypeFactory.fetchArrayType(scheme, maxSize,
																						elementType));
	}
	
	/**
	 * Creates a new column having the {@linkplain RefType reference column
	 * type}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link RefType} interface.
	 * 
	 * @return The created column.
	 */
	public static final Column<Ref> typeRef() {
		return new Column_<>(TypeFactory.fetchRef());
	}
	
	/**
	 * Creates a new column having an {@linkplain ArrayOfRefType array of
	 * references column type}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link ArrayOfRefType} interface.
	 * 
	 * @param  scheme The storage scheme of the type, not allowed to be
	 *         {@code null}.
	 * @param  maxSize The maximum number of elements in an array value of this
	 *         array type.
	 * 
	 * @return The created column of type array with elements being {@linkplain
	 *         Ref references}.
	 *
	 * @throws NullPointerException If {@code scheme} is {@code null}.
	 * @throws IllegalArgumentException If {@code maxSize} is less than 1.
	 */
	public static final Column<Ref[]> typeArrayOfRef(Scheme scheme,
													int maxSize) throws NullPointerException,
																		IllegalArgumentException {
		return new Column_<>(TypeFactory.fetchArrayOfRefType(scheme, maxSize));
	}
	
	/**
	 * Prevent object construction.
	 */
	private CL() {
	}
}
