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
import acdp.design.ST.Nulls;
import acdp.design.ST.OutrowStringLength;
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
	 * column} to the {@link BooleanType} interface or to the {@link SimpleType}
	 * class.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 * 
	 * @return The created column of type Boolean.
	 */
	public static final Column<Boolean> ofBoolean(Nulls nulls){
		return new Column_<>(TypeFactory.fetchBoolean(nulls.value()));
	}
	
	/**
	 * Creates a new column of type {@linkplain ByteType Byte}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link ByteType} interface or to the {@link SimpleType}
	 * class.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 * 
	 * @return The created column of type Byte.
	 */
	public static final Column<Byte> ofByte(Nulls nulls) {
		return new Column_<>(TypeFactory.fetchByte(nulls.value()));
	}
	
	/**
	 * Creates a new column of type {@linkplain ShortType Short}.
	 * It is safe to cast {@code column.getInfo().type()} of the returned {@code
	 * column} to the {@link ShortType} interface or to the {@link SimpleType}
	 * class.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 * 
	 * @return The created column of type Short.
	 */
	public static final Column<Short> ofShort(Nulls nulls) {
		return new Column_<>(TypeFactory.fetchShort(nulls.value()));
	}
	
	/**
	 * Creates a new column of type {@linkplain IntegerType Integer}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link IntegerType} interface or to the {@link SimpleType}
	 * class.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 * 
	 * @return The created column of type Integer.
	 */
	public static final Column<Integer> ofInteger(Nulls nulls){
		return new Column_<>(TypeFactory.fetchInteger(nulls.value()));
	}
	
	/**
	 * Creates a new column of type {@linkplain LongType Long}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link LongType} interface or to the {@link SimpleType}
	 * class.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 * 
	 * @return The created column of type Long.
	 */
	public static final Column<Long> ofLong(Nulls nulls) {
		return new Column_<>(TypeFactory.fetchLong(nulls.value()));
	}
	
	/**
	 * Creates a new column of type {@linkplain FloatType Float}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link FloatType} interface or to the {@link SimpleType}
	 * class.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 * 
	 * @return The created column of type Float.
	 */
	public static final Column<Float> ofFloat(Nulls nulls) {
		return new Column_<>(TypeFactory.fetchFloat(nulls.value()));
	}

	/**
	 * Creates a new column of type {@linkplain DoubleType Double}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link DoubleType} interface or to the {@link SimpleType}
	 * class.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 * 
	 * @return The created column of type Double.
	 */
	public static final Column<Double> ofDouble(Nulls nulls) {
		return new Column_<>(TypeFactory.fetchDouble(nulls.value()));
	}
	
	/**
	 * Creates a new column of type "classic Java" {@linkplain StringType
	 * String}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link StringType} interface or to the {@link SimpleType}
	 * class.
	 * <p>
	 * Invoking this method has the same effect as invoking {@code
	 * ofString(Nulls.NULLABLE, OutrowStringLength.GIANT)}.
	 * 
	 * @return The created column of type String.
	 */
	public static final Column<String> ofString() {
		return ofString(Nulls.NULLABLE, OutrowStringLength.GIANT);
	}
	
	/**
	 * Creates a new column of type {@linkplain StringType String} with an
	 * {@linkplain Scheme#OUTROW outrow storage scheme} and applying the "UTF-8"
	 * charset for any byte conversions.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link StringType} interface or to the {@link SimpleType}
	 * class.
	 * <p>
	 * Invoking this method has the same effect as invoking {@code
	 * ofString(Charset.forName("UTF-8"), nulls, length)}.
	 * 
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 * @param  length The length of the string.
	 * 
	 * @return The created column of type String.
	 */
	public static final Column<String> ofString(Nulls nulls,
																	OutrowStringLength length) {
		return ofString(Charset.forName("UTF-8"), nulls, length);
	}
	
	/**
	 * Creates a new column of type {@linkplain StringType String} with an
	 * {@linkplain Scheme#OUTROW outrow storage scheme} and applying the
	 * specified charset for any byte conversions.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link StringType} interface or to the {@link SimpleType}
	 * class.
	 * 
	 * @param  charset The {@link Charset} to be used to encode a string value,
	 *         not allowed to be {@code null}.
	 * @param  nulls Must be set to {@code Nulls.NULLABLE} if the type allows the
	 *         {@code null} value, {@code Nulls.NO_NULL} if not.
	 * @param  length The length of the string.
	 * 
	 * @return The created column of type String.
	 * 
	 * @throws NullPointerException If {@code charset} is {@code null}.
	 */
	public static final Column<String> ofString(Charset charset, Nulls nulls,
								OutrowStringLength length) throws NullPointerException {
		return new Column_<>(TypeFactory.fetchString(charset, nulls.value(),
																Scheme.OUTROW, length.limit()));
	}

	
	/**
	 * Creates a new column of type {@linkplain StringType String} with an
	 * {@linkplain Scheme#INROW inrow storage scheme} and applying the specified
	 * charset for any byte conversions.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link StringType} interface or to the {@link SimpleType}
	 * class.
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
	 * @return The created column of type String.
	 * 
	 * @throws NullPointerException If {@code charset} is {@code null}.
	 * @throws IllegalArgumentException If {@code length} is less than 1 or
	 *         greater than {@code Integer.MAX_VALUE} - 4.
	 */
	public static final Column<String> ofString(Charset charset, Nulls nulls,
				int length) throws NullPointerException, IllegalArgumentException {
		return new Column_<>(TypeFactory.fetchString(charset, nulls.value(),
																			Scheme.INROW, length));
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
	 * Creates a new column of type {@linkplain ArrayType array} with an
	 * {@linkplain Scheme#OUTROW outrow storage scheme} and with elements of the
	 * specified element type.
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
	 * <p>
	 * Invoking this method has the same effect as invoking {@code
	 * ofArray(Scheme.OUTROW, maxSize, elementType)}.
	 * 
	 * @param  <T> The type of the elements.
	 *
	 * @param  maxSize The maximum number of elements in an array value of this
	 *         array type.
	 * @param  elementType The type of the elements of the array, not allowed
	 *         to be {@code null}.
	 * 
	 * @return The created array type column.
	 *
	 * @throws NullPointerException If {@code elementType} is {@code null}.
	 * @throws IllegalArgumentException If {@code maxSize} is less than 1.
	 */
	public static final <T> Column<T[]> ofArray(int maxSize,
															SimpleType<T> elementType) throws
										NullPointerException, IllegalArgumentException {
		return ofArray(Scheme.OUTROW, maxSize, elementType);
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
	public static final <T> Column<T[]> ofArray(Scheme scheme, int maxSize,
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
	public static final Column<Ref> ofRef() {
		return new Column_<>(TypeFactory.fetchRef());
	}
	
	/**
	 * Creates a new column having an {@linkplain ArrayOfRefType array of
	 * references column type} with an {@linkplain Scheme#OUTROW outrow storage
	 * scheme}.
	 * It is safe to cast {@code column.info().type()} of the returned {@code
	 * column} to the {@link ArrayOfRefType} interface.
	 * <p>
	 * Invoking this method has the same effect as invoking {@code
	 * ofArrayOfRef(Scheme.OUTROW, maxSize)}.
	 * 
	 * @param  maxSize The maximum number of elements in an array value of this
	 *         array type.
	 * 
	 * @return The created column of type array with elements being {@linkplain
	 *         Ref references}.
	 *
	 * @throws IllegalArgumentException If {@code maxSize} is less than 1.
	 */
	public static final Column<Ref[]> ofArrayOfRef(int maxSize) throws
																		IllegalArgumentException {
		return ofArrayOfRef(Scheme.OUTROW, maxSize);
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
	public static final Column<Ref[]> ofArrayOfRef(Scheme scheme,
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
