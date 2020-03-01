/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.design;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Objects;

import acdp.exceptions.CreationException;
import acdp.internal.types.Type_;
import acdp.internal.misc.Utils_;
import acdp.internal.types.TypeFactory.Friend;
import acdp.misc.Utils;

/**
 * The super class of all non-array custom and built-in column types with the
 * exception of the built-in {@linkplain acdp.types.RefType reference column
 * type}.
 * Values of a simple column type can be used as elements of an {@linkplain
 * acdp.types.ArrayType array column type}.
 * <p>
 * Users that do not implement a custom column type can ignore this class.
 * This class must be part of the API because it appears in the signatures of
 * the {@link CL#typeArray} and {@link CL#create} methods and because
 * implementers of custom column types need to provide a concrete subclass of
 * this class.
 * However, even implementers of a custom column type can vastly ignore all
 * methods declared with a {@code public} access modifier and focus on the
 * instance methods declared with a {@code protected} access modifier.
 * Those public methods are either returning the arguments of the constructor or
 * are used internally by ACDP.
 * <p>
 * Implementers of a custom column type must implement the {@link
 * #toBytes(Object)} and the {@link #fromBytes(byte[], int, int)} methods and
 * may want to override the {@link #toBytes(Object, byte[], int)} and the
 * {@link #typeDescPrefix()} methods.
 * Furthermore, implementers of a custom column type must implement a {@code
 * public} and {@code static} type factory method annotated with the {@link
 * TypeFromDesc} annotation.
 * <p>
 * A simple column type has a <em>{@linkplain #length() length}</em>.
 * If this simple column type has an {@linkplain Scheme#INROW inrow} storage
 * scheme then its length is identical to the maximum number {@code n} of bytes
 * of the {@linkplain #toBytes(Object) byte representation} of any value of this
 * simple column type.
 * If this simple column type has an {@linkplain Scheme#OUTROW outrow} storage
 * scheme then {@code n} is equal to {@code min}(256<sup>{@code length}</sup> -
 * 1, {@code Integer.MAX_VALUE}) where {@code length} denotes the length of
 * this simple column type.
 * <p>
 * Other properties are explained in the description of the {@linkplain
 * #SimpleType constructor}.
 *
 * @author Beat Hoermann
 */
public abstract class SimpleType<T> extends Type_ {
	/**
	 * A class method annotated with the {@code TypeFromDesc} annotation
	 * takes the {@code String} representation of a type descriptor and returns
	 * an instance of a subclass of {@link SimpleType}.
	 * <p>
	 * The method should check if the type descriptor is a valid one and throw
	 * a {@code CreationException} if the type descriptor is invalid.
	 * The {@link TypeDesc} class can be used for parsing the {@code String}
	 * representation of the type descriptor.
	 * <p>
	 * The method is not allowed to return {@code null}.
	 *
	 * @author Beat Hoermann
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.METHOD)
	public @interface TypeFromDesc {
	}
	
	/**
	 * Returns the second part of the descriptor for a column type constructed
	 * with the specified arguments.
	 * <p>
	 * This method is hardly ever used by type implementers.
	 * 
	 * @param  scheme The storage scheme of the column type, not allowed to be
	 *         {@code null}.
	 * @param  nullable The information whether the column type allows the
	 *         {@code null} value ({@code true}) or not ({@code false}).
	 * @param  limit The limit of the column type.
	 *         This value must be greater than or equal to 1 and if {@code
	 *         scheme} is equal to {@code Scheme.OUTROW} then it must be less
	 *         than or equal to 4.
	 * @param  variable The information whether the length of the byte
	 *         representation of a value of the column type may vary from value
	 *         to value ({@code true}) or if the length is fixed ({@code false}).
	 *         This value is ignored if {@code scheme} is equal to
	 *         {@code Scheme.OUTROW}.
	 * 
	 * @return The suffix of the type descriptor, never {@code null} and never
	 *         an empty string.
	 */
	protected static final String typeDescSuffix(Scheme scheme, boolean nullable, 
																int limit, boolean variable) {
		final boolean inrow = scheme == Scheme.INROW;
		final String str = (nullable ? "n" : "!") + (inrow ? "i" : "o") + limit;
		
		return inrow && variable ? str + "v" : str;
	}
	
	/** 
	 * Provides convenient access to the individual fields of a type descriptor
	 * that is represented by a {@code String}.
	 * This class can be used by implementers of a custom column type to validate
	 * a {@code String} representation of a type descriptor at the time the
	 * custom column type is created.
	 * 
	 * @author Beat Hoermann
	 */
	protected static final class TypeDesc {
		/**
		 * The prefix of this type descriptor, never {@code null} and never an
		 * empty string.
		 */
		public final String prefix;
		/**
		 * The information whether this column type allows the {@code null} value.
		 */
		public final boolean nullable;
		/**
		 * The storage scheme of this column type, never {@code null}.
		 */
		public final Scheme scheme;
		/**
		 * The limit of this column type.
		 * The limit is directly related to the <em>{@linkplain #length()
		 * length}</em> of this column type.
		 * This value is greater than or equal to 1 and if {@code scheme} is
		 * equal to {@code Scheme.OUTROW} then it is less than or equal to 4.
		 */
		public final int limit;
		/**
		 * The information whether the byte representation of values of this
		 * column type are of variable length.
		 * If {@code scheme} is equal to {@code Scheme.OUTROW} then this value
		 * is equal to {@code false}.
		 */
		public final boolean variable;
		
		/**
		 * The constructor.
		 * 
		 * @param  typeDesc The type descriptor as a {@code String}.
		 * 
		 * @throws CreationException If parsing the specified type descriptor
		 *         fails.
		 */
		public TypeDesc(String typeDesc) throws CreationException {
			if (typeDesc == null || typeDesc.length() < 4) {
				throw new CreationException("Invalid type descriptor: \"" +
																				typeDesc + "\".");
			}
			
			// variable
			int k = typeDesc.length() - 1;
			final char lastCh = typeDesc.charAt(k);
			if (Character.isDigit(lastCh))
				variable = false;
			else if (lastCh == 'v') {
				variable = true;
				typeDesc = typeDesc.substring(0, k);
				k--;
			}
			else {
				throw new CreationException("Invalid type descriptor: \"" +
																				typeDesc + "\".");
			}
			// k >= 2
			
			// Find starting index of limit.
			while (k > 2 && Character.isDigit(typeDesc.charAt(k))) {
				k--;
			}
			// k >= 2
			
			// limit
			try {
				limit = Integer.parseInt(typeDesc.substring(k + 1));
			} catch (Exception e) {
				throw new CreationException("Invalid type descriptor: \"" +
																				typeDesc + "\".");
			}
			
			// scheme
			if (typeDesc.charAt(k) == 'i')
				scheme = Scheme.INROW;
			else if (typeDesc.charAt(k) == 'o')
				scheme = Scheme.OUTROW;
			else {
				throw new CreationException("Invalid type descriptor: \"" +
																				typeDesc + "\".");
			}
			
			k--;
			// k >= 1
			
			// nullable
			if (typeDesc.charAt(k) == 'n')
				nullable = true;
			else if (typeDesc.charAt(k) == '!')
				nullable = false;
			else {
				throw new CreationException("Invalid type descriptor: \"" +
																				typeDesc + "\".");
			}
			
			// prefix
			prefix = typeDesc.substring(0, k);
		}
	}
	
	/**
	 * The name of the class that contains the type factory method of this
	 * custom column type.
	 * This value is {@code null} if and only if this column type is a built-in
	 * column type.
	 */
	private String typeFactoryClassName = null;
	/**
	 * The path string denoting the directory that houses the class file of the
	 * class with the name equal to the value of the {@link
	 * #typeFactoryClassName} property and any depending class files.
	 * This value is {@code null} if this column type is a built-in column type.
	 * This value may even be {@code null} if this column type is a custom
	 * column type.
	 */
	private String typeFactoryClasspath = null;
	/**
	 * The object representing the {@linkplain #valueType() value type} of this
	 * column type, never {@code null}.
	 */
	private final Class<T> valueType;
	/**
	 * Indicates if this column type allows the {@code null} value.
	 */
	private final boolean nullable;
	/**
	 * See {@linkplain #length() here}.
	 */
	protected final int length;
	/**
	 * The fixed length of a byte array which houses the length of the byte
	 * representation of any value of this column type.
	 * The value is greater than zero if and only if this column type has an
	 * inrow storage scheme <em>and</em> the {@code variable} argument of the
	 * constructor is eqaul to {@code true}.
	 */
	private final int sizeLen;
	/**
	 * The maximum number of bytes of the byte array which results from
	 * converting a value of this column type to a byte array.
	 */
	private final int maxNofBytes;

	/**
	 * The constructor.
	 * 
	 * @param  valueType The object representing the {@linkplain #valueType()
	 *         value type} of this column type, not allowed to be {@code null}.
	 * @param  scheme The storage scheme of this column type, not allowed to be
	 *         {@code null}.
	 * @param  nullable The information whether this column type allows the
	 *         {@code null} value ({@code true}) or not ({@code false}).
	 *         Typically, values of column types forbidding the {@code null}
	 *         value can be persisted more efficiently than values of column
	 *         types allowing the {@code null} value.
	 * @param  limit The limit is directly related to the <em>{@linkplain
	 *         #length() length}</em> of this column type.
	 *         This value must be greater than or equal to 1 and if {@code
	 *         scheme} is equal to {@code Scheme.OUTROW} then it must be less
	 *         than or equal to 4.
	 * @param  variable The information whether the length of the byte
	 *         representation of a value of this column type may vary from value
	 *         to value ({@code true}) or if the length is fixed ({@code false}).
	 *         This value is ignored if {@code scheme} is equal to
	 *         {@code Scheme.OUTROW}.
	 *         
	 * @throws NullPointerException If {@code scheme} or {@code valueType} is
	 *         {@code null}.
	 * @throws IllegalArgumentException If {@code limit} is less than 1 or,
	 *         provided that {@code scheme} is equal to {@code Scheme.OUTROW},
	 *         {@code limit} is greater than 4.
	 *         Furthermore, this exception is thrown if {@code scheme} is equal
	 *         to {@code Scheme.INROW} and {@code variable} is equal to
	 *         {@code true} and {@code limit} is greater than
	 *         {@code Integer.MAX_VALUE} - 4.
	 */
	protected SimpleType(Class<T> valueType, Scheme scheme, boolean nullable, 
							int limit, boolean variable) throws NullPointerException,
																		IllegalArgumentException {
		super(scheme);
		
		if (limit < 1) {
			throw new IllegalArgumentException("The value of \"limit\" must " +
												"be greater than or equal to 1: " + limit);
		}
		if (limit > Integer.MAX_VALUE - 4 && scheme == Scheme.INROW && variable) {
			throw new IllegalArgumentException("The value of \"limit\" must " +
									"be less than or equal to Integer.MAX_VALUE - 4 " +
									"for a type with an inrow scheme and \"variable\" " +
									"set to true: " + limit);
			
		}
		if (scheme == Scheme.OUTROW && limit > 4) {
			throw new IllegalArgumentException("The value of \"limit\" must " +
										"be less than or equal to 4 for a type with " +
										"an outrow storage scheme: " + limit);
		}
		
		this.valueType = Objects.requireNonNull(valueType,
											"The value type is not allowed to be null.");
		this.nullable = nullable;
		this.sizeLen = scheme == Scheme.INROW && variable ? Utils.lor(limit) : 0;
		this.length = limit + sizeLen;
		if (scheme == Scheme.INROW)
			this.maxNofBytes = length;
		else {
			// this.length == limit && 1 <= limit <= 4
			this.maxNofBytes = Utils_.bnd4[limit];
		}
	}
	
	/**
	 * Friend-only setter of the name of the custom type factory class.
	 * (Since the friend resides in a different package, this method has to be
	 * public.)
	 * 
	 * @param  friend The friend, not allowed to be {@code null}.
	 * @param  cn The name of the custom type factory class.
	 * 
	 * @throws NullPointerException If {@code friend} is {@code null}.
	 */
	public final void setTypeFactoryClassName(Friend friend, String cn) throws
																			NullPointerException {
		typeFactoryClassName = cn;
	}
	
	/**
	 * Returns the name of the custom type factory class.
	 * 
	 * @return The name of the custom type factory class.
	 *         This value is {@code null} if and only if this column type is a
	 *         built-in column type.
	 */
	public final String getTypeFactoryClassName() {
		return typeFactoryClassName;
	}
	
	/**
	 * Friend-only setter of the classpath of the custom type factory class.
	 * (Since the friend resides in a different package, this method has to be
	 * public.)
	 * 
	 * @param  friend The friend, not allowed to be {@code null}.
	 * @param  cp The classpath of the custom type factory.
	 * 
	 * @throws NullPointerException If {@code friend} is {@code null}.
	 */
	public final void setTypeFactoryClasspath(Friend friend, String cp) throws
																			NullPointerException {
		typeFactoryClasspath = cp;
	}
	
	/**
	 * Returns the classpath of the custom type factory class.
	 * 
	 * @return The classpath of the custom type factory class, may be {@code
	 *         null}.
	 */
	public final String getTypeFactoryClasspath() {
		return typeFactoryClasspath;
	}
	
	/**
	 * Returns the first part of the descriptor of this column type.
	 * <p>
	 * Implementers can override this implementation to return a prefix with a
	 * smaller length or to handle prefixes that encode custom specific
	 * information.
	 * <p>
	 * Note that the returned value <em>must</em> start with an upper case letter
	 * so that ACDP can easily distinguish between the type descriptor of a
	 * built-in column type (which starts with a lower case letter) and a type
	 * descriptor of a custom column type.
	 * 
	 * @return The prefix of the type descriptor, never {@code null} and never
	 *         an empty string.
	 */
	protected String typeDescPrefix() {
		return "L" + this.getClass().getName();
	}
	
	@Override
	public final String typeDesc() {
		return typeDescPrefix() + typeDescSuffix(scheme, nullable,
																length - sizeLen, sizeLen > 0);
	}
	
	/**
	 * Returns the value type of this column type.
	 * <p>
	 * The value type is used, for example, when an arbitrary value of type
	 * {@code Object} is tested for {@linkplain #isCompatible compatibility}
	 * with this column type.
	 * 
	 * @return The value type of this column type, never {@code null}.
	 */
	public final Class<T> valueType() {
		return valueType;
	}
	
	/**
	 * Returns the information whether values of this column type are allowed to
	 * be {@code null}.
	 * 
	 * @return The boolean value {@code true} if values of this column type are
	 *         allowed to be {@code null}, {@code false} otherwise.
	 */
	public final boolean nullable() {
		return nullable;
	}
	
	/**
	 * Returns the <em>length</em> of this column type.
	 * The length is equal to the {@code limit} argument of the constructor
	 * unless this column type has an inrow storage scheme and the {@code
	 * variable} argument of the constructor is equal to {@code true}.
	 * In such a case the length is equal to {@code limit} +
	 * &lfloor;{@code log}<sub>256</sub>({@code limit})&rfloor; + 1.
	 * 
	 * @return The length of this column type, always greater than or equal to 1
	 *         and if the storage scheme is an outrow storage scheme then this
	 *         value is less than or equal to 4.
	 */
	public final int length() {
		return length;
	}
	
	/**
	 * Returns the information whether the length of the byte representation of a
	 * value of this column type, which is supposed to have an inrow storage
	 * scheme, may vary from value to value.
	 * 
	 * @return The boolean value {@code true} if and only if this column type has
	 *         an inrow storage scheme and if the {@code variable} argument
	 *         passed to the constructor was set to {@code true}.
	 */
	public final boolean variable() {
		return sizeLen > 0;
	}
	
	/**
	 * Tests if the specified value is compatible with this column type.
	 * In accordance with the general definition of {@linkplain
	 * acdp.types.Type#isCompatible compatibility} the specified value is
	 * compatible with this column type if and only if
	 * <ul>
	 * 	<li>The value is {@code null} and the {@link #nullable} method returns
	 *        {@code true} <em>or</em></li>
	 * 	<li>The value is not {@code null} but the value is {@link
	 *        Class#isInstance assignment-compatible} with the {@linkplain
	 *        #valueType() value type} of this column type.</li>
	 * </ul>
	 * 
	 * @param  val The value to test for compatibility.
	 * 
	 * @return The boolean value {@code true} if the value is compatible with
	 *         this column type, {@code false} otherwise.
	 */
	@Override
	public final boolean isCompatible(Object val) {
		return val == null ? nullable : valueType.isInstance(val);
	}
	
	/**
	 * Converts the specified value to its byte representation.
	 * <p>
	 * This method assumes that calling the {@link #isCompatible} method on
	 * {@code val} returns {@code true}.
	 * If this is not the case then this method may throw an exception not
	 * mentioned below.
	 * <p>
	 * The returned length of the byte representation must be less than or equal
	 * to the value returned by the {@link #length()} method.
	 * Implementers of this method should not check if this condition is met
	 * because it is internally checked anyway.
	 * 
	 * @param  val The value to convert, not allowed to be {@code null}.
	 * 
	 * @return The byte representation of the value, never {@code null}.
	 * 
	 * @throws NullPointerException If {@code val} is {@code null}.
	 */
	protected abstract byte[] toBytes(T val) throws NullPointerException;
	
	/**
	 * Converts the specified value to a byte array.
	 * <p>
	 * This method assumes that calling the {@code isCompatible} method on
	 * {@code val} returns {@code true}.
	 * If this is not the case then this method may throw an exception that is
	 * not mentioned below.
	 * <p>
	 * This method should be invoked only if this column type has an outrow
	 * storage scheme.
	 * 
	 * @param  val The value to convert, not allowed to be {@code null}.
	 *         
	 * @return The value as a byte array, never {@code null}.
	 * 
	 * @throws NullPointerException If {@code val} is {@code null}.
	 * @throws IllegalArgumentException If the length of the byte
	 *         representation of the specified value exceeds the maximum number
	 *         of bytes allowed by this column type.
	 */
	public final byte[] convertToBytes(Object val) throws NullPointerException,
																		IllegalArgumentException {
		final byte[] bytes = toBytes(valueType.cast(val));
		
		if (bytes.length > maxNofBytes) {
			throw new IllegalArgumentException("Converted value too large.");
		}
		
		return bytes;
	}
	
	/**
	 * Converts the specified value to its byte representation and puts it into
	 * the specified byte array starting at the specified offset.
	 * <p>
	 * This method assumes that calling the {@link #isCompatible} method on
	 * {@code val} returns {@code true}.
	 * If this is not the case then this method may throw an exception not
	 * mentioned below.
	 * <p>
	 * The returned length of the byte representation must be less than or equal
	 * to the value returned by the {@link #length()} method.
	 * Implementers of this method should not check if this condition is met
	 * because it is internally checked anyway.
	 * <p>
	 * Type implementers are encouraged to override this method if they can
	 * avoid a call to the {@code System.arraycopy} method.
	 * 
	 * @param  val The value to convert, not allowed to be {@code null}.
	 * @param  bytes The destination byte array, not allowed to be {@code null}.
	 * @param  offset The index within {@code bytes} where to start saving the
	 *         byte representation.
	 * 
	 * @return The length of the byte representation.
	 * 
	 * @throws NullPointerException If {@code val} or {@code bytes} are
	 *         {@code null}.
	 * @throws IndexOutOfBoundsException If saving the byte representation would
	 *         cause access of data outside of the array bounds of the specified
	 *         byte array.
	 */
	protected int toBytes(T val, byte[] bytes, int offset) throws
										NullPointerException, IndexOutOfBoundsException {
		final byte[] byteArr = toBytes(val);
		final int len = byteArr.length;
		System.arraycopy(byteArr, 0, bytes, offset, len);
		return len;
	}
	
	/**
	 * Converts the specified value to a byte array and puts it into the
	 * specified byte array starting at the specified offset.
	 * <p>
	 * This method assumes that calling the {@code isCompatible} method on
	 * {@code val} returns {@code true}.
	 * If this is not the case then this method may throw an exception that is
	 * not mentioned below.
	 * <p>
	 * This method should be invoked only if this column type has an inrow
	 * storage scheme.
	 * 
	 * @param  val The value to convert, not allowed to be {@code null}.
	 * @param  bytes The destination byte array, not allowed to be {@code null}.
	 * @param  offset The index within {@code bytes} where to start saving the
	 *         the converted value.
	 *         
	 * @return The length of the byte representation.
	 * 
	 * @throws NullPointerException If {@code val} or {@code bytes} is
	 *         {@code null}.
	 * @throws IllegalArgumentException If the length of the byte
	 *         representation of the specified value exceeds the maximum number
	 *         of bytes allowed by this column type.
	 * @throws IndexOutOfBoundsException If saving the byte representation would
	 *         cause access of data outside of the array bounds of the specified
	 *         byte array.
	 */
	public final int convertToBytes(Object val, byte[] bytes, int offset) throws
										NullPointerException, IllegalArgumentException,
																	IndexOutOfBoundsException {
		final int len;
		if (sizeLen > 0) {
			// Encode length.
			final int n = toBytes(valueType.cast(val), bytes, offset + sizeLen);
			len = n + sizeLen;
			if (len < 0 || len > maxNofBytes) {
				// len < 0 => Overflow has occurred.
				throw new IllegalArgumentException("Converted value too large.");
			}
			Utils.unsToBytes(n, sizeLen, bytes, offset);
		}
		else {
			len = toBytes(valueType.cast(val), bytes, offset);
			if (len > maxNofBytes) {
				throw new IllegalArgumentException("Converted value too large.");
			}
		}
		return len;
	}
	
	/**
	 * Converts the byte representation of the value contained in the specified
	 * byte subarray to an {@code Object}.
	 * 
	 * @param  bytes The byte array containing the byte representation of the
	 *         value, not allowed to be {@code null}.
	 * @param  offset The index within {@code bytes} of the first byte to
	 *         convert, must be greater than or equal to zero.
	 * @param  len The number of bytes to convert.
	 *         
	 * @return The resulting object.
	 * 
	 * @throws IndexOutOfBoundsException If converting the byte representation
	 *         would cause access of data outside of the array bounds of the
	 *         specified byte array.
	 */
	protected abstract T fromBytes(byte[] bytes, int offset, int len) throws
																		IndexOutOfBoundsException;

	/**
	 * Converts the specified byte subarray to an {@code Object}.
	 * <p>
	 * This method should be invoked only if this column type has an inrow
	 * storage scheme.
	 * 
	 * @param  bytes The byte array containing the byte representation of the
	 *         value, not allowed to be {@code null}.
	 * @param  offset The index within {@code bytes} of the first byte to
	 *         convert, must be greater than or equal to zero.
	 *         
	 * @return The resulting object.
	 * 
	 * @throws IndexOutOfBoundsException If converting the byte representation
	 *         would cause access of data outside of the array bounds of the
	 *         specified byte array.
	 */
	public final T convertFromBytes(byte[] bytes, int offset) throws
																	IndexOutOfBoundsException {
		final int len;
		if (sizeLen > 0) {
			len = (int) Utils.unsFromBytes(bytes, offset, sizeLen);
			offset += sizeLen;
		}
		else {
			len = length;
		}
		return fromBytes(bytes, offset, len);
	}

	/**
	 * Converts {@code len} bytes of the specified byte subarray to an {@code
	 * Object}.
	 * <p>
	 * This method should be invoked only if this column type has an outrow
	 * storage scheme.
	 * 
	 * @param  bytes The byte array containing the byte representation of the
	 *         value, not allowed to be {@code null}.
	 * @param  offset The index within {@code bytes} of the first byte to
	 *         convert, must be greater than or equal to zero.
	 * @param  len The number of bytes to convert.
	 *         
	 * @return The resulting object.
	 * 
	 * @throws IndexOutOfBoundsException If converting the byte representation
	 *         would cause access of data outside of the array bounds of the
	 *         specified byte array.
	 */
	public final T convertFromBytes(byte[] bytes, int offset, int len) throws
																	IndexOutOfBoundsException {
		return fromBytes(bytes, offset, len);
	}
}