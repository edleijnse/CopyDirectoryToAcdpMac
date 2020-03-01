/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.types;

/**
 * Defines the super interface of all column types.
 * <p>
 * There are <em>simple column types</em> and <em>array column types</em>.
 * <p>
 * A simple column type is either represented by the {@link RefType} interface
 * or by the {@link acdp.design.SimpleType SimpleType} class.
 * (Although the {@link acdp.types} package contains additional interfaces for
 * built-in simple column types, instances of built-in simple column types are
 * instances of classes extending the {@code SimpleType} class, see the
 * description of the {@link acdp.design.ST} class.)
 * A simple column type has a <em>value type</em> which is the type of the
 * value a {@linkplain acdp.Row row} can store in that column.
 * <p>
 * An array column type is either represented by the {@link ArrayOfRefType} or
 * the {@link ArrayType} interface.
 * An array column type has a <em>component type</em> which is a simple column
 * type.
 * <p>
 * There should be no need for clients to implement this interface.
 * (Implementers of custom column types extend the mentioned {@code SimpleType}
 * class.)
 * 
 * @author Beat Hoermann
 */
public interface Type {
	/**
	 * Defines the so called <em>inrow</em> and <em>outrow</em> storage schemes
	 * for values of a particular column type.
	 * <p>
	 * The byte representation of values of a column type with an inrow storage
	 * scheme are typically fixed in length or do not vary much in length.
	 * Examples are the boolean, integer, and long built-in column types with a
	 * fixed length of the byte representation of their values of one, four and
	 * eight bytes, respectively.
	 * Another example would be an inrow string column type limited to, say,
	 * three bytes:
	 * The byte representation of a value of such a column is not allowed to
	 * exceed three bytes but may be one byte only.
	 * Values of an inrow column type can be efficiently stored and accessed,
	 * provided that the length of the byte representation of their values do
	 * not vary too much.
	 * <p>
	 * An outrow storage scheme should be chosen if the length of the byte
	 * representation of the values vary much, for example if the values are
	 * {@code String} instances of arbitrary length.
	 * <p>
	 * The same holds for array column types:
	 * If the number of elements of the array values is fixed or does not vary
	 * too much then the array column type should be decared with an inrow
	 * storage scheme, otherwise with an outrow storage scheme.
	 * <p>
	 * The type of storage scheme is an important information for ACDP when
	 * choosing the best possible storage format for storing column values.
	 * 
	 * @author Beat Hoermann
	 */
	public enum Scheme {
		/**
		 * Values of an inrow simple column type or an inrow array column type
		 * with an inrow element column type are stored in the FL data block of
		 * the row.
		 * The number of bytes in an FL data block is fixed for all rows.
		 * <p>
		 * For a less technical description see the interface description.
		 */
		INROW,
		
		/**
		 * Values of an outrow simple column type or an outrow array column type
		 * or an inrow array column type with an outrow element column type are
		 * stored outside the fixed length FL data block of the row in a separate
		 * file that stores the outrow data of the rows.
		 * The FL data block just contains a fixed length pointer to the outrow
		 * data.
		 * <p>
		 * For a less technical description see the interface description.
		 */
		OUTROW;
	}
	
	/**
	 * Returns the storage scheme of this column type.
	 * 
	 * @return The storage scheme of this column type, never {@code null}.
	 */
	Scheme scheme();
	
	/**
	 * Returns the type descriptor of this column type.
	 * <p>
	 * The type descriptor uniquely identifies a particular column type among all
	 * other column types.
	 * It is used by a type factory to create an instance of that column type.
	 * <p>
	 * If the type descriptor starts with an upper case character then this
	 * column type is a custom column type otherwise it is a built-in column
	 * type.
	 * (A custom column type is provided by a client whereas a built-in column
	 * type is provided by ACDP.)
	 * <p>
	 *	Implementers of this method must ensure that the value returned by this
	 * method is indeed unique and that it remains consistent even across session
	 * boundaries.
	 * 
	 * @return The column type descriptor, never {@code null}.
	 */
	String typeDesc();
	
	/**
	 * Tests if the specified value is <em>compatible</em> with this column type.
	 * <p>
	 * If the column type is a simple column type then the specified value is
	 * compatible with this column type if and only if
	 * 
	 * <ul>
	 * 	<li>The value is {@code null} and this column type allows the {@code
	 *        null} value <em>or</em></li>
	 * 	<li>The value is different from {@code null} but the value is
	 *        {@linkplain Class#isInstance assignment-compatible} with the
	 *        value type of this column type.</li>
	 * </ul>
	 * <p>
	 * If the column type is an array column type then the specified value is
	 * compatible with this column type if and only if it is {@code null} or
	 * the following conditions are satisfied:
	 * 
	 * <ul>
	 * 	<li>The value is an array.</li>
	 * 	<li>If at least one element of the array value is equal to {@code null}
	 *        then this array column type's component type allows the {@code
	 *        null} value.</li>
	 * 	<li>The component type of the array value is equal to the value type of
	 *        this array column type's component type.</li>
	 * 	<li>The number of elements of the array value is less than or equal to
	 *        the maximum size allowed by this array column type.</li>
	 * </ul>
	 * 
	 * @param  value The value to test for compatibility.
	 * 
	 * @return The boolean value {@code true} if and only if the value is
	 *         compatible with this column type.
	 */
	boolean isCompatible(Object value);
}
