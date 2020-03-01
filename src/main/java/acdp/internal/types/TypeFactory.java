/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.types;

import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import acdp.Ref;
import acdp.design.SimpleType;
import acdp.design.SimpleType.TypeFromDesc;
import acdp.exceptions.CreationException;
import acdp.internal.Database_;
import acdp.types.StringType;
import acdp.types.Type.Scheme;

/**
 * This class is the only place where column types (either built-in or custom
 * column types) are created and handled as singletons.
 *
 * @author Beat Hoermann
 */
public final class TypeFactory {
	// Replicate C++ friend mechanism, see https://stackoverflow.com/questions/
	// 182278/is-there-a-way-to-simulate-the-c-friend-concept-in-java
	public static final class Friend	{private Friend() {}};
	private static final Friend friend = new Friend();
	
	/**
	 * Maps a type descriptor to a column type instance.
	 */
	private static final Map<String, Type_> cache = new HashMap<>();
	
	/**
	 * Returns the Boolean column type.
	 * 
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         
	 * @return The Boolean column type, never {@code null}.
	 */
	public static final BooleanType_ fetchBoolean(boolean nullable) {
		final String typeDesc = BooleanType_.typeDesc(nullable);
		BooleanType_ t;
		if (cache.containsKey(typeDesc))
			t = (BooleanType_) cache.get(typeDesc);
		else {
			t = new BooleanType_(nullable);
			cache.put(typeDesc, t);
		}
		return t;
	}
	
	/**
	 * Returns the Byte column type.
	 * 
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         
	 * @return The Byte column type, never {@code null}.
	 */
	public static final ByteType_ fetchByte(boolean nullable) {
		final String typeDesc = ByteType_.typeDesc(nullable);
		ByteType_ t;
		if (cache.containsKey(typeDesc))
			t = (ByteType_) cache.get(typeDesc);
		else {
			t = new ByteType_(nullable);
			cache.put(typeDesc, t);
		}
		return t;
	}
	
	/**
	 * Returns the Short column type.
	 * 
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         
	 * @return The Short column type, never {@code null}.
	 */
	public static final ShortType_ fetchShort(boolean nullable) {
		final String typeDesc = ShortType_.typeDesc(nullable);
		ShortType_ t;
		if (cache.containsKey(typeDesc))
			t = (ShortType_) cache.get(typeDesc);
		else {
			t = new ShortType_(nullable);
			cache.put(typeDesc, t);
		}
		return t;
	}
	
	/**
	 * Returns the Integer column type.
	 * 
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         
	 * @return The Integer column type, never {@code null}.
	 */
	public static final IntegerType_ fetchInteger(boolean nullable) {
		final String typeDesc = IntegerType_.typeDesc(nullable);
		IntegerType_ t;
		if (cache.containsKey(typeDesc))
			t = (IntegerType_) cache.get(typeDesc);
		else {
			t = new IntegerType_(nullable);
			cache.put(typeDesc, t);
		}
		return t;
	}
	
	/**
	 * Returns the Long column type.
	 * 
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         
	 * @return The Long column type, never {@code null}.
	 */
	public static final LongType_ fetchLong(boolean nullable) {
		final String typeDesc = LongType_.typeDesc(nullable);
		LongType_ t;
		if (cache.containsKey(typeDesc))
			t = (LongType_) cache.get(typeDesc);
		else {
			t = new LongType_(nullable);
			cache.put(typeDesc, t);
		}
		return t;
	}
	
	/**
	 * Returns the Float column type.
	 * 
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         
	 * @return The Float column type, never {@code null}.
	 */
	public static final FloatType_ fetchFloat(boolean nullable) {
		final String typeDesc = FloatType_.typeDesc(nullable);
		FloatType_ t;
		if (cache.containsKey(typeDesc))
			t = (FloatType_) cache.get(typeDesc);
		else {
			t = new FloatType_(nullable);
			cache.put(typeDesc, t);
		}
		return t;
	}
	
	/**
	 * Returns the Double column type.
	 * 
	 * @param  nullable Must be set to {@code true} if the type allows the
	 *         {@code null} value, {@code false} if not.
	 *         
	 * @return The Double column type, never {@code null}.
	 */
	public static final DoubleType_ fetchDouble(boolean nullable) {
		final String typeDesc = DoubleType_.typeDesc(nullable);
		DoubleType_ t;
		if (cache.containsKey(typeDesc))
			t = (DoubleType_) cache.get(typeDesc);
		else {
			t = new DoubleType_(nullable);
			cache.put(typeDesc, t);
		}
		return t;
	}
	
	/**
	 * Returns the String column type.
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
	 * @return The String column type, never {@code null}.
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
	public static final StringType_ fetchString(Charset charset,
										boolean nullable, Scheme scheme, int limit) throws
										NullPointerException, IllegalArgumentException {
		final String typeDesc = StringType_.typeDesc(charset, nullable, scheme,
																								limit);
		StringType_ t;
		if (cache.containsKey(typeDesc))
			t = (StringType_) cache.get(typeDesc);
		else {
			t = new StringType_(charset, nullable, scheme, limit);
			cache.put(typeDesc, t);
		}
		return t;
	}
	
	/**
	 * If the specified column type is contained in the internal cache of column
	 * types then this method returns the instance contained in the internal
	 * cache, otherwise this method returns the specified instance.
	 * 
	 * @param  type The column type, not allowed to be {@code null}.
	 * 
	 * @return The column type, never {@code null}.
	 *         Note that the returned column type may not be identical
	 *         ({@code ==}) to the specified column type but the type descriptors
	 *         will be equal ({@code String.equals}).
	 * 
	 * @throws NullPointerException If {@code type} is {@code null}.
	 */
	public static final Type_ getFromCache(Type_ type) throws
																			NullPointerException {
		final String typeDesc = type.typeDesc();
		if (cache.containsKey(typeDesc))
			type = cache.get(typeDesc);
		else {
			cache.put(typeDesc, type);
		}
		return type;
	}
	
	/**
	 * Returns the array column type with the specified element type.
	 * <p>
	 * Note that the element type of the returned array column may not be
	 * identical ({@code ==}) to the specified element type but the type
	 * descriptors will be equal ({@code String.equals}).
	 * 
	 * @param  scheme The storage scheme of the type, not allowed to be
	 *         {@code null}.
	 * @param  maxSize The maximum number of elements in an array value of this
	 *         array type.
	 * @param  elementType The type of the elements of the array, not allowed
	 *         to be {@code null}.
	 *         
	 * @return The array column type, never {@code null}.
	 *         Note that the element type of the array column type may not be
	 *         identical ({@code ==}) to the specified element type but the type
	 *         descriptors will be equal ({@code String.equals}).
	 * 
	 * @throws NullPointerException If {@code scheme} is {@code null}.
	 * @throws IllegalArgumentException If {@code maxSize} is less than 1.
	 */
	public static final ArrayType_ fetchArrayType(Scheme scheme, int maxSize, 
															SimpleType<?> elementType) throws
										NullPointerException, IllegalArgumentException {
		final String typeDesc = ArrayType_.typeDesc(scheme, maxSize, elementType);
		ArrayType_ t;
		if (cache.containsKey(typeDesc))
			t = (ArrayType_) cache.get(typeDesc);
		else {
			t = new ArrayType_(scheme, maxSize, (SimpleType<?>)
																	getFromCache(elementType));
			cache.put(typeDesc, t);
		}
		return t;
	}
	
	/**
	 * Returns a column type allowing for values being {@linkplain Ref
	 * references}.
	 *         
	 * @return The described column type, never {@code null}.
	 */
	public static final RefType_ fetchRef() {
		final String typeDesc = RefType_.refTypeDesc();
		RefType_ t;
		if (cache.containsKey(typeDesc))
			t = (RefType_) cache.get(typeDesc);
		else {
			t = new RefType_();
			cache.put(typeDesc, t);
		}
		return t;
	}
	
	/**
	 * Returns the array type with elements being {@linkplain Ref references}.
	 * 
	 * @param  scheme The storage scheme of the type, not allowed to be
	 *         {@code null}.
	 * @param  maxSize The maximum number of elements in an array value of this
	 *         array type.
	 *         
	 * @return The described column type, never {@code null}.
	 * 
	 * @throws NullPointerException If {@code scheme} is {@code null}.
	 * @throws IllegalArgumentException If {@code maxSize} is less than 1.
	 */
	public static final ArrayOfRefType_ fetchArrayOfRefType(Scheme scheme,
													int maxSize) throws NullPointerException,
																		IllegalArgumentException {
		final String typeDesc = ArrayOfRefType_.typeDesc(scheme, maxSize);
		ArrayOfRefType_ t;
		if (cache.containsKey(typeDesc))
			t = (ArrayOfRefType_) cache.get(typeDesc);
		else {
			t = new ArrayOfRefType_(scheme, maxSize);
			cache.put(typeDesc, t);
		}
		return t;
	}
	
	/**
	 * Creates the custom column type as described by the specified type
	 * descriptor using the type factory method contained in the class with the
	 * specified name.
	 * 
	 * @param  typeDesc The type descriptor, not allowed to be {@code null} and
	 *         not allowed to be an empty string.
	 * @param  cn The class name of the class that contains the type factory
	 *         method, not allowed to be {@code null}.
	 * @param  cp The path string denoting the directory that houses the class
	 *         file of the class with the specified class name and any depending
	 *         class files.
	 *         This value may be {@code null}.
	 * @param  layoutDir The directory of the column sublayout, not allowed to be
	 *         {@code null}.
	 *         The layout's directory is used to convert relative file paths
	 *         contained in the layout to absolute file paths.
	 * 
	 * @return The created custom column type as described by the specified type
	 *         descriptor, never {@code null}.
	 * 
	 * @throws CreationException If creating the custom column type fails.
	 */
	private static final SimpleType<?> createCustomType(String typeDesc,
					String cn, String cp, Path layoutDir) throws CreationException {
		SimpleType<?> customType = null;
		try {
			// Load the type factory class.
			final Class<?> cl = Database_.loadClass(cn, cp, layoutDir);
			// Search for the type factory class method within the type factory
			// class.
			Method[] methods = cl.getDeclaredMethods();
			int i = 0;
			while (i < methods.length && customType == null) {
				Method method = methods[i++];
				if (method.isAnnotationPresent(TypeFromDesc.class)) {
					customType = (SimpleType<?>) method.invoke(null, typeDesc);
				}
			}
			if (customType == null) {
				throw new NullPointerException("Custom column type is null. " +
													"Check if class \"" + cn + "\" has an " +
													"annotated type factory class method. " +
													"Also check the type descriptor.");
			}
			customType.setTypeFactoryClassName(friend, cn);
			customType.setTypeFactoryClasspath(friend, cp);
		} catch (Exception e) {
			throw new CreationException("Creating custom column type failed. " +
											"Type descriptor: \"" + typeDesc + "\".", e);
		}
		return customType;
	}
	
	/**
	 * Creates the built-in or custom column type as described by the specified
	 * type descriptor.
	 * 
	 * @param  typeDesc The type descriptor, not allowed to be {@code null} and
	 *         not allowed to be an empty string.
	 * @param  cn the class name of the class that contains the type factory
	 *         method.
	 *         This value is ignored if the type descriptor describes a built-in
	 *         column type.
	 *         If the type descriptor describes a custom column type then this
	 *         value is not allowed to be {@code null}.
	 * @param  cp the path string denoting the directory that houses the class
	 *         file of the class with the specified class name and any depending
	 *         class files.
	 *         This value is ignored if the type descriptor describes a built-in
	 *         column type.
	 * @param  layoutDir The directory of the column sublayout.
	 *         The layout's directory is used to convert relative file paths
	 *         contained in the layout to absolute file paths.
	 *         This value is ignored if the type descriptor describes a built-in
	 *         column type.
	 *         If the type descriptor describes a custom column type then this
	 *         value is not allowed to be {@code null}.
	 * 
	 * @return The created column type as described by the specified type
	 *         descriptor, never {@code null}.
	 * 
	 * @throws CreationException If creating the column type fails due to any
	 *         reason, including an invalid type descriptor or an error while
	 *         creating a custom column type.
	 */
	private static final Type_ createType(String typeDesc, String cn,
									String cp, Path layoutDir) throws CreationException {
		final Type_ type;
		final char kind = typeDesc.charAt(0);
		if (kind == ArrayType_.KIND) {
			// The type is an ArrayType_. The type of its elements is either a
			// built-in or a custom column type.
			final SimpleType<?> elementType = (SimpleType<?>) fetchType(
								ArrayType_.getTypeDescET(typeDesc), cn, cp, layoutDir);
			type = ArrayType_.createType(typeDesc, elementType);
		}
		else if (Type_.isBuiltInType(typeDesc)) {
			// The type is a built-in type but not an ArrayType_.
			switch (kind) {
				case BooleanType_.KIND :
								type = BooleanType_.createType(typeDesc); break;
				case ByteType_.KIND :
								type = ByteType_.createType(typeDesc); break;
				case ShortType_.KIND :
								type = ShortType_.createType(typeDesc); break;
				case IntegerType_.KIND :
								type = IntegerType_.createType(typeDesc); break;
				case LongType_.KIND :
								type = LongType_.createType(typeDesc); break;
				case FloatType_.KIND :
								type = FloatType_.createType(typeDesc); break;
				case DoubleType_.KIND :
								type = DoubleType_.createType(typeDesc); break;
				case StringType_.KIND :
								type = StringType_.createType(typeDesc); break;
				case RefType_.KIND :
								type = RefType_.createType(typeDesc); break;
				case ArrayOfRefType_.KIND :
								type = ArrayOfRefType_.createType(typeDesc); break;
				default : throw new CreationException("Invalid first character " +
											"of type descriptor: \"" + typeDesc + "\".");
			}
		}
		else {
			// The type is a custom column simple type.
			type = createCustomType(typeDesc, cn, cp, layoutDir);
		}
		// type != null;
		return type;
	}
	
	/**
	 * Returns the built-in or custom column type as described by the specified
	 * type descriptor.
	 * 
	 * @param  typeDesc The type descriptor, not allowed to be {@code null} and
	 *         not allowed to be an empty string.
	 * @param  cn the class name of the class that contains the type factory
	 *         method.
	 *         This value is ignored if the type descriptor describes a built-in
	 *         column type.
	 *         If the type descriptor describes a custom column type then this
	 *         value is not allowed to be {@code null}.
	 * @param  cp the path string denoting the directory that houses the class
	 *         file of the class with the specified class name and any depending
	 *         class files.
	 *         This value is ignored if the type descriptor describes a built-in
	 *         column type.
	 * @param  layoutDir The directory of the column sublayout.
	 *         The layout's directory is used to convert relative file paths
	 *         contained in the layout to absolute file paths.
	 *         This value is ignored if the type descriptor describes a built-in
	 *         column type.
	 *         If the type descriptor describes a custom column type then this
	 *         value is not allowed to be {@code null}.
	 * 
	 * @return The column type as described by the specified type descriptor,
	 *         never {@code null}.
	 * 
	 * @throws CreationException If creating the column type fails due to any
	 *         reason, including an invalid type descriptor or an error while
	 *         creating a custom column type.
	 */
	public static final Type_ fetchType(String typeDesc, String cn,
									String cp, Path layoutDir) throws CreationException {
		// typeDesc != null && !typeDesc.isEmpty() as per assumption.
		// if type descriptor denotes a custom column type then cn != null &&
		// layoutDir != null a per assumption.
		Type_ t;
		if (cache.containsKey(typeDesc))
			t = cache.get(typeDesc);
		else {
			t = createType(typeDesc, cn, cp, layoutDir);
			cache.put(typeDesc, t);
		}
		return t;
	}
	
	/**
	 * Prevent object construction.
	 */
	private TypeFactory() {
	}
}