/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.types;

import java.nio.charset.Charset;

/**
 * The column type analogon of a Java {@code String} type.
 * In contrast to the Java {@code String} type this type may be specified such
 * that the {@code null} value is forbidden.
 * <p>
 * Besides the property that decides if the {@code null} value is allowed or
 * not, the String type has three additional properties: The {@linkplain
 * Type.Scheme storage scheme}, the <em>limit</em> and the <em>charset</em>.
 * All have an influence on the maximum number of characters in a String value
 * as follows:
 * 
 * <h1>Inrow Storage Scheme</h1>
 * If the chosen charset is a single byte character set then the maximum number
 * of characters in a string is equal to the value of the limit.
 * If the chosen charset is not a single byte character set then the maximum
 * number of characters may be less than the value of the limit.
 * 
 * <h1>Outrow Storage Scheme</h1>
 * If the chosen charset is a single byte character set then the maximum number
 * of characters in a string is equal to {@code n} = {@code min}(256<sup>{@code
 * limit}</sup> - 1, {@code Integer.MAX_VALUE}), e.g., 255 for a {@code limit}
 * equal to 1.
 * If the chosen charset is not a single byte character set then the maximum
 * number of characters may be less than {@code n} characters.
 * <p>
 * There should be no need for clients to implement this interface.
 *
 * @author Beat Hoermann
 */
public interface StringType extends Type {
	/**
	 * Returns the character set of this string type.
	 * 
	 * @return The character set, never {@code null}.
	 */
	Charset charset();
	
	/**
	 * Returns the limit of this string type.
	 * 
	 * @return The limit, greater than or equal to 1.
	 */
	int limit();
}
