/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.design;

import javax.crypto.Cipher;

import acdp.tools.Setup;

/**
 * Declares methods used to encrypt and decrypt data in a database.
 * <p>
 * The creation and initialization of {@link Cipher} instances for encryption
 * and decryption is under control of the client.
 * The client can choose whatever type of cryptographic cipher he or she
 * prefers&mdash;with a single restriction:
 * For a WR database the cipher must encrypt data in the unit of a single byte.
 * (Such a cipher is sometimes called a <em>byte-oriented stream cipher</em> or
 * a block cipher that <em>is turned</em> into a byte-oriented stream cipher.
 * See the description of the {@code Cipher} class for details.)
 * No such restriction exists for an RO database.
 * <p>
 * For a WR database that needs cryptographic functionality
 * (encryption/decryption) the client must implement the {@link
 * #createAndInitWrCipher} method such that it returns a value different from
 * {@code null}.
 * Similarly, for an RO database that needs cryptographic functionality the
 * client must implement the {@link #createRoCipher} method such that it returns
 * a value different from {@code null} <em>as well as</em> the {@link
 * #initRoCipher} method such that it properly initializes the specified cipher.
 * If the client is dealing with both types of databases at the same time, for
 * instance, if the client wants to convert a WR database to an RO database, and
 * both types need cryptographic functionality then the client must provide a
 * reasonable implementation for all three methods of this interface.
 * <p>
 * The class implementing this interface must be registered in the layout of
 * the database.
 * Consult the {@link Setup} class description for more details.
 * <p>
 * Of course, this interface must only be implemented if cryptographic
 * functionality is required at all.
 * An implementation such that both create methods return a value equal to
 * {@code null} works fine but has no effect.
 *
 * @author Beat Hoermann
 */
public interface ICipherFactory {
	/**
	 * Returns a new cipher instance to be used by ACDP for encrypting/decrypting
	 * data of a WR database.
	 * The returned cipher must be a byte-oriented stream cipher or must behave
	 * that way and must be initialized according to the {@code encrypt}
	 * argument.
	 * <p>
	 * This method may throw various kinds of exceptions depending on the
	 * implementation of the cipher algorithm.
	 * See the description of the {@code getInstance} and {@code init} methods
	 * of the {@link Cipher} class for any details.
	 * 
	 * @param  encrypt Initialize the cipher for encryption ({@code true}) or
	 *         decryption ({@code false}).
	 * 
	 * @return The created and initialized cipher instance or {@code null}.
	 *         
	 * @throws Exception If creating or initializing the cipher fails.
	 */
	Cipher createAndInitWrCipher(boolean encrypt) throws Exception;
	
	/**
	 * Returns a new cipher instance to be used by ACDP for encrypting/decrypting
	 * data of an RO database.
	 * The returned cipher should not yet be initialized and may be a block
	 * cipher encrypting data in units larger than a single byte.
	 * <p>
	 * This method may throw various kinds of exceptions depending on the
	 * implementation.
	 * See the description of the {@code getInstance} methods of the {@link
	 * Cipher} class for any details.
	 * 
	 * @return The created cipher instance or {@code null}.
	 *         
	 * @throws Exception If creating the cipher fails.
	 */
	Cipher createRoCipher() throws Exception;

	/**
	 * Initializes the specified cipher, earlier created by a call to the
	 * {@link #createRoCipher} method.
	 * <p>
	 * This method may throw various kinds of exceptions depending on the
	 * implementation.
	 * See the description of the {@code init} methods of the {@link Cipher}
	 * class for any details.
	 * 
	 * @param  cipher The cipher to initialize, not allowed to be {@code null}.
	 * @param  encrypt Initialize the cipher for encryption ({@code true}) or
	 *         decryption ({@code false}).
	 *        
	 * @throws Exception If initializing the cipher fails.
	 */
	void initRoCipher(Cipher cipher, boolean encrypt) throws Exception;
}
