/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import java.util.Arrays;
import java.util.Objects;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;

import acdp.design.ICipherFactory;
import acdp.exceptions.CreationException;
import acdp.exceptions.CryptoException;

/**
 * The crypto provider provides the necessary infrastructure for encrypting
 * and decrypting table data in a WR or RO database.
 * <p>
 * Note that the creation and initialization of {@link Cipher} instances is
 * not part of ACDP.
 * It is up to the customer of ACDP to specify the type of cipher he or she
 * prefers by providing an implementation of the {@link ICipherFactory}
 * interface.
 *
 * @author Beat Hoermann
 */
public final class CryptoProvider {
	/**
	 * Defines a node.
	 * Nodes are used by a {@linkplain CryptoProvider.CipherPool cipher pool}.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class Node {
		/**
		 * The node's pointer to its previous node.
		 * Following the pointers to the previous nodes from one node to another
		 * results in a list of "previous nodes".
		 */
		Node previous = null;
		/**
		 * The node's pointer to its next node.
		 * Following the pointers to the next nodes from one node to another
		 * results in a list of "next nodes".
		 */
		Node next = null;
		/**
		 * The cipher instance associated with this node.
		 */
		Cipher cipher = null;
	}
	
	/**
	 * Defines a cipher pool.
	 * The purpose of a cipher pool is to reuse {@link Cipher} instances in the
	 * presence of multiple concurrent threads.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class CipherPool {
		/**
		 * The pointer which separates the "empty" nodes from the "cipher" nodes.
		 * An "empty" node is ready to keep a cipher instance.
		 * If a cipher instance is brought to an "empty" node by a call to the
		 * {@link #takeIn} method then the "empty" node becomes a "cipher" node.
		 * Likewise, a "cipher" node is ready to give a cipher instance away.
		 * If a cipher instance is given away from the "cipher" node by a call to
		 * the {@link #takeOut} method then the "cipher" node becomes an "empty"
		 * node.
		 * The {@code cur} pointer points to the one and only one "empty" node
		 * such that all of its {@linkplain CryptoProvider.Node#previous previous
		 * nodes}, if any, are "empty" nodes and all of its {@linkplain
		 * CryptoProvider.Node#next next nodes}, if any, are "cipher" nodes.
		 */
		private Node cur;
		
		/**
		 * Constructs the cipher pool and initializes it with the specified
		 * cipher.
		 * 
		 * @param firstCipher The first cipher in the cipher pool, not allowed
		 *        to be {@code null}.
		 */
		CipherPool(Cipher firstCipher) {
			cur = new Node();
			final Node second = new Node();
			cur.next = second;
			second.previous = cur;
			second.cipher = firstCipher;
		}
		
		/**
		 * Returns a cipher instance or {@code null} if this cipher pool has no
		 * more cipher instances.
		 * <p>
		 * A call to this method must be compensated by a call to the {@link
		 * #takeIn} method, even if this method returns {@code null}.
		 * 
		 * @return A cipher instance from this cipher pool or {@code null} if
		 *         this cipher pool has no more cipher instances.
		 */
		synchronized Cipher takeOut() {
			if (cur.next == null) {
				final Node next = new Node();
				cur.next = next;
				next.previous = cur;
				cur = next;
				return null;
			}
			else {
				cur = cur.next;
				return cur.cipher;
			}
		}
		
		/**
		 * Includes the specified cipher into the cipher pool.
		 * 
		 * @param cipher The cipher to include or reinclude into this cipher pool.
		 *        The latter is the case if this cipher was returned by a
		 *        previous call to the {@code takeOut} method.
		 *        This value is not allowed to be {@code null}.
		 */
		synchronized void takeIn(Cipher cipher) {
			cur.cipher = cipher;
			cur = cur.previous;
		}
	}
	
	/**
	 * Provides some functions for encrypting and decrypting byte arrays in a
	 * WR database.
	 * 
	 * @author Beat Hoermann
	 */
	public static final class WRCrypto {
		/**
		 * The cipher factory, never {@code null}.
		 */
		private final ICipherFactory cf; 
		/**
		 * The one and only one cipher instance for encrypting, never {@code
		 * null}.
		 */
		private final Cipher encCipher;
		/**
		 * A cipher instance for decrypting, never {@code null}.
		 */
		private final Cipher decCipher;
		/**
		 * The cipher pool keeping cipher instances for decrypting, never {@code
		 * null}.
		 * Any cipher taken from this pool is initialized.
		 */
		private final CipherPool decPool;
		
		/**
		 * The constructor.
		 * 
		 * @param encCipher A cipher for encrypting byte arrays in a WR database,
		 *        not allowed to be {@code null}.
		 * @param decCipher A cipher for decrypting byte arrays in a WR database,
		 *        not allowed to be {@code null}.
		 * @param cf the cipher factory, not allowed to be {@code null}.
		 */
		private WRCrypto(Cipher encCipher, Cipher decCipher, ICipherFactory cf) {
			this.cf = cf;
			this.encCipher = encCipher;
			this.decCipher = decCipher;
			this.decPool = new CipherPool(decCipher);
		}
		
		/**
		 * Encrypts the specified array of bytes starting at the specified offset
		 * and ending after the specified number of bytes are encrypted.
		 * Applies a cipher behaving like a byte-oriented stream cipher.
		 * Thus, this method is able to store the result in the same byte array
		 * as the specified byte array at the very same offset and with the very
		 * same length.
		 * <p>
		 * Note that this method is not safe for use by multiple concurrent
		 * threads.
		 * 
		 * @param  byteArr The byte array, not allowed to be {@code null}.
		 * @param  offset The offset where encrypting the bytes start.
		 * @param  len The number of bytes to encrypt.
		 * 
		 * @throws CryptoException If encrypting the byte array fails.
		 */
		public final void encrypt(byte[] byteArr, int offset, int len) throws
																				CryptoException {
			try {
				encCipher.doFinal(byteArr, offset, len, byteArr, offset);
			} catch (Exception e) {
				throw new CryptoException(e);
			}
		}
		
		/**
		 * Decrypts the specified array of bytes starting at the specified offset
		 * and ending after the specified number of bytes are decrypted.
		 * Applies a cipher behaving like a byte-oriented stream cipher.
		 * Thus, this method is able to store the result in the same byte array
		 * as the specified byte array at the very same offset and with the very
		 * same length.
		 * <p>
		 * Note that this method is not safe for use by multiple concurrent
		 * threads.
		 * 
		 * @param  byteArr The byte array, not allowed to be {@code null}.
		 * @param  offset The offset where decrypting the bytes start.
		 * @param  len The number of bytes to decrypt.
		 * 
		 * @throws CryptoException If decrypting the data fails.
		 */
		public final void dcrpt(byte[] byteArr, int offset, int len) throws
																					CryptoException {
			try {
				decCipher.doFinal(byteArr, offset, len, byteArr, offset);
			} catch (Exception e) {
				throw new CryptoException(e);
			}
		}
		
		/**
		 * Decrypts the specified array of bytes starting at the specified offset
		 * and ending after the specified number of bytes are decrypted.
		 * Applies a cipher behaving like a byte-oriented stream cipher.
		 * Thus, this method is able to store the result in the same byte array
		 * as the specified byte array at the very same offset and with the very
		 * same length.
		 * <p>
		 * This method is safe for use by multiple concurrent threads.
		 * 
		 * @param  byteArr The byte array, not allowed to be {@code null}.
		 * @param  offset The offset where decrypting the bytes start.
		 * @param  len The number of bytes to decrypt.
		 * 
		 * @throws CryptoException If decrypting the byte array fails.
		 */
		public final void decrypt(byte[] byteArr, int offset, int len) throws
																				CryptoException {
			Cipher decCipher = decPool.takeOut();
			try {
				if (decCipher == null) {
					decCipher = cf.createAndInitWrCipher(false);
				}
				decCipher.doFinal(byteArr, offset, len, byteArr, offset);
			} catch (Exception e) {
				throw new CryptoException(e);
			} finally {
				decPool.takeIn(decCipher);
			}
		}
	}
	
	/**
	 * Provides some functions for encrypting and decrypting byte arrays in an
	 * RO database.
	 * 
	 * @author Beat Hoermann
	 */
	public static final class ROCrypto {
		/**
		 * The cipher factory, never {@code null}.
		 */
		private final ICipherFactory cf; 
		/**
		 * The one and only one cipher instance for encrypting, never {@code
		 * null}.
		 */
		private final Cipher encCipher;
		/**
		 * The cipher pool keeping cipher instances for decrypting, never {@code
		 * null}.
		 * There is no guarantee that a cipher instance taken from this pool is
		 * already initialized.
		 */
		private final CipherPool decPool;
		
		/**
		 * The constructor.
		 * 
		 * @param encCipher A cipher for encrypting byte arrays in an RO database,
		 *        not allowed to be {@code null}.
		 * @param decCipher A cipher for decrypting byte arrays in an RO database,
		 *        not allowed to be {@code null}.
		 * @param cf the cipher factory, not allowed to be {@code null}.
		 */
		private ROCrypto(Cipher encCipher, Cipher decCipher, ICipherFactory cf) {
			this.cf = cf;
			this.encCipher = encCipher;
			this.decPool = new CipherPool(decCipher);
		}
		
		/**
		 * Returns the reference to a cipher singleton kept by this class.
		 * Note that the cipher may not behave like a byte-oriented stream cipher.
		 * Furthermore, don't assume that the cipher is initialized.
		 * Invoke the {@link #init} method to initialize the cipher.
		 * <p>
		 * Since a cipher has a state, the returned cipher is not safe for use by
		 * multiple concurrent threads.
		 * 
		 * @return The cipher, never {@code null}.
		 */
		public final Cipher get() {
			return encCipher;
		}
		
		/**
		 * Returns an instance of a cipher, intended to be used for decrypting
		 * data.
		 * Note that the cipher may not behave like a byte-oriented stream cipher.
		 * Furthermore, don't assume that the cipher is initialized for
		 * decryption.
		 * Invoke the {@link #init} method with the second parameter set to
		 * {@code false} to initialize the cipher.
		 * <p>
		 * This method is safe for use by multiple concurrent threads.
		 * The returned cipher is guaranteed not in use by another thread unless
		 * you share it with another thread.
		 * <p>
		 * A call to this method must be compensated by a call to the {@link
		 * #release} method.
		 * 
		 * @return The cipher, intended to be used for decrypting data, never
		 *         {@code null}.
		 */
		public final Cipher request() {
			Cipher decCipher = decPool.takeOut();
			if (decCipher == null) {
				try {
					decCipher = cf.createRoCipher();
				} catch (Exception e) {
					throw new CryptoException(e);
				}
			}
			return decCipher;
		}
		
		/**
		 * Releases the specified cipher which was previously got from a call to
		 * the {@link #request} method.
		 * 
		 * @param cipher The cipher to release, not allowed to be {@code null}.
		 */
		public final void release(Cipher cipher) {
			decPool.takeIn(cipher);
		}
		
		/**
		 * Initializes the specified cipher.
		 * 
		 * @param  cipher The cipher, not allowed to be {@code null}.
		 * @param  encrypt The flag which indicates if the cipher must be
		 *         initialized for encryption ({@code true}) or for decryption
		 *         ({@code false}).
		 *        
		 * @throws NullPointerException If {@code cipher} is {@code null}.
		 * @throws CryptoException This exception should not be thrown.
		 *         It may be throws if the specified cipher was not obtained from
		 *         this crypto provider.
		 */
		public final void init(Cipher cipher, boolean encrypt) throws
														NullPointerException, CryptoException {
			try {
				cf.initRoCipher(cipher, encrypt);
			} catch (Exception e) {
				// Should not happen.
				throw new CryptoException(e);
			}
		}
	}
	
	/**
	 * The cipher factory, never {@code null}.
	 */
	private final ICipherFactory cf; 
	
	/**
	 * The constructor.
	 * 
	 * @param  cf The cipher factory, not allowed to be {@code null}.
	 * 
	 * @throws NullPointerException If the cipher factory is {@code null}.
	 */
	CryptoProvider(ICipherFactory cf) throws NullPointerException {
		this.cf = Objects.requireNonNull(cf);
	}
	
	/**
	 * Tests if the specified ciphers properly work.
	 * 
	 * @param  encCipher The cipher for encrypting data.
	 * @param  decCipher The cipher for decrypting data.
	 * @param  stream Indicates if {@code encCipher} is required to behave like
	 *         a byte-oriented stream cipher ({@code true}) or not ({@code
	 *         false}).
	 * 
	 * @throws IllegalBlockSizeException If at least one of the ciphers is a
	 *         block cipher, no padding has been requested (only in encryption
	 *         mode), and the total input length of the data processed by this
	 *         cipher is not a multiple of block size; or if this encryption
	 *         algorithm is unable to process the input data provided.
	 * @throws BadPaddingException If (un)padding has been requested for {@code
	 *         decCipher}, but the decrypted data is not bounded by the
	 *         appropriate padding bytes.
	 * @throws IllegalArgumentException If {@code stream} is {@code true} and
	 *         {@code encCipher} does not behave like a byte-oriented stream
	 *         cipher or if {@code decCipher} does not properly decrypt.
	 */
	private final void test(Cipher encCipher, Cipher decCipher,
										boolean stream) throws IllegalBlockSizeException,
										BadPaddingException, IllegalArgumentException {
		final byte[] probe = { -12, 123, 3, -75, 63 };
		
		// Encrypt probe.
		byte[] enc = encCipher.doFinal(probe);
		// Decrypt enc.
		final byte[] dec = decCipher.doFinal(enc);
		// Check if encCipher is a stream cipher.
		if (stream && enc.length != probe.length) {
			throw new IllegalArgumentException("Cipher does not behave like a " +
																"byte-oriented stream cipher.");
		}
		// Check if dec equals probe.
		if (!Arrays.equals(dec, probe)) {
			throw new IllegalArgumentException("Cipher does not properly " +
																						"decrypt.");
		}
	}
	
	/**
	 * Creates the WR crypto object.
	 * 
	 * @return The WR crypto object or {@code null} if the cipher factory given
	 *         to this class via its constructor returns {@code null} when asked
	 *         to create a WR cipher.
	 * 
	 * @throws CreationException If the creation of an instance of the {@code
	 *         WRCrypto} class fails.
	 */
	final WRCrypto createWRCrypto() throws CreationException {
		Cipher encCipher = null;
		Cipher decCipher = null;
		try {
			encCipher = cf.createAndInitWrCipher(true);
			if (encCipher != null) {
				decCipher = cf.createAndInitWrCipher(false);
				test(encCipher, decCipher, true);
			}
		} catch (Exception e) {
			throw new CreationException(e);
		}
		if (encCipher == null)
			return null;
		else {
			return new WRCrypto(encCipher, decCipher, cf);
		}
	}
	
	/**
	 * Creates the RO crypto object.
	 * 
	 * @return The RO crypto object or {@code null} if the cipher factory given
	 *         to this class via its constructor returns {@code null} when asked
	 *         to create an RO cipher.
	 * 
	 * @throws CreationException If the creation of an instance of the {@code
	 *         ROCrypto} class fails.
	 */
	final ROCrypto createROCrypto() throws CreationException {
		Cipher encCipher = null;
		Cipher decCipher = null;
		try {
			encCipher = cf.createRoCipher();
			if (encCipher != null) {
				cf.initRoCipher(encCipher, true);
				decCipher = cf.createRoCipher();
				cf.initRoCipher(decCipher, false);
				test(encCipher, decCipher, false);
			}
		} catch (Exception e) {
			throw new CreationException(e);
		}
		if (encCipher == null)
			return null;
		else {
			return new ROCrypto(encCipher, decCipher, cf);
		}
	}
}