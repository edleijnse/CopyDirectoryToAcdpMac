/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package example;

import java.nio.file.Path;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import acdp.design.CustomDatabase;
import acdp.design.ICipherFactory;
import acdp.tools.Setup.Setup_Database;
import acdp.tools.Setup.Setup_TableDeclaration;

/**
 *
 *
 * @author Beat Hoermann
 */

@Setup_Database (
	name = "PersonDB",
	version = "1.5",
	tables = { "Person" }
)
public final class PersonDB extends CustomDatabase {
	public static final class CipherFactory implements ICipherFactory {
	   private final IvParameterSpec iv = new IvParameterSpec(new byte[] {
												114, -8, 22, -67, -71, 30, 118, -103,
												51, -45, -110, -65, 16, -127, -73, 103 });
	   
	   private final Key key = new SecretKeySpec(new byte[] { 114, -8, 23, -67,
	   											-71, 30, 118, -103, 51, -45, -110, -65,
	   											16, -127, -73, 103 }, "AES");

		@Override
	   public final Cipher createAndInitWrCipher(boolean encrypt) throws
	   					NoSuchAlgorithmException, NoSuchPaddingException,
	   					InvalidKeyException, InvalidAlgorithmParameterException {
	   	final Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
	   	cipher.init(encrypt ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, key,
	   																							iv);
	   	return cipher;
	   }

		@Override
	   public final Cipher createRoCipher() throws NoSuchAlgorithmException,
																		NoSuchPaddingException {
	   	return Cipher.getInstance("AES/CTR/NoPadding");
	   }

		@Override
	   public final void initRoCipher(Cipher cipher, boolean encrypt) throws
	   					InvalidKeyException, InvalidAlgorithmParameterException {
	   	cipher.init(encrypt ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, key,
	   																							iv);
	   }
	}
	
	@Setup_TableDeclaration("Person")
	public final example.PersonTable personTable = new example.PersonTable();
	
	PersonDB(Path mainFile, int opMode, boolean writeProtect,
																		int consistencyNumber) {
		open(mainFile, opMode, writeProtect, consistencyNumber, personTable);
	}
}
