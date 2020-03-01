/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package example;

import acdp.design.CustomDatabase;
import acdp.design.ICipherFactory;
import acdp.tools.Setup.Setup_Database;
import acdp.tools.Setup.Setup_TableDeclaration;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.file.Path;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

/**
 *
 *
 * @author Ed Leijnse
 */

@Setup_Database (
	name = "ImageDB",
	version = "1.0",
	tables = { "Image" }
)
public final class ImageDB extends CustomDatabase {
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

	@Setup_TableDeclaration("Image")
	public final ImageTable imageTable = new ImageTable();

	public ImageDB(Path mainFile, int opMode, boolean writeProtect,
            int consistencyNumber) {
		open(mainFile, opMode, writeProtect, consistencyNumber, imageTable);
	}
}
