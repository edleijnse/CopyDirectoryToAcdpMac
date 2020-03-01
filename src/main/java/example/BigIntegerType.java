/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package example;

import java.math.BigInteger;
import java.util.Arrays;

import acdp.design.SimpleType;
import acdp.exceptions.CreationException;

/**
 *
 *
 * @author Beat Hoermann
 */
public final class BigIntegerType extends SimpleType<BigInteger> {
	private static final String TD_PREFIX = "BigInteger";
	
	@TypeFromDesc
	public static final BigIntegerType createType(String typeDesc) throws
																				CreationException {
		final TypeDesc td = new TypeDesc(typeDesc);
		if (!td.prefix.equals(TD_PREFIX)) {
			throw new CreationException("Illegal type descriptor");
		}
		return new BigIntegerType(td.nullable, td.limit);
	}
	
	BigIntegerType(boolean nullable, int maxLength)
															throws IllegalArgumentException {
		super(BigInteger.class, Scheme.INROW, nullable, maxLength, true);
	}

	@Override
	protected final String typeDescPrefix() {
		return TD_PREFIX;
	}
	
	@Override
	protected final byte[] toBytes(BigInteger val) throws NullPointerException {
		return val.toByteArray();
	}

	@Override
	public final BigInteger fromBytes(byte[] bytes, int offset, int len) throws
																	IndexOutOfBoundsException {
		return new BigInteger(Arrays.copyOfRange(bytes, offset, offset + len));
	}
}
