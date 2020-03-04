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
		if (!td.prefix.equals(TD_PREFIX) || td.nullable) {
			throw new CreationException("Illegal type descriptor");
		}
		return new BigIntegerType(td.limit);
	}
	
	BigIntegerType(int limit) throws IllegalArgumentException {
		super(BigInteger.class, Scheme.INROW, false, limit, true);
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
