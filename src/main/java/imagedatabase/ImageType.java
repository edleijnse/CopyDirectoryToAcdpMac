package imagedatabase;

import acdp.design.SimpleType;
import acdp.exceptions.CreationException;

import java.util.Arrays;

/**
 * The image column type.
 *
 * @author Ed Leijnse
 */
public final class ImageType extends SimpleType<byte[]> {
	private static final String TD_PREFIX = "Image";
	
	@TypeFromDesc
	public static final ImageType createType(String typeDesc) throws
            CreationException {
		final TypeDesc td = new TypeDesc(typeDesc);
		if (!td.prefix.equals(TD_PREFIX) || td.scheme != Scheme.OUTROW ||
																td.nullable || td.limit != 4) {
			throw new CreationException("Illegal type descriptor.");
		}
		return new ImageType();
	}

	protected ImageType() {
		super(byte[].class, Scheme.OUTROW, false, 4, false);
	}
	
	@Override
	protected final String typeDescPrefix() {
		return TD_PREFIX;
	}

	@Override
	protected byte[] toBytes(byte[] val) throws NullPointerException {
		return val;
	}

	@Override
	protected byte[] fromBytes(byte[] bytes, int offset, int len)
															throws IndexOutOfBoundsException {
		return Arrays.copyOfRange(bytes, offset, offset + len);
	}

}
