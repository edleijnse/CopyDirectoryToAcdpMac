/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store;

import acdp.design.SimpleType;
import acdp.internal.Ref_;
import acdp.internal.misc.Utils_;
import acdp.internal.types.RefType_;
import acdp.misc.Utils;

/**
 * The reference type is actually represented by the {@link RefType_} class.
 * In the class description of that class it is explained, why the {@code
 * RefType_} class does not extend the {@link SimpleType} class.
 * However, there are rare occasions where a reference type being a simple type
 * reduces the complexity of the source code.
 *
 * @author Beat Hoermann
 */
public final class RefST extends SimpleType<Ref_> {
	/**
	 * A reference type of a particular length is a singleton.
	 */
	private static final RefST[] refSTs = new RefST[] { null, null, null, null,
																null, null, null, null, null };
	/**
	 * The factory method.
	 * Creates objects of this class.
	 * The number of bytes of the byte representation a reference is greater
	 * than or equal to 1 and less than or equal to 8.
	 * 
	 * @param  length The length of the reference type, must be greater than or
	 *         equal to 1 and less than or equal to 8.
	 *         
	 * @return The created reference type.
	 * 
	 * @throws IllegalArgumentException If {@code length} is less than 1 or
	 *         greater than 8.
	 */
	public static final RefST newInstance(int length) throws
																		IllegalArgumentException {
		if (length < 1 || 8 < length) {
			throw new IllegalArgumentException("The value of \"length\" must " +
													"be greater than or equal to 1 and " +
													"less than or equal to 8: " + length);
		}
		// 1 <= length <= 8
		RefST refST = refSTs[length];
		if (refST == null) {
			refST = new RefST(length);
			refSTs[length] = refST;
		}
		return refST;
	}
	
	/**
	 * The private constructor.
	 * The {@link RefST#newInstance} factory method is the only method that
	 * invokes this constructor.
	 * This guarantees that the value of the {@code length} parameter is greater
	 * than or equal to 1 and less than or equal to 8.
	 * Therefore, this constructor should never throw an exception.
	 * 
	 * @param length The length of this reference type, hence, the number of
	 *        bytes of the byte representation of a reference of this type.
	 */
	private RefST(int length) {
		super(Ref_.class, Scheme.INROW, false, length, false);
	}
	
	@Override
	protected final byte[] toBytes(Ref_ val) throws NullPointerException {
		return val == Ref_.NULL_REF ? Utils_.zeros[length] :
													Utils.unsToBytes(val.rowIndex(), length);
	}

	@Override
	protected final int toBytes(Ref_ val, byte[] bytes, int offset) throws
										NullPointerException, IndexOutOfBoundsException {
		if (val == Ref_.NULL_REF)
			System.arraycopy(Utils_.zeros[length], 0, bytes, offset, length);
		else {
			Utils.unsToBytes(val.rowIndex(), length, bytes, offset);
		}
		return length;
	}

	@Override
	protected final Ref_ fromBytes(byte[] bytes, int offset, int len) throws
																	IndexOutOfBoundsException {
		// Note that a byte array filled with zeros is converted to "null" rather
		// than "Ref_.NULL_REF".
		return Utils.isZero(bytes, offset, length) ? null : new Ref_(
												Utils.unsFromBytes(bytes, offset, length));
	}
}
