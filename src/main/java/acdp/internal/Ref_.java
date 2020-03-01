/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import acdp.Ref;

/**
 * Implements a reference.
 * A reference internally keeps an integer value greater than zero which is
 * equal to the index of the referenced row within the array of rows of the
 * referenced table.
 *
 * @author Beat Hoermann
 */
public final class Ref_ implements Ref {
	private static final long serialVersionUID = -290656526578519203L;

	/**
	 * The null-reference.
	 */
	public static final Ref_ NULL_REF = new Ref_(0);
	
	/**
	 * The row index of the reference.
	 */
	private final long rowIndex;
	
	/**
	 * The constructor.
	 * Since this class is an <em>internal</em> class, creating objects of this
	 * class is exclusively reserved to ACDP despite the public access level
	 * modifier.
	 * 
	 * @param rowIndex The row index, not allowed to be negative.
	 */
	public Ref_(long rowIndex) {
		this.rowIndex = rowIndex;
	}
	
	/**
	 * Returns the row index of this reference, hence, the index of the
	 * referenced row within the array of rows of the referenced table.
	 * The first row of a table has index 1.
	 * 
	 * @return The row index of this reference, never negative.
	 */
	public final long rowIndex() {
		return rowIndex;
	}
	
	@Override
	public String toString() {
		return Long.toString(rowIndex);
	}

	@Override
	public boolean equals(Object obj) {
		return (obj instanceof Ref_) && rowIndex == ((Ref_) obj).rowIndex;
	}

	@Override
	public int hashCode() {
		// Copied from Long.hashCode().
		return (int) (rowIndex ^ (rowIndex >>> 32));
	}
}
