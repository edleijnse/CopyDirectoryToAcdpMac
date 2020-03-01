/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.exceptions;

import acdp.internal.Ref_;
import acdp.internal.Table_;

/**
 * Thrown if a row reference is illegal.
 * A row rerence is illegal if and only if one of the following two conditions
 * is satisfied while a row is being accessed:
 * 
 * <ul>
 * 	<li>The reference points to a row that does not exist within the
 *        table.</li>
 * 	<li>The reference points to a row gap.</li>
 * </ul>
 * <p>
 * Note: To be illegal a row reference cannot be equal to {@code null}.
 *
 * @author Beat Hoermann
 */
public final class IllegalReferenceException extends ACDPException {
	private static final long serialVersionUID = -8656581671127524871L;
	private final boolean rowGap;
	
	/**
    * The constructor.
    *
    * @param table The table.
    * @param ref The reference.
    * @param rowGap The flag that indicates if the pointed row is a row gap,
    *        {@code true} if it is, {@code false} if not.
    */
	public IllegalReferenceException(Table_ table, Ref_ ref, boolean rowGap) {
		super(table, "Row with index " + ref.rowIndex() +
										(rowGap ? " is a row gap." : " does not exist."));
		this.rowGap = rowGap;
	}
	
	/**
	 * Indicates if this exception was thrown due to a row gap.
	 * 
	 * @return The boolean value {@code true} if and only if this exception was
	 *         thrown due to a row gap.
	 */
	public boolean rowGap() {
		return rowGap;
	}
}
