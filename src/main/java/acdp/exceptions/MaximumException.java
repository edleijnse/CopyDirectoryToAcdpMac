/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.exceptions;

import acdp.internal.Table_;

/**
 * Thrown if one of the maximum values specified in the {@code nobsRowRef},
 * {@code nobsOutrowPtr} and {@code nobsRefCount} fields of the table layout is
 * violated.
 * <p>
 * Note that if these fields are all set equal to their maximum value of 8 then
 * it is very unlikely that this exception will ever be thrown.
 * However, setting all these fields equal to 8 almost always leads to an
 * unnecessarily large database.
 *
 * @author Beat Hoermann
 */
public final class MaximumException extends ACDPException {
	private static final long serialVersionUID = 2153489993308295824L;

	/**
    * Constructs this type of exception with a detailed message composed from
    * the specified table and message.
    *
    * @param table The table.
    * @param message The message.
    */
	public MaximumException(Table_ table, String message) {
   	super(table, message);
	}
}
