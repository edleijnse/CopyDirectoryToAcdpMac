/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.exceptions;

/**
 * Thrown to indicate that a {@linkplain acdp.misc.Layout layout} is missing a
 * requested entry.
 *
 * @author Beat Hoermann
 */
public final class MissingEntryException extends ACDPException {
	private static final long serialVersionUID = 81731616913322022L;

	/**
    * Constructs this type of exception with the specified message.
    *
    * @param message The message.
    */
	public MissingEntryException(String message) {
   	super(message);
	}
}
