/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.exceptions;

import acdp.internal.Database_;

/**
 * Thrown to indicate that the {@link acdp.Unit unit} is broken.
 *
 * @author Beat Hoermann
 */
public final class UnitBrokenException extends ACDPException {
	private static final long serialVersionUID = 1531017140010301102L;

	/**
    * Constructs this type of exception with a detail message composed from
    * the specified database and with the specified cause.
	 * 
	 * @param db The database.
    * @param cause The cause.
	 */
	public UnitBrokenException(Database_ db, Throwable cause) {
		super(db, cause);
	}
	
	/**
    * Constructs this type of exception with a detail message composed from
    * the specified database and message and with the specified cause.
    *
    * @param db The database.
    * @param message The message.
    * @param cause The cause.
    */
	public UnitBrokenException(Database_ db, String message, Throwable cause) {
   	super(db, message, cause);
	}
}
