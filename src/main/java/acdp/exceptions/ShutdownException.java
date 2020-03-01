/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.exceptions;

import acdp.internal.Database_;

/**
 * Thrown if the database is still used after it was closed.
 * <p>
 * More technical: Thrown if a file channel is requested from the file channel
 * provider after the file channel provider was shut down or if the
 * synchronization manager is shut down and there is still a client around that
 * 
 * <ul>
 *    <li>tries to get a unit or</li>
 *    <li>tries to open a read zone or</li>
 *    <li>tries to open the ACDP zone or</li>
 *    <li>tries to invoke a Kamikaze write.</li>
 * </ul>
 *
 * @author Beat Hoermann
 */
public final class ShutdownException extends ACDPException {
	private static final long serialVersionUID = -6933096858965095564L;

	/**
    * Constructs this type of exception with the specified message.
    *
    * @param message The message.
    */
	public ShutdownException(String message) {
   	super(message);
	}
	
	/**
    * Constructs this type of exception with a message composed from the
    * specified database and message.
    *
	 * @param db The database.
    * @param message The message.
    */
	public ShutdownException(Database_ db, String message) {
   	super(db, message);
	}
}
