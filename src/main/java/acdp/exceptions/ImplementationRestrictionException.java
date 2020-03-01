/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.exceptions;

import acdp.internal.Table_;

/**
 * Thrown if an exception due to an implementation restricition has occurred.
 *
 * @author Beat Hoermann
 */
public final class ImplementationRestrictionException extends ACDPException {
	private static final long serialVersionUID = 459426032636050259L;
	
	/**
    * Constructs this type of exception with the specified detail message.
    *
    * @param message The message.
    */
	public ImplementationRestrictionException(String message) {
   	super(message);
	}

	/**
    * Constructs this type of exception with a detailed message composed from
    * the specified table and the specified message.
    *
    * @param table The table.
    * @param message The message.
    */
	public ImplementationRestrictionException(Table_ table, String message) {
   	super(table, message);
	}
	
	/**
    * Constructs this type of exception with a detail message composed from the
    * specified table and the specified column name.
	 * 
	 * @param table The table.
	 * @param columnName The column name.
	 * @param message The message.
	 */
	public ImplementationRestrictionException(Table_ table, String columnName,
																				String message) {
		super(table, columnName, message);
	}
}
