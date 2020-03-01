/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.exceptions;

import acdp.internal.Database_;
import acdp.internal.Table_;

/**
 * Thrown to indicate that an object can't be created due to any reason.
 *
 * @author Beat Hoermann
 */
public final class CreationException extends ACDPException {
	private static final long serialVersionUID = -3874039776597420238L;
	
	/**
    * Constructs this type of exception with the specified message.
    *
    * @param message The message.
    */
	public CreationException(String message) {
   	super(message);
	}
	
	/**
    * Constructs this type of exception with a message composed from the
    * specified database and the specified message.
    *
	 * @param db The database.
    * @param message The message.
    */
	public CreationException(Database_ db, String message) {
		super(db, message);
	}
	
	/**
    * Constructs this type of exception with a message composed from the
    * specified database, the specified table name and the specified message.
    *
	 * @param db The database.
	 * @param tableName The table name.
    * @param message The message.
    */
	public CreationException(Database_ db, String tableName, String message) {
		super(db, tableName, message);
	}
	
	/**
    * Constructs this type of exception with a message composed from the
    * specified table and the specified message.
    *
	 * @param table The table.
    * @param message The message.
    */
	public CreationException(Table_ table, String message) {
		super(table, message);
	}
	
	/**
    * Constructs this type of exception with a message composed from the
    * specified arguments.
    * 
    * @param table The table.
    * @param columnName The name of the column.
    * @param message The message.
    */
	public CreationException(Table_ table, String columnName, String message) {
   	super(table, columnName, message);
	}
	
	/**
    * Constructs this type of exception with the specified cause.
    *
    * @param cause The cause.
    */
	public CreationException(Throwable cause) {
   	super(cause);
	}
	
	/**
    * Constructs this type of exception with a detail message composed from the
    * specified database and the specified cause.
    *
	 * @param db The database.
    * @param cause The cause.
    */
	public CreationException(Database_ db, Throwable cause) {
   	super(db, cause);
	}
	
	/**
    * Constructs this type of exception with a detail message composed from the
    * specified table and with the specified cause.
    *
	 * @param table The table.
    * @param cause The cause.
    */
	public CreationException(Table_ table, Throwable cause) {
   	super(table, cause);
	}
	
	/**
    * Constructs this type of exception with the specified message and cause.
    *
    * @param message The message.
    * @param cause The cause.
    */
	public CreationException(String message, Throwable cause) {
   	super(message, cause);
	}
	
	/**
    * Constructs this type of exception with a detail message composed from the
	 * first two parameters and with the specified cause.
    *
    * @param db The database.
    * @param message The message.
    * @param cause The cause.
    */
	public CreationException(Database_ db, String message, Throwable cause) {
   	super(db, message, cause);
	}
	
	/**
    * Constructs this type of exception with a detail message composed from the
    * specified table name and message and with the specified cause.
    *
    * @param tableName The table name.
    * @param message The message.
    * @param cause The cause.
    */
	public CreationException(String tableName, String message, Throwable cause) {
   	super(tableName, message, cause);
	}
	
	/**
    * Constructs this type of exception with a detail message composed from the
    * specified table and messsage and with the specified cause.
    *
    * @param table The table.
    * @param message The message.
    * @param cause The cause.
    */
	public CreationException(Table_ table, String message, Throwable cause) {
   	super(table, message, cause);
	}
	
	/**
    * Constructs this type of exception with a detail message composed from the
    * specified database, the specified table name, the specified message and
    * with the specified cause.
    *
	 * @param db The database.
	 * @param tableName The table name.
    * @param message The message.
    * @param cause The cause.
    */
	public CreationException(Database_ db, String tableName, String message,
																				Throwable cause) {
   	super(db, tableName, message, cause);
	}
}
