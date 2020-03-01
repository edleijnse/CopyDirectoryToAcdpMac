/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.exceptions;

import acdp.internal.Column_;
import acdp.internal.Database_;
import acdp.internal.Table_;

/**
 * The super class of all other exception types in this package.
 *
 * @author Beat Hoermann
 */
public class ACDPException extends RuntimeException {
	private static final long serialVersionUID = 2869813967954453711L;
	
	/**
	 * Composes a text that contains the specified database name, the specified
	 * table name and the specified column name.
	 * 
	 * @param  dbName The database name, may be {@code null}.
	 * @param  tableName The table name, may be {@code null}.
	 * @param  columnName The column name, may be {@code null}.
	 * 
	 * @return The described text.
	 */
	public static String compose(String dbName, String tableName,
																				String columnName) {
		String str = dbName != null ? "Database \"" + dbName + "\"" : "";
		if (tableName != null) {
			if (!str.isEmpty()) {
				str += ", ";
			}
			str += "Table \"" + tableName + "\"";
		}
		if (columnName != null) {
			str += ", Column \"" + columnName + "\"";
		}
		return str;
	}
	
	/**
	 * Returns a text that contains the name of the specified database.
	 * The detail message of an exception can be prefixed with this text.
	 * 
	 * @param  db The database.
	 * 
	 * @return The described text.
	 */
	public static String prefix(Database_ db) {
		return compose(db.name(), null, null) + ": ";
	}
	
	/**
	 * Returns a text that contains the specified table name.
	 * The detail message of an exception can be prefixed with this text.
	 * 
	 * @param  tableName The table name.
	 * 
	 * @return The described text.
	 */
	public static String prefix(String tableName) {
		return compose(null, tableName, null) + ": ";
	}
	
	/**
	 * Returns a text that contains the specified database name and the specified
	 * table name.
	 * The detail message of an exception can be prefixed with this text.
	 * 
	 * @param  dbName The database name.
	 * @param  tableName The table name.
	 * 
	 * @return The described text.
	 */
	public static String prefix(String dbName, String tableName) {
		return compose(dbName, tableName, null) + ": ";
	}
	
	/**
	 * Returns a text that contains the name of the specified database and the
	 * specified table name.
	 * The detail message of an exception can be prefixed with this text.
	 * 
	 * @param  db The database.
	 * @param  tableName The table name.
	 * 
	 * @return The described text.
	 */
	public static String prefix(Database_ db, String tableName) {
		return compose(db.name(), tableName, null) + ": ";
	}
	
	/**
	 * Returns a text that contains the name of the database and the name of the
	 * specified table.
	 * The detail message of an exception can be prefixed with this text.
	 * 
	 * @param  table The table.
	 * 
	 * @return The described text.
	 */
	public static String prefix(Table_ table) {
		return compose(table.db().name(), table.name(), null) + ": ";
	}
	
	/**
	 * Returns a text that contains the name of the database, the name of the
	 * specified table and the specified column name.
	 * The detail message of an exception can be prefixed with this text.
	 * 
	 * @param  table The table.
	 * @param  columnName The name of the column.
	 * 
	 * @return The described text.
	 */
	public static String prefix(Table_ table, String columnName) {
		return compose(table.db().name(), table.name(), columnName) + ": ";
	}
	
	/**
	 * Returns a text that contains the name of the database, the name of the
	 * specified table and the name of the specified column.
	 * The detail message of an exception can be prefixed with this text.
	 * 
	 * @param  table The table.
	 * @param  column The column.
	 * 
	 * @return The described text.
	 */
	public static String prefix(Table_ table, Column_<?> column) {
		return compose(table.db().name(), table.name(), column.name()) + ": ";
	}
	
	/**
    * Constructs a new ACDP exception with a detail message composed from the
    * specified database.
    * The detail message is saved for later retrieval by the {@link
    * #getMessage()} method.
    * The cause is not initialized, and may subsequently be initialized by a
    * call to {@link #initCause}. 
    * 
	 * @param db The database.
	 */
	ACDPException(Database_ db) {
		this(compose(db.name(), null, null));
	}
	
	/**
    * Constructs a new ACDP exception with a detail message composed from the
    * specified table.
    * The detail message is saved for later retrieval by the {@link
    * #getMessage()} method.
    * The cause is not initialized, and may subsequently be initialized by a
    * call to {@link #initCause}. 
    * 
	 * @param table The table.
	 */
	ACDPException(Table_ table) {
		this(compose(table.db().name(), table.name(), null));
	}
	
   /**
    * Constructs a new ACDP exception with the specified detail message.
    * The cause is not initialized, and may subsequently be initialized by a
    * call to {@link #initCause}.
    *
    * @param message The detail message.
    *        The detail message is saved for later retrieval by the
    *        {@link #getMessage()} method.
    */
	public ACDPException(String message) {
		super(message);
	}
	
   /**
    * Constructs a new ACDP exception with a detail message composed from the
    * specified database und the specified message.
    * The cause is not initialized, and may subsequently be initialized by a
    * call to {@link #initCause}.
	 * (The detail message is saved for later retrieval by the {@link
	 * #getMessage()} method.)
    *
	 * @param db The database.
	 * @param message The message.
    */
	public ACDPException(Database_ db, String message) {
		super(prefix(db) + message);
	}
	
   /**
    * Constructs a new ACDP exception with a detail message composed from the
    * specified arguments.
    * The cause is not initialized, and may subsequently be initialized by a
    * call to {@link #initCause}.
	 * (The detail message is saved for later retrieval by the {@link
	 * #getMessage()} method.)
    *
	 * @param db The database.
	 * @param tableName The name of the table.
	 * @param message The message.
    */
	ACDPException(Database_ db, String tableName, String message) {
		super(prefix(db, tableName) + message);
	}
	
   /**
    * Constructs a new ACDP exception with a detail message composed from the
    * specified table and the specified message.
    * The cause is not initialized, and may subsequently be initialized by a
    * call to {@link #initCause}.
	 * (The detail message is saved for later retrieval by the {@link
	 * #getMessage()} method.)
    *
	 * @param table The table.
	 * @param message The message.
    */
	public ACDPException(Table_ table, String message) {
		super(prefix(table) + message);
	}
	
	/**
    * Constructs a new ACDP exception with a detail message composed from the
    * specified arguments.
    * The cause is not initialized, and may subsequently be initialized by a
    * call to {@link #initCause}.
	 * (The detail message is saved for later retrieval by the {@link
	 * #getMessage()} method.)
	 * 
	 * @param table The table.
	 * @param columnName The name of the column.
	 * @param message The message.
	 */
	ACDPException(Table_ table, String columnName, String message) {
		super(prefix(table, columnName) + message);
	}
	
   /**
    * Constructs a new ACDP exception with the specified cause and a detail
    * message of {@code (cause==null ? null : cause.toString())} (which
    * typically contains the class and detail message of {@code cause}). 
    * This constructor is useful for ACDP exceptions that are little more than
    * wrappers for other throwables.
    *
    * @param cause The cause (which is saved for later retrieval by the
    *        {@link #getCause()} method). 
    *        (A <code>null</code> value is permitted, and indicates that the
    *        cause is nonexistent or unknown.)
    */
	ACDPException(Throwable cause) {
		super(cause);
	}
	
	/**
	 * Invokes the {@link #ACDPException(String, Throwable)} with a detail
	 * message composed from the first argument.
	 * 
	 * @param db The database.
	 * @param cause The cause (which is saved for later retrieval by the
	 *        {@link #getCause()} method).
	 *        (A {@code null} value is permitted, and indicates that the cause
	 *        is nonexistent or unknown.)
	 */
	ACDPException(Database_ db, Throwable cause) {
		this(compose(db.name(), null, null), cause);
	}
	
	/**
	 * Invokes the {@link #ACDPException(String, Throwable)} with a detail
	 * message composed from the first argument.
	 * 
	 * @param table The table.
	 * @param cause The cause (which is saved for later retrieval by the
	 *        {@link #getCause()} method).
	 *        (A {@code null} value is permitted, and indicates that the cause
	 *        is nonexistent or unknown.)
	 */
	ACDPException(Table_ table, Throwable cause) {
		this(compose(table.db().name(), table.name(), null), cause);
	}
	
	/**
	 * Constructs a new ACDP exception with the specified detail message and
	 * cause.
	 * <p>
	 * Note that the detail message associated with {@code cause} is <i>not</i>
	 * automatically incorporated in this runtime exception's detail message.
	 * 
	 * @param message The detail message (which is saved for later retrieval
	 *        by the {@link #getMessage()} method).
	 * @param cause The cause (which is saved for later retrieval by the
	 *        {@link #getCause()} method).
	 *        (A {@code null} value is permitted, and indicates that the cause
	 *        is nonexistent or unknown.)
	 */
	public ACDPException(String message, Throwable cause) {
		super(message, cause);
	}
	
	/**
	 * Constructs a new ACDP exception with a detail message composed from the
	 * specified database and message and with the specified cause.
	 * (The detail message is saved for later retrieval by the {@link
	 * #getMessage()} method.)
	 * <p>
	 * Note that the detail message associated with {@code cause} is <i>not</i>
	 * automatically incorporated in this runtime exception's detail message.
	 * 
	 * @param db The database.
	 * @param message The message.
	 * @param cause The cause (which is saved for later retrieval by the
	 *        {@link #getCause()} method).
	 *        (A {@code null} value is permitted, and indicates that the cause
	 *        is nonexistent or unknown.)
	 */
	public ACDPException(Database_ db, String message, Throwable cause) {
		this(prefix(db) + message, cause);
	}
	
	/**
	 * Constructs a new ACDP exception with a detail message composed from the
	 * first two arguments and the specified cause.
	 * (The detail message is saved for later retrieval by the {@link
	 * #getMessage()} method.)
	 * <p>
	 * Note that the detail message associated with {@code cause} is <i>not</i>
	 * automatically incorporated in this runtime exception's detail message.
	 * 
	 * @param tableName The name of the table.
	 * @param message The message.
	 * @param cause The cause (which is saved for later retrieval by the
	 *        {@link #getCause()} method).
	 *        (A {@code null} value is permitted, and indicates that the cause
	 *        is nonexistent or unknown.)
	 */
	ACDPException(String tableName, String message, Throwable cause) {
		this(prefix(tableName) + message, cause);
	}
	
	/**
	 * Constructs a new ACDP exception with a detail message composed from the
	 * first two arguments and the specified cause.
	 * (The detail message is saved for later retrieval by the {@link
	 * #getMessage()} method.)
	 * <p>
	 * Note that the detail message associated with {@code cause} is <i>not</i>
	 * automatically incorporated in this runtime exception's detail message.
	 * 
	 * @param table The table.
	 * @param message The message.
	 * @param cause The cause (which is saved for later retrieval by the
	 *        {@link #getCause()} method).
	 *        (A {@code null} value is permitted, and indicates that the cause
	 *        is nonexistent or unknown.)
	 */
	ACDPException(Table_ table, String message, Throwable cause) {
		this(prefix(table) + message, cause);
	}
	
	/**
	 * Constructs a new ACDP exception with a detail message composed from the
	 * first three arguments and the specified cause.
	 * (The detail message is saved for later retrieval by the {@link
	 * #getMessage()} method.)
	 * <p>
	 * Note that the detail message associated with {@code cause} is <i>not</i>
	 * automatically incorporated in this runtime exception's detail message.
	 * 
	 * @param db The database.
	 * @param tableName The name of the table.
	 * @param message The message.
	 * @param cause The cause (which is saved for later retrieval by the
	 *        {@link #getCause()} method).
	 *        (A {@code null} value is permitted, and indicates that the cause
	 *        is nonexistent or unknown.)
	 */
	ACDPException(Database_ db, String tableName, String message,
																				Throwable cause) {
		this(prefix(db, tableName) + message, cause);
	}
}
