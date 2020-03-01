/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Thrown to indicate that an {@code IOException} has occurred where a file or
 * a directory was involved so that we can provide the path.
 *
 * @author Beat Hoermann
 */
public final class FileIOException extends IOException {
	private static final long serialVersionUID = -3663373700197520478L;
	/**
	 * Indicates if the end of the file has unexpectedly been reached while this
	 * file was read.
	 */
	private final boolean eof;

	/**
    * Constructs this type of exception with a detail message returned by
    * {@code path.toString()} and the specified cause.
    *
    * @param path The path of a file or a directory.
    * @param cause The cause.
    */
	public FileIOException(Path path, IOException cause) {
   	super(path.toString(), cause);
		this.eof = false;
	}
	
	/**
    * Constructs this type of exception with a detail message depending on
    * the {@code path} and {@code eof} arguments and the specified cause.
    *
    * @param path The path of a file or a directory.
    * @param cause The cause.
    * @param eof Indicates if the end of the file has unexpectedly been reached
    *        while the file was read.
    */
	public FileIOException(Path path, IOException cause, boolean eof) {
		super((eof ? "Unexpected end of file: " : "") + path, cause);
		this.eof = eof;
	}
	
	/**
	 * Returns the flag indicating if the end of the file has unexpectedly been
	 * reached while this file was read.
	 * 
	 * @return The boolean value {@code true} if the end of the file has
	 *         unexpectedly been reached while this file was read.
	 */
	public final boolean eof() {
		return eof;
	}
}
