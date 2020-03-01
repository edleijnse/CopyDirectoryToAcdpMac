/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.exceptions;

/**
 * Thrown to indicate that a manually added comment in a {@linkplain
 * acdp.misc.Layout layout} read from a file or a stream is not properly
 * followed or not followed at all by an entry  or an element of a sequence.
 *
 * @author Beat Hoermann
 */
public final class DanglingCommentException extends ACDPException {
	private static final long serialVersionUID = -3982399889280101296L;

	/**
    * The constructor.
    *
    * @param line The last line of the dangling comment.
    */
	public DanglingCommentException(String line) {
   	super("Dangling comment after \"" + line + "\".");
	}
}
