/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.exceptions;

/**
 * Thrown to indicate that the lines of text in a {@linkplain acdp.misc.Layout
 * layout} read from a file or a stream are not properly indented.
 *
 * @author Beat Hoermann
 */
public final class IndentationException extends ACDPException {
	private static final long serialVersionUID = -4360265762798593445L;

	/**
    * The constructor.
    *
    * @param line The line of text where the wrong indentation was discovered.
    */
	public IndentationException(String line) {
   	super("Wrong indentation at \"" + line + "\".");
	}
}
