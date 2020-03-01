/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.misc;

import java.io.IOException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;

import acdp.exceptions.CreationException;
import acdp.exceptions.IOFailureException;
import acdp.internal.Database_;
import acdp.internal.FileIOException;
import acdp.internal.Verifyer;

/**
 * Provides some methods that are needed only in special situations.
 *
 * @author Beat Hoermann
 */
public final class ACDP {
	/**
	 * Verifies the integrity of the specified WR database by verifying the
	 * integrity of the table data for each table of the database, and, provided
	 * that the {@code fix} argument is set to {@code true}, makes an attempt to
	 * fix any detected integrity violations that have an automatic fix.
	 * <p>
	 * Each table is subjected to a series of <em>tests</em> each test verifying
	 * a specific aspect of the table's integrity.
	 * If a test detects an integrity violation then this method returns {@code
	 * false}, and, provided that the {@code report} argument is set to {@code
	 * true} sends a suitable message to the {@linkplain System#out standard
	 * output stream}.
	 * If the {@code fix} argument is set to {@code true} then the test starts
	 * an attempt to fix the issue, provided that the test has an automatic fix.
	 * <p>
	 * A test either <em>completes</em> or <em>aborts</em>.
	 * If a test aborts then some table data could not be verified.
	 * <p>
	 * A test completes <em>successfully</em> if and only if the test either did
	 * not detect an integrity violation or if the test was able to resolve a
	 * detected integrity violation.
	 * <p>
	 * If all tests for all tables in the database are successfully completed
	 * then this method returns {@code true}, and, provided that the {@code
	 * report} argument is set to {@code true}, sends one of the following
	 * messages to the standard output stream:
	 * 
	 * <pre>
	 * Completed - No violations detected.</pre>
	 * 
	 * or
	 * 
	 * <pre>
	 * Completed - n violations detected, n fixed.</pre>
	 * 
	 * where {@code n} denotes the number of detected integrity violations.
	 * <p>
	 * If you plan to invoke this method with the {@code fix} argument set to
	 * {@code true} then you may want to {@linkplain
	 * acdp.Database#zip backup the database} before executing this
	 * method.
	 * No changes are made to the database if the {@code fix} argument is equal
	 * to {@code false}.
	 * <p>
	 * Note that this method fails if the database is not a WR database or if
	 * the database is currently open.
	 * In general, this method throws an exception if and only if the
	 * prerequisites for verifying the integrity of the database are not met.
	 * <p>
	 * Note also that if the specified file does not contain a valid layout for
	 * a WR database then this method may throw an exception that is different
	 * from the listed exceptions.
	 * 
	 * @param  layoutFile The WR database layout file, not allowed to be {@code
	 *         null}.
	 * @param  fix The information whether an attempt should be made to fix any
	 *         detected integrity violations.
	 * @param  report The information whether a result report should be sent to
	 *         the standard output stream.
	 *         If set to {@code true} then this method sends a result report to
	 *         the standard output stream.
	 *         
	 * @return The boolean value {@code true} if and only if all tests
	 *         successfully terminated, that is, none of the tests was aborted
	 *         and no integrity violation was detected or the {@code fix}
	 *         argument is set to {@code true} and all detected integrity
	 *         violations could be fixed.
	 * 
	 * @throws UnsupportedOperationException If the database is an RO database.
	 * @throws NullPointerException If {@code layoutFile} is {@code null}.
	 * @throws OverlappingFileLockException If the WR database is currently open.
	 * @throws CreationException If the database can't be opened due to any other
	 *         reason.
	 */
	public static final boolean verify(Path layoutFile, boolean fix,
																					boolean report) {
		return new Verifyer().run(layoutFile, fix, report);
	}
	
	/**
	 * Creates the backing files of the WR database described by the layout
	 * contained in the specified layout file.
	 * <p>
	 * If the specified file does not contain a valid layout for a WR database
	 * then this method may throw an exception that is different from the listed
	 * exceptions.
	 * 
	 * @param  layoutFile The WR database layout file, not allowed to be {@code
	 *         null}.
	 * 
	 * @throws NullPointerException If {@code layoutFile} is {@code null}.
	 * @throws IOFailureException If the layout file does not exist or if at
	 *         least one of the backing files already exists or if another I/O
	 *         error occurs.
	 */
	public static final void createDBFiles(Path layoutFile) throws
												NullPointerException, IOFailureException {
		// Read the layout from the layout file.
		final Layout layout;
		try {
			layout = Layout.fromFile(layoutFile);
		} catch (IOException e) {
			throw new IOFailureException(e);
		}
		// Create the backing database files.
		Database_.createFiles(layout, layoutFile.getParent());
	}
	
	/**
	 * Deletes the backing files of the specified WR database and optionally
	 * deletes the layout file too, thus completely removing the database.
	 * <p>
	 * This method fails if the database is not a WR database or if the database
	 * is currently open.
	 * <p>
	 * If the specified file does not contain a valid layout for a WR database
	 * then this method may throw an exception that is different from the listed
	 * exceptions.
	 * <p>
	 * Note that emptying the database can be achieved by executing
	 * 
	 * <pre>
	 * ACDP.deleteDBFiles(layoutFile, false);
	 * ACDP.createDBFiles(layoutFile);</pre>
	 * 
	 * where {@code layoutFile} denotes the path to the layout file of the WR
	 * database (file name included).
	 * 
	 * @param  layoutFile The WR database layout file, not allowed to be {@code
	 *         null}.
	 * @param  deleteLayoutFile The information whether the specified layout file
	 *         should be deleted or not.
	 *         If set to {@code true} then this method deletes the specified
	 *         layout file.
	 *         
	 * @throws UnsupportedOperationException If the database is an RO database.
	 * @throws NullPointerException If {@code layoutFile} is {@code null}.
	 * @throws OverlappingFileLockException If the WR database is currently open.
	 * @throws CreationException If the database can't be opened due to any other
	 *         reason.
	 * @throws IOFailureException If the layout file does not exist or if at
	 *         least one of the backing files does not exist or if another I/O
	 *         error occurs.
	 */
	public static final void deleteDBFiles(Path layoutFile,
																boolean deleteLayoutFile) throws
									UnsupportedOperationException, NullPointerException,
									OverlappingFileLockException, CreationException,
																				IOFailureException {
		try (Database_ db = new Database_(layoutFile, 0, false, -1, null)) {
			db.deleteFiles(deleteLayoutFile);
		} catch(FileIOException e) {
			throw new IOFailureException(e);
		}
	}
	
	/**
	 * Prevent object construction.
	 */
	private ACDP() {
	}
}
