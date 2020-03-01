/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import acdp.Information.DatabaseInfo.DBType;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CreationException;
import acdp.internal.store.wr.WRStore;

/**
 * Provides the {@link #run} method which verifies the integrity of the table
 * data for each table of the database.
 * <p>
 * Ensuring the integrity of the table data should ensure general error-free
 * operation of ACDP within the documented exceptional situations.
 *
 * @author Beat Hoermann
 */
public final class Verifyer {
	/**
	 * Reports events and messages sent by the individual tests by sending
	 * suitable messages to the {@linkplain System#out standard output stream}.
	 * 
	 * @author Beat Hoermann
	 */
	public static final class Reporter {
		private final boolean fix;
		private final boolean print;
		
		private int totalViolations;
		private int totalFixed;
		private int totalAborted;
		
		private int violations;
		private int fixed;
		private int aborted;
		
		/**
		 * One of two constructors.
		 * 
		 * @param fix The information whether an attempt should be made by the
		 *        tests to fix a detected integrity violation.
		 * @param print The information whether the report should send the report
		 *        to the standard output stream.
		 *        If set to {@code false} then the reporter is turned off and has
		 *        no effect.
		 */
		private Reporter(boolean fix, boolean print) {
			this.fix = fix;
			this.print = print;
			
			totalViolations = 0;
			totalFixed = 0;
			totalAborted = 0;
			
			violations = 0;
			fixed = 0;
			aborted = 0;
		}
		
		/**
		 * The copy constructor.
		 * 
		 * @param r The reporter to copy, not allowed to be {@code null}.
		 */
		private Reporter(Reporter r) {
			fix = r.fix;
			print = r.print;
			
			totalViolations = r.totalViolations + r.violations;
			totalFixed = r.totalFixed + r.fixed;
			totalAborted = r.totalAborted + r.aborted;
			
			violations = 0;
			fixed = 0;
			aborted = 0;
		}
		
		/**
		 * Prints the specified status message.
		 * 
		 * @param message The message, not allowed to be {@code null}.
		 */
		private final void status(String message) {
			if (print) {
				System.out.println(message);
			}
		}
		
		/**
		 * Reports an integrity violation.
		 * 
		 * @param n The number of the test.
		 * @param message The message, not allowed to be {@code null}.
		 */
		public final void violation(int n, String message) {
			if (print) {
				System.out.println("      Test " + n + " violation detected: " +
																							message);
				violations++;
			}
		}
		
		/**
		 * Reports the start of a fix.
		 */
		public final void fixing( ) {
			if (print) {
				System.out.println("      Fixing ...");
			}
		}
		
		/**
		 * Reports the termination of a fix.
		 */
		public final void fixed() {
			if (print) {
				System.out.println("      Fixed");
				fixed++;
			}
		}
		
		/**
		 * Reports the abort of the test with the specified number.
		 * 
		 * @param n The number of the aborted test.
		 * @param e The exception that lead to the abortion, not allowed to be
		 *        {@code null}.
		 */
		public final void aborted(int n, Exception e) {
			if (print) {
				final String msg = e.getMessage();
				if (msg != null)
					System.out.println("      Test " + n + " aborted: " + msg);
				else {
					System.out.println("      Test " + n + " aborted.");
					e.printStackTrace();
				}
				aborted++;
			}
		}
		
		/**
		 * Composes and prints a summary over the results of the tests on either
		 * the table or the database level.
		 * 
		 * @param table The information whether the summary should be composed on
		 *        the level of the table ({@code true}) or the level of the
		 *        database ({@code false}).
		 */
		private final void printReport(boolean table) {
			if (print) {
				String s;
				final int v;
				final int f;
				final int a;
				if (table) {
					s = "   ";
					v = violations;
					f = fixed;
					a = aborted;
				}
				else {
					s = "";
					v = totalViolations;
					f = fixed;
					a = totalAborted;
				}
			
				s += a > 0 ? "Aborted - " : "Completed - ";
				if (a > 0) {
					if (a == 1)
						s += "1 test aborted, ";
					else {
						s += a + " tests aborted, ";
					}
				}
				if (v > 0) {
					if (v == 1)
						s += "1 violation detected";
					else {
						s += v + " violations detected";
					}
				}
				else {
					s += a > 0 ? "n" : "N";
					s += "o violations detected";
				}
				if (a > 0) {
					s += " until abort";
				}
				s += fix && v > 0 ? ", " + f + " fixed." : ".";
				
				System.out.println(s);
			}
		}
	}
	
	/**
	 * Verifies the integrity of the specified database.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1}.
	 * 
	 * @param  db The database to check, not allowed to be {@code null}.
	 * @param  fix The information whether an attempt should be made to fix any
	 *         detected integrity violations.
	 * @param  report The information whether a result report should be send to
	 *         the standard output stream.
	 *         If set to {@code true} then this method sends a result report to
	 *         the standard output stream.
	 *        
	 * @return The boolean value {@code true} if and only if all tests
	 *         successfully terminated, that is, none of the tests was aborted
	 *         and no integrity violation was detected or the {@code fix}
	 *         argument is set to {@code true} and all detected integrity
	 *         violations could be fixed.
	 */
	private final boolean verify(Database_ db, boolean fix, boolean report) {
		final Table_[] tables = (Table_[]) db.getTables();
		final int n = tables.length;
		
		// Build initial gaps map.
		final Map<WRStore, long[]> gapsMap = new HashMap<>((int) (n * 4.0 /
																							3.0) + 1);
		for (Table_ table : tables) {
			gapsMap.put((WRStore) table.store(), null);
		}
		
		boolean good = true;
		
		Reporter rp = new Reporter(fix, report);
		rp.status("Checking database \"" + db + "\" ...");
		for (int i = 0; i < n; i++) {
			final Table_ table = tables[i];
			rp.status("   Checking table \"" + table + "\" (" + (i + 1) + " of " +
																						n + ") ...");
			good = good && ((WRStore) table.store()).verifyIntegrity(fix, rp,
																							gapsMap);
			rp.printReport(true);
			rp = new Reporter(rp);
		}
		rp.printReport(false);
		
		return good;
	}
	
	/**
	 * Verifies the integrity of the specified database by verifying the
	 * integrity of the table data for each table of the database, and, provided
	 * that the {@code fix} argument is set to {@code true}, makes an attempt to
	 * fix any detected integrity violations that have an automatic fix.
	 * <p>
	 * If this method throws an exception then the prerequisites for verifying
	 * the integrity are not met.
	 * Otherwise, the process of verifying the integrity starts and this method
	 * will no longer throw an exception.
	 * <p>
	 * Each table is subjected to a series of <em>tests</em> each test verifying
	 * a specific aspect of integrity.
	 * If a test detects an integrity violation it sends a suitable message
	 * to the {@linkplain System#out standard output stream}.
	 * If the {@code fix} argument is set to {@code true} then the test starts
	 * an attempt to fix the issue, provided that the test has an automatic fix.
	 * <p>
	 * A test either <em>completes</em> or <em>aborts</em>.
	 * If a test aborts then not all table data could be verified.
	 * <p>
	 * A test terminates <em>successfully</em> if and only if it has not aborted
	 * and if it has not detected an integrity violation or if it has been able
	 * to fix a detected integrity violation.
	 * <p>
	 * If all tests for a table successfully terminate then the verification of
	 * the table successfully terminates and if the verification of all tables
	 * successfully terminate then the verification of the database successfully
	 * terminates.
	 * In such a case this method returns {@code true} and, provided that the
	 * {@code print} argument is set to {@code true}, sends one of the following
	 * messages to the standard output stream:
	 * 
	 * <pre>
	 * Completed - No violations detected.</pre>
	 * 
	 * or
	 * 
	 * <pre>
	 * Completed - x violations detected, x fixed.</pre>
	 * 
	 * where {@code x} denotes the number of detected violations.
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
	 * <p>
	 * Note also that if the specified file does not contain a valid layout for
	 * a WR database then this method may throw an exception that is different
	 * from the listed exceptions.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1}.
	 * 
	 * @param  layoutFile The WR database layout file, not allowed to be {@code
	 *         null}.
	 * @param  fix The information whether an attempt should be made to fix any
	 *         detected integrity violations.
	 * @param  report The information whether a result report should be send to
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
	public final boolean run(Path layoutFile, boolean fix, boolean report) throws
									UnsupportedOperationException, NullPointerException,
									OverlappingFileLockException, CreationException {
		try (Database_ db = new Database_(layoutFile, -1, fix ? false : true, -1,
																								null)) {
			if (db.info().type() == DBType.RO) {
				throw new UnsupportedOperationException(ACDPException.prefix(db) +
																"Database is an RO database.");
			}
			// The database is a WR database. It is writable if and only if fix
			// is set to true.
			return verify(db, fix, report);
		}
	}
}
