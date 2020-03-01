/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.tools;

import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;

import acdp.Database;
import acdp.Information.DatabaseInfo;
import acdp.Information.TableInfo;
import acdp.Information.WRStoreInfo;
import acdp.Information.DatabaseInfo.DBType;
import acdp.exceptions.CreationException;
import acdp.exceptions.IOFailureException;
import acdp.internal.misc.Utils_;
import acdp.misc.Utils;

/**
 * Writes to the {@linkplain System#out standard output stream} a few key
 * figures of the stores of the tables of a WR database.
 * Knowing these figures, a user can decide to do the following things:
 * 
 * <ul>
 *    <li>Compacting the {@linkplain acdp.Table#compactVL() VL} or
 *    {@linkplain acdp.Table#compactFL() FL} file space.</li>
 *    <li>Changing the values of the table-specific settings {@linkplain
 *    Refactor#nobsRowRef nobsRowRef}, {@linkplain Refactor#nobsOutrowPtr
 *    nobsOutrowPtr} and {@linkplain Refactor#nobsRowRef nobsRefCount}.</li>
 * </ul>
 *
 * @author Beat Hoermann
 */
public final class KeyFigures {
	
	/**
	 * Prints the key figures for the file spaces.
	 * 
	 * @param used The used amount of file space memory.
	 * @param unused The unused amount file space memory.
	 * @param label A text introducing the figures.
	 */
	private static final void printFSKeyFigures(long used, long unused,
																					String label) {
		final long total = used + unused;
		final int percent = total == 0 ? 0 : (int) (unused * 100 / total);
		System.out.println("   " + label + ":");
		System.out.println("      " + total + ", of which " + unused +
																" unused (" + percent + " %)");
	}
	
	/**
	 * Prints to the {@linkplain System#out standard output stream} a few key
	 * figures of the stores of the database's tables.
	 * 
	 * @param  di The database's info object, not allowed to be {@code null}.
	 * 
	 * @throws IOFailureException If an I/O error occurs.
	 *         This exception never happens if {@code refCount} is equal to
	 *         {@code false}.
	 */
	private static final void printKeyFigures(DatabaseInfo di) throws
																				IOFailureException {
		System.out.println("Key figures of database \"" + di.name() + "\"");
		
		final TableInfo[] tis = di.getTableInfos();
		
		System.out.println("   Number of tables: " + tis.length);
		
		for (int i = 0; i < tis.length; i++) {
			final TableInfo ti = tis[i];
			
			System.out.println();
			// Print table name.
			System.out.println("Table \"" + ti.name() + "\"");
			
			final WRStoreInfo si = (WRStoreInfo) ti.storeInfo();
			final int blockSize = si.blockSize();
			final long nofRows = si.flUsed() / blockSize;
			
			// Print key figures of VL file space.
			printFSKeyFigures(si.vlUsed(), si.vlUnused(), "VL file space [bytes]");
			// Print key figures of FL file space.
			printFSKeyFigures(nofRows, si.flUnused() / blockSize,
															"FL file space [memory blocks]");
			// Print value of nobsRowRef and related key figures.
			if (i == 0)
				System.out.println("   nobsRowRef/Number of rows/Smallest " +
										"nobsRowRef/Maximum number of memory blocks " +
										"with smallest nobsRow:");
			else {
				System.out.println("   nobsRowRef");
			}
			int smallest = Utils.lor(nofRows);
			System.out.println("      " + si.nobsRowRef() + "/" + nofRows + "/" +
										smallest + "/" + Utils_.bnd8[smallest]);
			
			final int nobsOutrowPtr = si.nobsOutrowPtr();
			
			if (nobsOutrowPtr > 0) {
				// Print value of nobsOutrowPtr and related key figures.
				if (i == 0)
					System.out.println("   nobsOutrowPtr/Size of VL data file in " +
										"bytes/Smallest nobsOutrowPtr/Maximum safe " +
										"size in bytes with smallest nobsOutrowPtr:");
				else {
					System.out.println("   nobsOutrowPtr");
				}
				final long size = si.sizeOfVlDataFile();
				smallest = Utils.lor(size + 1);
				final long max = Utils_.bnd8[smallest];
				System.out.println("      " + si.nobsOutrowPtr() + "/" + size +
										"/" + smallest + "/" + (smallest == 8 ? max :
										max + 1));
			}
			else {
				System.out.println("   Table has no outrow data.");
			}
			
			final int nobsRefCount = si.nobsRefCount();
			
			if (nobsRefCount > 0) {
				// Print value of nobsRefCount and related key figures.
				if (i == 0)
					System.out.println("   nobsRefCount/Highest value of " +
										"reference counter/Smallest nobsRefCount/" +
										"Maximum value of reference counter with " +
										"smallest nobsRefCount:");
				else {
					System.out.println("   nobsRefCount");
				}
				final long h = si.highestRefCount();
				smallest = Utils.lor(h);
				System.out.println("      " + nobsRefCount + "/" + h + "/" +
										smallest + "/" + Utils_.bnd8[smallest]);
			}
			else {
				System.out.println("   Table not referenced.");
			}
		}
	}
	
	/**
	 * Writes to the {@linkplain System#out standard output stream} a few key
	 * figures of the stores of the tables of the specified WR database.
	 * <p>
	 * Note that this method fails if the database is not a WR database or if
	 * the database is currently open.
	 * <p>
	 * Note also that if the given path denotes a valid file path but not of
	 * a valid layout file of a WR database then this method may throw an
	 * exception that is different from the listed exceptions.
	 * 
	 * @param  layoutFile The layout file of the WR database, not allowed to be
	 *         {@code null}.
	 * 
	 * @throws NullPointerException If {@code layoutFile} is {@code null}.
	 * @throws UnsupportedOperationException If the database is an RO database.
	 * @throws OverlappingFileLockException If the database is currently open.
	 * @throws CreationException If the database can't be opened due to any other
	 *         reason.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	public static final void run(Path layoutFile) throws
									NullPointerException, UnsupportedOperationException,
									OverlappingFileLockException, CreationException,
																				IOFailureException {
		try (Database db = Database.open(layoutFile, 0, true)) {
			final DatabaseInfo di = db.info();
			if (di.type() == DBType.RO) {
				throw new UnsupportedOperationException(
																"Database is an RO database.");
			}
			// The database is a read-only WR database.
			printKeyFigures(di);
		}
	}
	
	/**
	 * Prevent object construction.
	 */
	private KeyFigures() {
	}
}
