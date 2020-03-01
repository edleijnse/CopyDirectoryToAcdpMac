/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.util.ArrayList;
import java.util.List;

import acdp.exceptions.ACDPException;
import acdp.exceptions.DeleteConstraintException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.ShutdownException;
import acdp.internal.Column_;
import acdp.internal.Database_;
import acdp.internal.FileIOException;
import acdp.internal.store.wr.FLDataReader.FLData;
import acdp.internal.store.wr.FLDataReader.IFLDataReader;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.internal.types.Type_;
import acdp.misc.Utils;

/**
 * The truncator provides the {@code run} method which deletes all rows of the
 * table associated with a WR store and truncates the corresponding data files.
 *
 * @author Beat Hoermann
 */
final class Truncator {
	
	/**
	 * Iterates the store's table and checks the reference counters of each row.
	 * <p>
	 * This method throws a {@code DeleteConstraintException} if there exists
	 * at least one row in the table with a reference counter greater than zero,
	 * hence, at least one row in the table is referenced by a foreign row.
	 * <p>
	 * This method assumes the value of {@code store.nobsRefCount} to be greater
	 * than zero.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1}.
	 * 
	 * @param  store The store, not allowed to be {@code null}.
	 * 
	 * @throws DeleteConstraintException If there exits at least one row in the
	 *         table which is referenced by a foreign row.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void checkRefCounts(WRStore store) throws
												DeleteConstraintException, FileIOException {
		// Precondition: store.nobsRefCount > 0.
		final int nBM = store.nBM;
		final int nRC = store.nobsRefCount;

		final long nofBlocks = store.flFileSpace.nofBlocks();
		// Note that an empty column array results in a "whole FL data reader".
		IFLDataReader flDataReader = FLDataReader.createNextFLDataReader(
									new Column_<?>[0], store, 0, nofBlocks, store.gb1);
		for (long index = 0; index < nofBlocks; index++) {
			final FLData flData = flDataReader.readFLData(index);
			// flData == null <=> row gap
			if (flData != null) {
				final long rc = Utils.unsFromBytes(flData.bytes, nBM, nRC);
				if (rc > 0) {
					throw new ACDPException(store.table, "Row " + (index + 1) +
									" can't be deleted because it is referenced by " +
									rc + " foreign row(s). No changes were made.");
				}
			}
		}
	}
	
	/**
	 * Returns the column info objects of the RT or A[RT] column.
	 * 
	 * @param  store The store, not allowed to be {@code null}.
	 * 
	 * @return The column info objects of the columns referencing a table, never
	 *         {@code null} but may be empty.
	 */
	private final List<WRColInfo> getRefCIList(WRStore store) {
		final List<WRColInfo> refCIList = new ArrayList<>();
		for (WRColInfo ci : store.colInfoArr) {
			if (ci.refdStore != null) {
				refCIList.add(ci);
			}
		}
		return refCIList;
	}
	
	/**
	 * Iterates the table and decrements the reference(s) stored in the
	 * specified columns of each row.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1},
	 * {@linkplain WRStore.GlobalBuffer GB2}.
	 * 
	 * @param  store The store, not allowed to be {@code null}.
	 * @param  refCIList The list of column info objects of columns that are
	 *         RT or A[RT] columns, not allowed to be {@code null} and expected
	 *         to contain at least one element.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void decRCs(WRStore store, List<WRColInfo> refCIList) throws
																					FileIOException {
		// Preparations for the inner loop.
		final WRColInfo[] ciArr = refCIList.toArray(new WRColInfo[0]);
		final int n = ciArr.length;
		final Type_[] types = new Type_[n];
		for (int i = 0; i < n; i++) {
			types[i] = ciArr[i].col.type();
		}

		final long nofBlocks = store.flFileSpace.nofBlocks();
		// Note that an empty column array results in a" whole FL data reader".
		IFLDataReader flDataReader = FLDataReader.createNextFLDataReader(
									new Column_<?>[0], store, 0, nofBlocks, store.gb1);
		for (long index = 0; index < nofBlocks; index++) {
			final FLData flData = flDataReader.readFLData(index);
			// flData == null <=> row gap
			if (flData != null) {
				for (int i = 0; i < n; i++) {
					Delete.decRCs(flData.bytes, types[i], ciArr[i], store, null);
				}
			}	
		}
	}
	
	/**
	 * Deletes all rows of the table associated with the specified store and
	 * truncates the corresponding data files.
	 * <p>
	 * Note that if the table contains at least one row which is referenced by
	 * a foreign row then this method throws a {@code DeleteConstraintException}.
	 * No changes are made to the database in such a situation.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1},
	 * {@linkplain WRStore.GlobalBuffer GB2}.
	 * 
	 * @param  store The WR store, not allowed to be {@code null}.
	 * 
	 * @throws DeleteConstraintException If there exits at least one row in the
	 *         table which is referenced by a foreign row.
	 *         This exception never happens if none of the tables in the database
	 *         reference the table associated with the specified store.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void truncate(WRStore store) throws DeleteConstraintException,
																					FileIOException {
		if (store.numberOfRows() > 0) {
			if (store.nobsOutrowPtr > 0) {
				// The table is referenced by this or another table.
				checkRefCounts(store);
			};
			
			final List<WRColInfo> refCIList = getRefCIList(store);
			if (refCIList.size() > 0) {
				// The table has columns referencing this or another table.
				decRCs(store, refCIList);
			}
		}
		store.flFileSpace.clearAndTruncate();
		if (store.vlFileSpace != null) {
			store.vlFileSpace.clearAndTruncate();
		}
	}
	
	/**
	 * Deletes all rows of the table associated with the specified store and
	 * truncates the corresponding data files.
	 * <p>
	 * Note that if the table contains at least one row which is referenced by
	 * a foreign row then this method throws a {@code DeleteConstraintException}.
	 * No changes are made to the database in such a situation.
	 * <p>
	 * Since this method opens an ACDP zone and executes its main task within
	 * that zone, invoking this method can be done during a session.
	 * 
	 * @param  store The WR store, not allowed to be {@code null}.
	 * 
	 * @throws DeleteConstraintException If the table associated with the
	 *         specified store contains at least one row which is referenced by
	 *         a foreign row.
	 *         This exception never happens if none of the tables in the database
	 *         reference the table associated with the specified store.
	 * @throws ACDPException If the database is read-only or if this method is
	 *         invoked within a read zone or a unit.
	 * @throws ShutdownException If the database is closed.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	final void run(WRStore store) throws DeleteConstraintException,
								ACDPException, ShutdownException, IOFailureException {
		final Database_ db = store.table.db();
		db.openACDPZone();
		// DB is writable
		try {
			store.flDataFile.open();
			try {
				store.vlDataFile.open();
				try {
					truncate(store);
				} finally {
					store.vlDataFile.close();
				}
			} finally {
				store.flDataFile.close();
			}
		} catch (FileIOException e) {
			throw new IOFailureException(store.table, e);
		} finally {
			db.closeACDPZone();
		}
	}
}
