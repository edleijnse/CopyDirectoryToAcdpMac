/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.util.Objects;

import acdp.Column;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.ShutdownException;
import acdp.internal.Buffer;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.Row_;
import acdp.internal.store.Bag;
import acdp.internal.store.ReadOp;
import acdp.internal.store.wr.FLDataReader.ColOffset;
import acdp.internal.store.wr.FLDataReader.FLData;
import acdp.internal.store.wr.FLDataReader.IFLDataReader;
import acdp.misc.Utils;

/**
 * The read-only operation of a WR table.
 * <p>
 * A read-only operation of a writable WR table is either <em>synchronized</em>
 * or <em>unsynchronized</em>.
 * A read-only operation of a read-only WR table is always unsynchronized.
 * See the description of the {@code acdp.internal.core.SyncManager} class to
 * learn about "synchronized database operations".
 *
 * @author Beat Hoermann
 */
final class WRReadOp extends ReadOp {
	/**
	 * The table's store.
	 */
	private final WRStore store;
	
	/**
	 * Constructs the read-only operation for a WR table.
	 * 
	 * @param store The store this read-only operation belongs to, not allowed
	 *        to be {@code null}.
	 */
	WRReadOp(WRStore store) {
		super(store);
		this.store = store;
	}
	
	@Override
	protected final IRowLoader createRowLoader(Column<?>[] cols) throws
										NullPointerException, IllegalArgumentException {
		Objects.requireNonNull(cols, ACDPException.prefix(table) +
													"Column array not allowed to be null.");
		return new WRRowLoader(cols, FLDataReader.createRandomFLDataReader(cols,
																								store));
	}
	
	@Override
	protected final RowAdvancer createRowAdvancer(long start, long end,
																		Column<?>[] cols) throws
										NullPointerException, IllegalArgumentException {
		// Precondition: index >= 0.
		Objects.requireNonNull(cols, ACDPException.prefix(table) +
													"Column array not allowed to be null.");
		return new RowAdvancer(new WRRowLoader(cols,
										FLDataReader.createNextFLDataReader(cols, store,
													start, end, new Buffer())), start, end);
	}
	
	/**
	 * The row loader of a WR table.
	 * <p>
	 * Note that the {@code load} method may be invoked more than once.
	 * 
	 * @author Beat Hoermann
	 */
	private class WRRowLoader implements IRowLoader {
		/**
		 * The array of columns, never {@code null} but may be empty.
		 */
		private final Column<?>[] cols;
		/**
		 * The FL data reader to be used by the row loader, never {@code null}.
		 */
		private final IFLDataReader flDataReader;
		
		/**
		 * The constructor.
		 * 
		 * @param cols The array of columns, not allowed to be {@code null}.
		 *        The columns must be columns of {@code table}.
		 *        If the array of columns is empty then the row loader behaves as
		 *        if the value was identical to the table definition.
	    * @param flDataReader The FL data reader to be used by the row loader,
	    *        not allowed to be {@code null}.
		 */
		WRRowLoader(Column<?>[] cols, IFLDataReader flDataReader) {
			this.cols = cols;
			this.flDataReader = flDataReader;
		}
		
		/**
		 * Load the values based on the specified FL data.
		 * 
		 * @param  flData The FL data, not allowed to be {@code null}.
		 * 
		 * @return The array of values, never {@code null} and never an empty
		 *         array.
		 *         The order and length of the array is in strict accordance with
		 *         the array of columns of the {@link #cols} property or with the
		 *         table definition if {@code cols} is  an empty array.
		 * 
		 * @throws CryptoException If decrypting the byte array fails.
		 *         This exception never happens if the WR database does not apply
		 *         encryption.
		 * @throws FileIOException If an I/O error occurs while reading the VL
		 *         data file.
		 */
		private final Object[] loadValues(FLData flData) throws CryptoException,
																					FileIOException {
			final byte[] bytes = flData.bytes;
			final long bitmap = Utils.unsFromBytes(bytes, store.nBM);
			final Bag bag = new Bag(bytes);
			final ColOffset[] colOffsets = flData.colOffsets;
			final int n = colOffsets.length;
			
			final Object[] vals = new Object[n];
			for (int i = 0; i < n; i++) {
				final ColOffset co = colOffsets[i];
				bag.offset = co.offset;
				vals[i] = co.ci.b2o.convert(bitmap, bag);
			}
			return vals;
		}
		
		@Override
		public final Row_ load(long index) throws CryptoException,
													ShutdownException, IOFailureException {
			// Precondition: index >= 0
			try {
				store.flDataFile.open();
				final FLData flData;
				try {
					flData = flDataReader.readFLData(index);
				} finally {
					store.flDataFile.close();
				}
				if (flData == null)
					return null;
				else {
					final FileIO vlDataFile = store.vlDataFile;
					vlDataFile.open();
					try {
						return new Row_(table, cols, loadValues(flData));
					} finally {
						vlDataFile.close();
					}
				}
			} catch (FileIOException e) {
				throw new IOFailureException(table, e);
			}
		}
	}
}