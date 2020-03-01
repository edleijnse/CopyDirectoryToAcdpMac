/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.nio.ByteBuffer;
import java.util.Objects;

import acdp.exceptions.ACDPException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.internal.Column_;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.misc.Utils;

/**
 * Given a particular column of a particular table, the {@link #run} method
 * of the column processor reads the bitmap (if necessary) and the FL column
 * data of that column for each row of the table and invokes the {@link
 * #process} method.
 * <p>
 * By implementing the {@link #process} method, subclasses specify what they
 * want to do with the next FL data of the column.
 *
 * @author Beat Hoermann
 */
abstract class ColProcessor {
	/**
	 * The store, never {@code null}.
	 */
	protected final WRStore store;
	/**
	 * See {@linkplain WRStore#flDataFile}.
	 */
	protected final FileIO flDataFile;
	/**
	 * See {@linkplain WRStore#flFileSpace}.
	 */
	private final FLFileSpace flFileSpace;
	/**
	 * The column info object.
	 */
	protected final WRColInfo ci;
	/**
	 * See {@linkplain WRColInfo#offset}.
	 */
	private final int offset;
	/**
	 * See {@linkplain WRColInfo#len}.
	 */
	protected final int len;
	/**
	 * The byte buffer storing the bitmap.
	 */
	protected final ByteBuffer bmBuf;
	/**
	 * The byte array of {@code bmBuf}.
	 */
	protected final byte[] bmBufArr;
	/**
	 * The byte buffer storing the column data.
	 */
	private final ByteBuffer buf;
	
	/**
	 * The constructor.
	 * 
	 * @param  store The store, not allowed to be {@code null}.
	 * @param  col The column to be processed, not allowed to be {@code null}.
	 *         The column must be a column of the table.
	 * 
	 * @throws NullPointerException If one of the arguments is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If the column is not a column of the
	 *         table.
	 */
	ColProcessor(WRStore store, Column_<?> col) throws NullPointerException,
																		IllegalArgumentException {
		this.store = store;
		flDataFile = store.flDataFile;
		flFileSpace = store.flFileSpace;
		ci = store.colInfoMap.get(Objects.requireNonNull(col,
								ACDPException.prefix(store.table) + "Column is null."));
		if (ci == null) {
			throw new IllegalArgumentException(ACDPException.prefix(
														store.table) + "Column \"" + col +
														"\" is not a column of this table.");
		}
		offset = ci.offset;
		len = ci.len;
		bmBuf = ci.nullBitMask != 0 ? ByteBuffer.allocate(store.nBM) : null;
		bmBufArr = bmBuf != null ? bmBuf.array() : null;
		buf = ByteBuffer.allocate(len);
	}
	
	/**
	 * Returns the byte buffer containing the FL column data.
	 * <p>
	 * This is the gobal byte buffer used by the column processor whenever the
	 * FL column data is read from the next row.
	 * Subclasses may want to use this buffer either for changing the column
	 * data and directly writing the changed FL column data back to the FL data
	 * file or indirectly by referencing the FL column data via the {@code
	 * ByteBuffer.array()} method.
	 * 
	 * @return The byte buffer containing the columnd data, never {@code null}.
	 */
	protected final ByteBuffer getBuf() {
		return buf;
	}

	/**
	 * Processes the bitmap and the FL column data of the next row of the table.
	 * <p>
	 * Note that the FL column data is saved in the {@code getBuf().array()} byte
	 * array which is created during object construction and remains invariant
	 * for the lifetime of the column processor.
	 * (Only the values of the elements change from one invocation of this method
	 * to the next invocation but not the reference to the byte array itself.)
	 * <p>
	 * Depending on the implementation, this method may throw an exception that
	 * is different from the listed {@code FileIOException}.
	 * 
	 * @param  bitmap The stored bitmap of the row or 0 if the FL data consists
	 *         of the FL column data only.
	 * @param  rowPos The position within the FL data file where the row starts.
	 * @param  colPos The position within the FL data file where the column
	 *         starts.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	protected abstract void process(long bitmap, long rowPos, long colPos) throws
																					FileIOException;
	
	/**
	 * Reads the bitmap (if necessary) and the FL column data of the given
	 * column for the row with the specified index and invokes the {@link
	 * #process} method.
	 * <p>
	 * Depending on the implementation of the {@code process} method, this
	 * method may throw an exception that is different from the listed {@code
	 * FileIOException}.
	 * 
	 * @param  index The index of the FL memory block housing the row, must
	 *         be greater than or equal to zero and less than the number of
	 *         rows in the table.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void process(long index) throws FileIOException {
		final long rowPos = flFileSpace.indexToPos(index);
		final long colPos = rowPos + offset;
		long bitmap = 0;
		
		// Read bitmap.
		if (bmBuf != null) {
			bmBuf.rewind();
			flDataFile.read(bmBuf, rowPos);
			bitmap = Utils.unsFromBytes(bmBufArr, store.nBM);
		}
		
		// Read FL column data.
		buf.rewind();
		flDataFile.read(buf, colPos);
		
		process(bitmap, rowPos, colPos);
	}
	
	/**
	 * Given a particular column of a particular table, this method reads the
	 * bitmap (if necessary) and the FL column data of that column for each row
	 * of the table and invokes the {@link #process} method.
	 * <p>
	 * Depending on the implementation of the {@code process(long)} method, this
	 * method may throw an exception that is different from the listed
	 * exceptions.
	 * 
	 * @throws ImplementationRestrictionException If the number of row gaps is
	 *         greater than {@code Integer.MAX_VALUE}.
	 * @throws FileIOException If an I/O error occurs.
	 */
	final void run() throws ImplementationRestrictionException, FileIOException {
		final long nofBlocks = flFileSpace.nofBlocks();
		final long[] gaps = flFileSpace.gaps();
		final int nofGaps = gaps.length;
		long gap = nofGaps > 0 ? gaps[0] : -1;
		int gapsIndex = 1;
		
		for (long index = 0; index < nofBlocks; index++) {
			if (gap == -1 || index < gap)
				process(index);
			else if (gapsIndex < nofGaps)
				gap = gaps[gapsIndex++];
			else {
				gap = -1;
			}
		}
	}
}
