/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import acdp.exceptions.ACDPException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.ShutdownException;
import acdp.internal.Column_;
import acdp.internal.Database_;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.Table_;
import acdp.internal.store.Bag;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.internal.types.RefType_;
import acdp.internal.types.Type_;
import acdp.misc.Utils;
import acdp.types.Type.Scheme;

/**
 * The FL compactor provides the {@code run} method which eliminates the gaps
 * of unused memory blocks within the FL file space of a WR store and truncates
 * the corresponding FL data file.
 * <p>
 * Since some or even all rows in use must be relocated, any references to
 * these rows within this or anothoer table of the database must be adjusted.
 * Those references are persisted in RT or A[RT] columns.
 * <p>
 * <em>Note that example references may be invalidated by compacting the FL
 * file space of a WR store.</em>
 *
 * @author Beat Hoermann
 */
final class FLCompactor {
	/**
	 * Adjusts the specified row index with respect to the specified sorted list
	 * of gap indices.
	 * <p>
	 * For instance, assume the following list of gap indices:
	 * <pre>
	 * 3 135 389 390 391 489 613 650 661 679</pre>
	 * and assume {@code rowIndex} having the values
	 * <pre>
	 * 2, 3, 5, 500, 679, 681, 700</pre>
	 * respectively.
	 * Then this method returns the values
	 * <pre>
	 * 2, 3, 4, 494, 670, 671, 690</pre>
	 * respectively.
	 * 
	 * @param  rowIndex The row index to be adjusted.
	 * @param  gaps The list of gap indices.
	 *         Add one to a gap index to get the index of the corresponding
	 *         row gap.
	 *         
	 * @return The adjusted row index.
	 */
	static final long adjustRowIndex(long rowIndex, long[] gaps) {
		final int i = Arrays.binarySearch(gaps, rowIndex);
		if (i >= 0)
			rowIndex -= i;
		else {
			rowIndex += (i + 1);
		}
		return rowIndex;
	}
	
	/**
	 * Compacts the FL file space.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1}.
	 * 
	 * @param  store The store of the target table, not allowed to be {@code
	 *         null}.
	 * @param  gaps The sorted indices of the gaps in the FL file space of the
	 *         target table, not allowed to be {@code null} and not allowed to
	 *         be an empty array.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void compactFLFileSpace(WRStore store, long[] gaps) throws
																					FileIOException {
		// Precondition: gaps.length > 0
		final FLFileSpace flFileSpace = store.flFileSpace;
		final FileIO file = store.flDataFile;
		file.open();
		try {
			// The first memory block to move must be moved to the first gap.
			file.position(flFileSpace.indexToPos(gaps[0]));
			// The variable k denotes an index of a memory block to move provided
			// that the memory block is in use (allocated memory block).
			// Allocated memory blocks with indices i such that 0 <= i < gaps[0]
			// must never be moved. Since gaps[0] is a gap the first candidate to
			// move is gaps[0] + 1.
			long k = gaps[0] + 1;
			for (int j = 1; j < gaps.length; j++) {
				// k <= gaps[j]
				if (k < gaps[j]) {
					// Memory block with index k is an allocated memory block. This
					// memory block must be moved. Are there following any more
					// memory blocks that must be moved?
					final long pos = flFileSpace.indexToPos(k);
					k++;
					while (k < gaps[j]) {
						k++;
					}
					// One or more memory blocks must be moved.
					file.copyTo(pos, flFileSpace.indexToPos(k) - pos, store.gb1);
					// k == gaps[j]
				}
				k++;
				// k > gaps[j]
			}
			// k > gaps[gaps.length - 1]
			final long pos = flFileSpace.indexToPos(k);
			long len = flFileSpace.size() - pos;
			if (len > 0) {
				// Move one or more memory blocks.
				file.copyTo(pos, len, store.gb1);
			}
			// Truncate FL data file.
			file.truncate(file.position());
			// Reset file space.
			flFileSpace.reset(file.position());
			// Should be a good time to force materialization.
			file.force(true);
		} finally {
			file.close();
		}
	}
	
	/**
	 * Keeps two flags that are set by an adjuster to indicate that it has
	 * changed the contents of the FL data file or of the VL data file.
	 * <p>
	 * Based on the value of theses flags, changes made to the contents of the
	 * FL data file or of the VL data file of a table referencing the target
	 * table are materialized or not.
	 * (Forcing changes to be written to a file is expensive even if the file
	 * was not changed.)
	 * 
	 * @author Beat Hoermann
	 */
	private static final class ForceFlags {
		boolean flDataFileForce = false;
		boolean vlDataFileForce = false;
	}
	
	/**
	 * The {@code RT} adjuster adjusts the references stored in an RT column.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1}.
	 *
	 * @author Beat Hoermann
	 */
	private static class RTAdjuster extends ColProcessor {
		/**
		 * The flags to signal the caller that the contents of the FL data file
		 * or of the VL data file has been changed.
		 */
		private final ForceFlags ff;
		/**
		 * The list of gap indices of the target store.
		 * Add one to a gap index to get the index of the corresponding row gap.
		 */
		private final long[] gaps;
		/**
		 * The FL column data as stored in the next row of the table.
		 */
		private final byte[] colData0;
		/**
		 * The byte buffer used for writing the changed FL column data back to
		 * the FL data file.
		 */
		private final ByteBuffer buf;
		/**
		 * The changed FL column data.
		 */
		private final byte[] colData;
		
		/**
		 * The constructor.
		 * 
		 * @param store The store of the table referencing the target table, not
		 *        allowed to be {@code null}.
		 * @param col The column storing the references that need to be adjusted,
		 *        not allowed to be {@code null}.
		 *        The column must be a column of the store's table and it must
		 *        be an RT column.
		 * @param gaps The sorted indices of the gaps in the FL file space of the
		 *        target table, not allowed to be {@code null}.
		 * @param ff The flags set by the adjuster to indicate that it has changed
		 *        the contents of the FL data file or of the VL data file.
		 */
		RTAdjuster(WRStore store, Column_<?> col, long[] gaps, ForceFlags ff) {
			super(store, col);
			this.ff = ff;
			this.gaps = gaps;
			colData0 = getBuf().array();
			buf = ByteBuffer.allocate(len);
			colData = buf.array();
		}

		@Override
		protected final void process(long bitmap0, long rowPos,
														long colPos) throws FileIOException {
			final long rowIndex0 = Utils.unsFromBytes(colData0, len);
			if (rowIndex0 > 0) {
				// Stored row index not null. Adjust row index.
				final long rowIndex = adjustRowIndex(rowIndex0, gaps);
			
				if (rowIndex != rowIndex0) {
					// Write new row index;
					Utils.unsToBytes(rowIndex, len, colData);
					buf.rewind();
					flDataFile.write(buf, colPos);
					ff.flDataFileForce = true;
				}
			}
		}
	}
	
	/**
	 * The {@code InAofRT} adjuster adjusts the references stored in an INROW
	 * A[RT] column.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1}.
	 *
	 * @author Beat Hoermann
	 */
	private static final class InAofRTAdjuster extends ColProcessor {
		/**
		 * The flags to signal the caller that the contents of the FL data file
		 * or of the VL data file has been changed.
		 */
		private final ForceFlags ff;
		/**
		 * The list of gap indices of the target store.
		 * Add one to a gap index to get the index of the corresponding row gap.
		 */
		private final long[] gaps;
		/**
		 * See {@link WRColInfo#nullBitMask}.
		 */
		private final long nullBitMask;
		/**
		 * See {@link WRColInfo#sizeLen}.
		 */
		private final int sizeLen;
		/**
		 * The number of bytes needed to store a reference.
		 */
		private final int rtLen;
		/**
		 * The byte buffer containing the FL column data.
		 * The very same byte buffer is used to write the changed FL column data
		 * back to the FL data file.
		 */
		private final ByteBuffer buf;
		/**
		 * The FL column data, used as input and as output.
		 */
		private final byte[] colData;

		/**
		 * The constructor.
		 * 
		 * @param store The store of the table referencing the target table, not
		 *        allowed to be {@code null}.
		 * @param col The column storing the array of references that need to be
		 *        adjusted, not allowed to be {@code null}.
		 *        The column must be a column of the store's table and it
		 *        must be an INROW A[RT] column.
		 * @param gaps The sorted indices of the gaps in the FL file space of the
		 *        target table, not allowed to be {@code null}.
		 * @param ff The flags set by the adjuster to indicate that it has changed
		 *        the contents of the FL data file or of the VL data file.
		 */
		InAofRTAdjuster(WRStore store, Column_<?> col, long[] gaps,
																					ForceFlags ff) {
			super(store, col);
			this.ff = ff;
			this.gaps = gaps;
			nullBitMask = ci.nullBitMask;
			sizeLen = ci.sizeLen;
			rtLen = ci.refdStore.nobsRowRef;
			buf = getBuf();
			colData = buf.array();
		}
		
		@Override
		protected final void process(long bitmap, long rowPos, long colPos) throws
																					FileIOException {
			if ((bitmap & nullBitMask) ==  0) {
				// Array value is not null.
				final int size = (int) Utils.unsFromBytes(colData, sizeLen);
				if (size > 0) {
					// Array has at least one element.
					boolean changed = false;
					int offset = sizeLen;
					for (int i = 0; i < size; i++) {
						final long rowIndex0 = Utils.unsFromBytes(colData, offset,
																								rtLen);
						if (rowIndex0 > 0) {
							// Stored row index not null. Adjust row index.
							final long rowIndex = adjustRowIndex(rowIndex0, gaps);
							
							if (rowIndex != rowIndex0) {
								changed = true;
								Utils.unsToBytes(rowIndex, rtLen, colData, offset);
							}
						}
						offset += rtLen;
					}
					if (changed) {
						// At least one reference was adjusted.
						buf.rewind();
						flDataFile.write(buf, colPos);
						ff.flDataFileForce = true;
					}
				}
			}
		}
	}
	
	/**
	 * The {@code OutAofRT} adjuster adjusts the references stored in an OUTROW
	 * A[RT] column.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1},
	 * {@linkplain WRStore.GlobalBuffer GB3}.
	 *
	 * @author Beat Hoermann
	 */
	private static final class OutAofRTAdjuster extends ColProcessor {
		/**
		 * The flags to signal the caller that the contents of the FL data file
		 * or of the VL data file has been changed.
		 */
		private final ForceFlags ff;
		/**
		 * The list of gap indices of the target store.
		 * Add one to a gap index to get the index of the corresponding row gap.
		 */
		private final long[] gaps;
		/**
		 * See {@link WRColInfo#sizeLen}.
		 */
		private final int sizeLen;
		/**
		 * See {@link WRColInfo#lengthLen}.
		 */
		private final int lengthLen;
		/**
		 * The number of bytes needed to store a reference.
		 */
		private final int rtLen;
		/**
		 * The FL column data as stored in the next row of the table.
		 */
		private final byte[] colData;
		/**
		 * The store's VL data file, never {@code null}.
		 */
		private final FileIO vlDataFile;
		/**
		 * The number of bytes required for referencing any outrow data from a
		 * row.
		 */
		private final int nobsOutrowPtr;

		/**
		 * The constructor.
		 * 
		 * @param store The store of the table referencing the target table, not
		 *        allowed to be {@code null}.
		 * @param col The column storing the array of references that need to be
		 *        adjusted, not allowed to be {@code null}.
		 *        The column must be a column of the store's table and it must
		 *        be an OUTROW A[RT] column.
		 * @param gaps The sorted indices of the gaps in the FL file space of the
		 *        target table, not allowed to be {@code null}.
		 * @param ff The flags set by the adjuster to indicate that it has changed
		 *        the contents of the FL data file or of the VL data file.
		 */
		OutAofRTAdjuster(WRStore store, Column_<?> col, long[] gaps,
																					ForceFlags ff) {
			super(store, col);
			this.ff = ff;
			this.gaps = gaps;
			sizeLen = ci.sizeLen;
			lengthLen = ci.lengthLen;
			rtLen = ci.refdStore.nobsRowRef;
			colData = getBuf().array();
			vlDataFile = store.vlDataFile;
			nobsOutrowPtr = store.nobsOutrowPtr;
		}
		
		/**
		 * A writer saving the written data to the VL data file without changing
		 * the file channel's position.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB3}.
		 * 
		 * 
		 * @author Beat Hoermann
		 */
		private final class Writer extends AbstractWriter {
			/**
			 * The current file position.
			 */
			private long pos;
			
			/**
			 * The constructor.
			 * <p>
			 * {@linkplain WRStore.GlobalBuffer GB3}.
			 * 
			 * @param pos The position where to start writing data to the VL data
			 *        file.
			 * @param length The total length of the outrow data used to set the
			 *        limit of the internal byte buffer.
			 */
			Writer(long pos, long length) {
				super(length, store.gb3);
				this.pos = pos;
			}
			
			@Override
			protected void save(ByteBuffer buf) throws FileIOException {
				vlDataFile.write(buf, pos);
				pos += buf.limit();
			}
		}

		@Override
		protected final void process(long bitmap, long rowPos, long colPos)
																		throws FileIOException {
			if (!Utils.isZero(colData, 0, lengthLen)) {
				// The array is not null and has at least one element.
				final long ptr = Utils.unsFromBytes(colData, lengthLen,
																					nobsOutrowPtr);
				final long length = Utils.unsFromBytes(colData, lengthLen);
				// Create the streamer to read the array from the VL data file.
				final IStreamer sr = new FileStreamer(vlDataFile, ptr, length,
																					store.gb1, true);
				final Bag bag = new Bag();
				// Get the size of the array. The size is greater than zero.
				sr.pull(sizeLen, bag);
				final int size = (int) Utils.unsFromBytes(bag.bytes, bag.offset,
																							sizeLen);
				// Create the writer.
				final Writer wr = new Writer(ptr + sizeLen, length);
				
				final byte[] bytes = new byte[rtLen];
				
				// Read and adjust the references and write them back.
				for (int i = 0; i < size; i++) {
					sr.pull(rtLen, bag);
					final long rowIndex0 = Utils.unsFromBytes(bag.bytes, bag.offset,
																								rtLen);
					if (rowIndex0 > 0) {
						// Stored row index not null. Adjust row index.
						final long rowIndex = adjustRowIndex(rowIndex0, gaps);
					
						if (rowIndex != rowIndex0) {
							Utils.unsToBytes(rowIndex, rtLen, bytes);
							wr.write(bytes, 0, rtLen);
							ff.vlDataFileForce = true;
						}
						else {
							wr.write(bag.bytes, bag.offset, rtLen);
						}
					}
				}
				
				wr.flush();
			}
		}
	}
	
	/**
	 * Creates a references adjuster.
	 * 
	 * @param  store The store of the table referencing the target table, not
	 *         allowed to be {@code null}.
	 * @param  col The column storing the references that need to be adjusted,
	 *         not allowed to be {@code null}.
	 *         The column must be a column of the store's table and it must be
	 *         an RT or an A[RT] column.
	 * @param  gaps The sorted indices of the gaps in the FL file space of the
	 *         target table, not allowed to be {@code null}.
	 * @param  ff The flags set by the adjuster to indicate that it has changed
	 *         the contents of the FL data file or of the VL data file.
	 *         
	 * @return The created references adjuster.
	 */
	private final ColProcessor createAdjuster(WRStore store, Column_<?> col,
																	long[] gaps, ForceFlags ff) {
		final Type_ type = col.type();
		if (type instanceof RefType_)
			return new RTAdjuster(store, col, gaps, ff);
		else {
			// type instanceof ArrayOfRefType_
			if (type.scheme() == Scheme.INROW)
				return new InAofRTAdjuster(store, col, gaps, ff);
			else {
				return new OutAofRTAdjuster(store, col, gaps, ff);
			}
		}
	}
	
	/**
	 * Adjusts the references stored in the specifed columns of the target table.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1},
	 * {@linkplain WRStore.GlobalBuffer GB3}.
	 * 
	 * @param  store The store of the table referencing the target table, not
	 *         allowed to be {@code null}.
	 * @param  cols The columns that reference the rows of the target table.
	 *         Each column must be a column of the store's table and it must be
	 *         an RT or an A[RT] column.
	 * @param  gaps The sorted indices of the gaps in the FL file space of the
	 *         target table, not allowed to be {@code null}.
	 * 
	 * @throws ImplementationRestrictionException If the number of row gaps in
	 *         the store's table is greater than {@code Integer.MAX_VALUE}.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void adjustReferences(WRStore store, List<Column_<?>> cols,
								long[] gaps) throws ImplementationRestrictionException,
																					FileIOException {
		final FileIO flDataFile = store.flDataFile;
		final FileIO vlDataFile = store.vlDataFile;
		flDataFile.open();
		try {
			vlDataFile.open();
			try {
				final ForceFlags ff = new ForceFlags();
				for (Column_<?> col : cols) {
					createAdjuster(store, col, gaps, ff).run();
				}
				if (ff.flDataFileForce) {
					flDataFile.force(false);
				}
				if (ff.vlDataFileForce) {
					vlDataFile.force(false);
				}
			} finally {
				vlDataFile.close();
			}
		} finally {
			flDataFile.close();
		}
	}
	
	/**
	 * Compacts the FL file space of the target table, assuming that the table
	 * is writable.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1},
	 * {@linkplain WRStore.GlobalBuffer GB3}.
	 * 
	 * @param  store The store of the target table, not allowed to be {@code
	 *         null}.
	 * 
	 * @throws ImplementationRestrictionException If the number of row gaps in
	 *         the store's table or in at least one of the tables referencing
	 *         that table is greater than {@code Integer.MAX_VALUE}.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void compact(WRStore store) throws
									ImplementationRestrictionException, FileIOException {
		
		// Compute the gaps in the FL file space.
		final long[] gaps = store.flFileSpace.gaps();
		if (gaps.length == 0) {
			// There are no gaps in the FL file space. Nothing to do.
			return;
		}
		// There is at least one gap.
		compactFLFileSpace(store, gaps);
		
		// Adjust FL file Space of tables referencing this table.
		for (Table_ table : (Table_[]) store.table.db().getTables()) {
			final WRStore refStore = (WRStore) table.store();
			final List<Column_<?>> refCols = new ArrayList<>();
			for (WRColInfo ci : refStore.colInfoArr) {
				if (ci.refdStore == store) {
					refCols.add(ci.col);
				}
			}
			
			if (!refCols.isEmpty()) {
				adjustReferences(refStore, refCols, gaps);
			}
		}
	}
	/**
	 * Eliminates the gaps of unused memory blocks within the FL file space of
	 * the specified WR store and truncates the corresponding FL data file.
	 * <p>
	 * Since this method opens an ACDP zone and executes its main task within
	 * that zone, invoking this method can be done during a session.
	 * <p>
	 * <em>Note that example references may be invalidated by compacting the FL
	 * file space of a WR store.</em>
	 * 
	 * @param  store The store, not allowed to be {@code null}.
	 * 
	 * @throws ImplementationRestrictionException If the number of row gaps in
	 *         the store's table or in at least one of the tables referencing
	 *         that table is greater than {@code Integer.MAX_VALUE}.
	 * @throws ACDPException If the database is read-only or if this method is
	 *         invoked within a read zone or a unit.
	 * @throws ShutdownException If the database is closed.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	final void run(WRStore store) throws ImplementationRestrictionException,
								ACDPException, ShutdownException, IOFailureException {
		final Database_ db = store.table.db();
		db.openACDPZone();
		// DB is writable
		try {
			compact(store);
		} catch (FileIOException e) {
			throw new IOFailureException(store.table, e);
		} finally {
			db.closeACDPZone();
		}
	}
}
