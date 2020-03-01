/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import acdp.exceptions.ACDPException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.ShutdownException;
import acdp.internal.Database_;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.store.wr.VLFSAreaUtility.FSArea;
import acdp.internal.store.wr.VLFSAreaUtility.FSAreaAdvancer;
import acdp.internal.store.wr.VLFSAreaUtility.FSAreaTreap;
import acdp.misc.Utils;

/**
 * The VL compactor provides the {@code run} method which eliminates the gaps
 * of unused memory areas within the VL file space of a WR store and truncates
 * the corresponding VL data file.
 * <p>
 * Since some or even all memory areas in use must be relocated, any pointer
 * from the FL file space pointing to a relocated memory area in the VL file
 * space must be adjusted.
 * Those pointers are persisted within columns having an outrow type or an
 * inrow array type with an outrow elment type.
 * We call these columns <em>VL columns</em>.
 *
 * @author Beat Hoermann
 */
final class VLCompactor {
	/**
	 * An {@link FSArea} with a {@link #delta} property.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class DeltaArea extends FSArea {
		/**
		 * The distance in bytes between this VL file space area and the VL file
		 * space area immediately before this VL file space area in a sorted list
		 * of VL file space areas.
		 */
		long delta;
		
		/**
		 * Constructs a VL file space area.
		 * 
		 * @param ptr The position within the VL data file where the VL file
		 *        space area starts, must be greater than or equal to zero.
		 * @param length The length of the VL file space area in bytes, must be
		 *        greater than or equal to zero.
		 */
		DeltaArea(long ptr, long length) {
			super(ptr, length);
		}
	}
	
	/**
	 * Compares two VL file space areas of length greater than zero that are
	 * either disjoint and unconnected or one is contained in the other.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class AreaComparator implements Comparator<FSArea> {
		
		@Override
		public final int compare(FSArea a1, FSArea a2) {
			if (a1.ptr + a1.length < a2.ptr)
				return -1;
			else if (a2.ptr + a2.length < a1.ptr)
				return 1;
			else {
				// One area is contained in the other.
				return 0;
			}
		}
	}
	
	/**
	 * Computes the list of used areas in the VL file space sorted according to
	 * the values of their {@code ptr} property.
	 * 
	 * @param  fsAreaAdvancer The VL file space area advancer.
	 * 
	 * @return The used areas in the VL file space sorted according to the
	 *         values of their {@code ptr} property, never {@code null} but may
	 *         be empty.
	 *         Each VL file space area in the list has a length greater than
	 *         zero.
	 * 
	 * @throws ACDPException If there is at least one VL file space area that
	 *         overlaps with an other VL file space area or if there is at least
	 *         one row containing a negative pointer to some outrow data or a
	 *         negative length of some outrow data.
	 *         This exception can happen only if the integrity of the table data
	 *         is violated.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final List<FSArea> computeFsAreas(
								FSAreaAdvancer fsAreaAdvancer) throws ACDPException,
																					FileIOException {
		final FSAreaTreap fsAreaTreap = new FSAreaTreap();
		
		FSArea[] fsAreas = fsAreaAdvancer.next();
		while (fsAreas != null) {
			for (FSArea fsArea : fsAreas) {
				if (fsArea.length > 0) {
					// Insert file space areas with a positive length only.
					fsAreaTreap.insert(new DeltaArea(fsArea.ptr, fsArea.length));
				}
			}
			fsAreas = fsAreaAdvancer.next();
		};
		
		return fsAreaTreap.computeFSAreaList();
	}
	
	/**
	 * Compacts the VL file space.
	 * As a side effect the {@code delta} property of the specified file space
	 * areas is set.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1}.
	 * 
	 * @param  store The WR store.
	 * @param  sortedFsAreas The used areas in the VL file space sorted
	 *         according to the values of their {@code ptr} property, not allowed
	 *         to be {@code null} but allowed to be empty.
	 *         Each VL file space area in the list must have a length greater
	 *         than zero.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void compactVLFileSpace(WRStore store,
									List<FSArea> sortedFsAreas) throws FileIOException {
		final FileIO file = store.vlDataFile;
		final Iterator<FSArea> fsAreasIt = sortedFsAreas.iterator();
		if (fsAreasIt.hasNext()) {
			FSArea fsArea = fsAreasIt.next();
			long ptr = fsArea.ptr;
			final long startPos = store.vlFileSpace.start;
			((DeltaArea) fsArea).delta = ptr - startPos;
			if (ptr == startPos)
				// Skip first file space area.
				file.position(startPos + fsArea.length);
			else {
				// Move first file space area.
				file.position(startPos);
				file.copyTo(ptr, fsArea.length, store.gb1);
			}
			// Copy other file space areas.
			while (fsAreasIt.hasNext()) {
				fsArea = fsAreasIt.next();
				ptr = fsArea.ptr;
				((DeltaArea) fsArea).delta = ptr - file.position();
				file.copyTo(ptr, fsArea.length, store.gb1);
			}
		}
		final long endPos = file.position();
		// Truncate VL data file.
		file.truncate(endPos);
		// Reset file space.
		store.vlFileSpace.reset(endPos);
		// Should be a good time to force materialization.
		file.force(true);
	}
	
	/**
	 * Adjust the pointers to the used VL file space areas in the FL data file.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1}.
	 * 
	 * @param  store The WR store.
	 * @param  fsAreaAdvancer The file space area advancer.
	 * @param  sortedFsAreas The used areas in the VL file space sorted
	 *         according to the values of their {@code ptr} property, not allowed
	 *         to be {@code null} and not allowed to be empty.
	 *         Each VL file space area in the list must have a length greater
	 *         than zero.
	 *         
	 * @throws ACDPException If there is at least one row containing a negative
	 *         pointer to some outrow data or a negative length of some outrow
	 *         data.
	 *         This exception can happen only if the integrity of the table data
	 *         is violated.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void adjustFlDataFile(WRStore store,
					FSAreaAdvancer fsAreaAdvancer, List<FSArea> sortedFsAreas) throws
																ACDPException, FileIOException {
		fsAreaAdvancer.reset();
		final int nobsOutrowPtr = store.nobsOutrowPtr;
		final ByteBuffer buf = ByteBuffer.allocate(nobsOutrowPtr);
		final byte[] arr = buf.array();
		final FileIO file = store.flDataFile;
		FSArea[] fsAreas = fsAreaAdvancer.next();
		while (fsAreas != null) {
			for (int i = 0; i < fsAreas.length; i++) {
				final FSArea fsArea = fsAreas[i];
				if (fsArea.length > 0) {
					// Find out if we must adjust the pointer of fsArea.
					final long delta = ((DeltaArea) sortedFsAreas.get(
										Collections.binarySearch(sortedFsAreas, fsArea,
																new AreaComparator()))).delta;
					if (delta > 0) {
						Utils.unsToBytes(fsArea.ptr - delta, nobsOutrowPtr, arr);
						buf.rewind();
						file.write(buf, fsAreaAdvancer.filePos(i));
					}
				}
			}
			fsAreas = fsAreaAdvancer.next();
		}
		// Should be a good time to force materialization.
		file.force(true);
	}
	
	/**
	 * Compacts the VL file space, assuming that the table is writable and can
	 * save outrow data.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1}.
	 * 
	 * @param  store The WR store.
	 * 
	 * @throws ACDPException If there is at least one VL file space area that
	 *         overlaps with an other VL file space area or if there is at least
	 *         one row containing a negative pointer to some outrow data or a
	 *         negative length of some outrow data.
	 *         This exception can happen only if the integrity of the table data
	 *         is violated.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void compact(WRStore store) throws ACDPException,
																					FileIOException {
		// store.vlFileSpace != null since DB is writable.
		if (store.vlFileSpace.deallocated() == 0)
			// Nothing to do!
			return;
		else {
			// The size of the deallocated memory space is greater than zero. Note
			// that the size of the allocated memory space may be zero.
			final FSAreaAdvancer fsAreaAdvancer = new FSAreaAdvancer(store);
			
			// Compute the used VL file space areas.
			final List<FSArea> sortedFsAreas = computeFsAreas(fsAreaAdvancer);
			
			// Compact VL file space and set delta-property of areas.
			compactVLFileSpace(store, sortedFsAreas);
			
			if (sortedFsAreas.size() > 0) {
				// Adjust outrow pointers in FL data file by reusing the file
				// space area advancer.
				adjustFlDataFile(store, fsAreaAdvancer, sortedFsAreas);
			}
		}
	}
	
	/**
	 * Eliminates the gaps of unused memory blocks within the VL file space of
	 * the specified WR store and truncates the corresponding VL data file.
	 * <p>
	 * Since this method opens an ACDP zone and executes its main task within
	 * that zone, invoking this method can be done during a session.
	 * <p>
	 * This method has no effect if the store has no VL file space.
	 * 
	 * @param  store The WR store, not allowed to be {@code null}.
	 * 
	 * @throws ACDPException If the database is read-only or if this method is
	 *         invoked within a read zone or a unit.
	 * @throws ShutdownException If the database is closed.
	 * @throws IOFailureException If an I/O error occurs.
	 */
	final void run(WRStore store) throws ACDPException, ShutdownException,
																				IOFailureException {
		if (store.nobsOutrowPtr > 0) {
			// Table has outrow data.
			final Database_ db = store.table.db();
			db.openACDPZone();
			// DB is writable
			try {
				store.flDataFile.open();
				try {
					store.vlDataFile.open();
					try {
						compact(store);
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
}
