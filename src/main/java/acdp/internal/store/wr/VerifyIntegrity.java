/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import acdp.Column;
import acdp.ColVal;
import acdp.Row;
import acdp.exceptions.ACDPException;
import acdp.exceptions.IllegalReferenceException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.internal.ColVal_;
import acdp.internal.Column_;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.Ref_;
import acdp.internal.Verifyer.Reporter;
import acdp.internal.misc.Utils_;
import acdp.internal.store.wr.VLFSAreaUtility.FSArea;
import acdp.internal.store.wr.VLFSAreaUtility.FSAreaAdvancer;
import acdp.internal.store.wr.VLFSAreaUtility.FSAreaTreap;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.misc.Utils;

/**
 * Provides the {@link #run} method which executes a series of tests to find
 * out whether the integrity of the table data of a particular table is harmed
 * or not.
 * In case there are one or more integrity violations an optional attempt to
 * fix the violations is made, although not all types of integrity violations
 * can be automatically fixed.
 * <p>
 * Ensuring the integrity of the table data should guarantee general fault-free
 * operation of ACDP within the scope of the documented situations that cause
 * exceptions to be raised.
 *
 * @author Beat Hoermann
 */
final class VerifyIntegrity {
	/**
	 * The constructor.
	 */
	VerifyIntegrity() {
	}
	
	/**
	 * The base class of all tests testing a speficic aspect of the integrity
	 * of the table data.
	 * <p>
	 * A concrete test must implement the {@link #test} method and may override
	 * the {@link #fix} method.
	 * <p>
	 * A test is aborted if and only if the {@code test} method or the {@code
	 * fix} method throws an exception.
	 * <p>
	 * To run the test invoke the {@link #run} method.
	 * 
	 * @author Beat Hoermann
	 */
	private static abstract class Test {
		/**
		 * The number of the test.
		 * <p>
		 * The test number is used to make it clear which test was aborted in
		 * case the test is aborted.
		 */
		private final int number;
		/**
		 * Indicates if this test makes an attempt to fix a detected integrity
		 * violation.
		 */
		private final boolean hasFix;
		
		/**
		 * The constructor.
		 * Subclasses should override the {@link #fix} method if they invoke
		 * this constructor with the {@code hasFix} parameter set to {@code true}.
		 * 
		 * @param number The test number.
		 * @param hasFix The information whether this test makes an attempt to
		 *        fix a detected integrity violation.
		 */
		Test(int number, boolean hasFix) {
			this.number = number;
			this.hasFix = hasFix;
		}
		
		/**
		 * The actual content of the test to be executed.
		 * <p>
		 * This method may throw an exception that is different from the listed
		 * {@code FileIOException}.
		 * 
		 * @param  store The store housing the table data to be tested.
		 * 
		 * @return The message to be reported in case the test has detected an
		 *         integrity violation.
		 *         The value must be {@code null} if and only if the test has not
		 *         detected an integrity violation.
		 *
		 * @throws FileIOException If an I/O error occurs.
		 */
		protected abstract String test(WRStore store) throws FileIOException;
		
		/**
		 * Fixes a detected violation of the integrity of the table data.
		 * This method is called if and only if the following three conditions
		 * are satisfied:
		 * 
		 * <ol>
		 *    <li>This test was constructed with the {@code hasFix} argument set
		 *        to {@code true}.</li>
		 *    <li>The {@code fix} argument of the {@code run} method is equal to
		 *        {@code true}.</li>
		 *    <li>The {@code test} method returns a message different from {@code
		 *        null}.</li>
		 * </ol>
		 * <p>
		 * This implementation does nothing.
		 * Subclasses should override this method if they invoke the constructor
		 * with the {@code hasFix} parameter set to {@code true}.
		 * <p>
		 * This method may throw an exception that is different from the listed
		 * {@code FileIOException}.
		 * 
		 * @param  store The store housing the table data to be tested.
		 * 
		 * @throws FileIOException If an I/O error occurs.
		 */
		protected void fix(WRStore store) throws FileIOException {
		}
		
		/**
		 * Runs this test on the specified store by invoking the {@link #test}
		 * method and optionally by invoking the {@link #fix} method.
		 * <p>
		 * If it turns out that there is an integrity violation then this method
		 * makes an attempt to fix it, provided that the following two conditions
		 * are satisfied:
		 * 
		 * <ol>
		 *    <li>This test was constructed with the {@code hasFix} argument set
		 *        to {@code true}.</li>
		 *    <li>The {@code fix} argument is set to {@code true}.</li>
		 * </ol>
		 * <p>
		 * Note that this method never throws an exception.
		 * Instead this method signals an abort if and only if the {@code test}
		 * method or the {@code fix} method throws an exception of any kind.
		 * 
		 * @param store The store housing the table data to be verified, not
		 *        allowed to be {@code null}.
		 * @param fix The information whether an attempt should be made to fix
		 *        a detected integrity violation.
		 * @param rp The reporter, not allowed to be {@code null}.
		 * 
		 * @return The boolean value {@code true} if and only if this test was
		 *         not aborted and no integrity violation was detected or a
		 *         detected integrity violation could be fixed.
		 */
		final boolean run(WRStore store, boolean fix, Reporter rp) {
			boolean good = true;
			try {
				String msg = test(store);
				if (msg != null) {
					rp.violation(number, msg);
					good = false;
					if (fix && hasFix) {
						rp.fixing();
						fix(store);
						rp.fixed();
						good = true;
					}
				}
			} catch (Exception e) {
				rp.aborted(number, e);
				good = false;
			}
			return good;
		}
	}
	
	/**
	 * Tests if the number of rows exceeds the maximum number of rows.
	 * <p>
	 * The maximum number of rows depends on the value of the {@link
	 * WRStore#nobsRowRef nobsRowRef} parameter.
	 * <p>
	 * There is no automatic fix if the number of rows exceeds the maximum
	 * number of rows.
	 * However, you can try to {@linkplain acdp.tools.Refactor#nobsRowRef
	 * increase} the value of the {@code nobsRowRef} parameter.
	 *   
	 * @author Beat Hoermann
	 */
	private static final class Test1 extends Test {

		Test1() {
			super(1, false);
		}

		@Override
		protected final String test(WRStore store) {
			// The successful opening of the database guarantees that
			// nobsRowRef >= 1 and <= 8.
			String s = null;
			if (store.flFileSpace.nofBlocks() > Utils_.bnd8[store.nobsRowRef]) {
				s = "Number of rows exceeds the maximum number of rows.";
				if (store.nobsRowRef < 8) {
					s += " You can try to increase the value of the " +
									"\"nobsRowRef\" parameter by refactoring the table.";
				}
			}
			return s;
		}
	}
	
	/**
	 * Tests if the set of gaps in the FL file space computed via the chain of
	 * gaps is identical with the set of gaps computed by looking at the first
	 * bit of each memory block.
	 * <p>
	 * Provides an automatic fix that consists of rebuilding the chain of gaps.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class Test2 extends Test {

		Test2() {
			super(2, true);
		}

		@Override
		protected final String test(WRStore store) throws
									ImplementationRestrictionException, FileIOException {
			final FLFileSpace flFileSpace = store.flFileSpace;
			
			// Compute the list of gaps from chain of gaps.
			final long[] gaps;
			try {
				gaps = flFileSpace.gaps();
			} catch (Exception e) {
				if (e instanceof ImplementationRestrictionException)
					throw e;
				else {
					return "The chain of gaps of the FL file space seems to be " +
																						"corrupted.";
				}
			}
			// gaps != null
			
			// Loop over all memory blocks, recognize memory blocks being gaps and
			// check if they match with the memory blocks contained in the array
			// of gaps.
			
			// File position of current memory block.
			long pos = flFileSpace.indexToPos(0);
			// Ending file position.
			final long endPos = flFileSpace.size();
			// Increment file position by the length of the memory blocks.
			final long n = store.n;
			// Index of current memory block.
			long i = 0;
			// Index of current gap in array of gaps.
			int j = 0;
			
			final ByteBuffer buf = ByteBuffer.allocate(1);
			final byte[] bytes = buf.array();
			
			final int gapsLen = gaps.length;
			String s = null;
			
			final FileIO flDataFile = store.flDataFile;
			flDataFile.open();
			try {
				while (pos < endPos && s == null) {
					buf.rewind();
					flDataFile.read(buf, pos);
					if (bytes[0] < 0) {
						// gap!
						if (!(j < gapsLen && i == gaps[j])) {
							// Gap not contained in chain of gaps or there is a
							// memory block in the chain of gaps which is not a gap.
							s = "The memory blocks contained in the chain of gaps " +
										"of the FL file space do not match the memory " +
																		"blocks marked as gaps.";
						}
						j++;
					}
					i++;
					pos += n;
				}
			} finally {
				flDataFile.close();
			}
			
			return s;
		}
		
		@Override
		protected final void fix(WRStore store) throws FileIOException {
			store.flDataFile.open();
			try {
				store.flFileSpace.rebuildChainOfGaps();
			} finally {
				store.flDataFile.close();
			}
		}
	}
	
	/**
	 * Tests if there exist any overlapping VL file space areas and if there
	 * exist any illegal pointers to some outrow data.
	 * <p>
	 * There is no automatic fix.
	 *   
	 * @author Beat Hoermann
	 */
	private static final class Test3 extends Test {
		/**
		 * The amount of used VL memory space in bytes.
		 */
		private long used;

		Test3() {
			super(3, false);
			used = 0;
		}

		@Override
		protected final String test(WRStore store) throws ACDPException,
																					FileIOException {
			// The successful opening of the database guarantees that if table has
			// at least one VL column then nobsOutrowPtr is >= 1 and <= 8. If
			// table has no VL column then nobsOutrowPtr == 0.
			String s = null;
			if (store.nobsOutrowPtr > 0) {
				final FSAreaTreap treap = new FSAreaTreap();
				store.flDataFile.open();
				try {
					// Insert the VL file space areas into the treap. As a side
					// effect compute the total length of used outrow data.
					final FSAreaAdvancer advancer = new FSAreaAdvancer(store);
					// Note that advancer.next() throws an ACDPException if it finds
					// a negative value of the pointer or the length.
					FSArea[] fsAreas = advancer.next();
					while (fsAreas != null) {
						for (FSArea fsArea : fsAreas) {
							final long length = fsArea.length;
							if (length > 0) {
								// Insert file space areas with a positive length
								// only. Note that the insert method throws an
								// ACDPException if the VL file space area overlaps
								// with a previously inserted VL file space!
								treap.insert(new FSArea(fsArea.ptr, length));
								used += length;
							}
						}
						fsAreas = advancer.next();
					};
				} catch (ACDPException e) {
					s = "Overlapping VL file space areas.";
				} finally {
					store.flDataFile.close();
				}
				if (s != null && !treap.isEmpty()) {
					long size = 0;
					try {
						size = Files.size(store.vlDataFile.path);
					} catch (IOException e) {
						throw new FileIOException(store.vlDataFile.path, e);
					}
					if (treap.getLeftMost().ptr < store.vlFileSpace.start ||
													size - 1 <= treap.getRightMost().ptr) {
						s = "At least one pointer to outrow data is illegal.";
					}
				}
			}
			
			return s;
		}
		
		/**
		 * Returns the amount of used VL file space in bytes.
		 * 
		 * @return The amount of used VL file space in bytes.
		 */
		final long used() {
			return used;
		}
	}
	
	/**
	 * Tests if the amount of used VL file space is equal to the amount of
	 * allocated VL file space.
	 * <p>
	 * Provides an automatic fix that consists of updating the state of the
	 * VL file space manager to the correct number of bytes of allocated VL file
	 * space.
	 *   
	 * @author Beat Hoermann
	 */
	private static final class Test4 extends Test {
		/**
		 * The information whether test #3 has successfully terminated.
		 * See the description of the return value of the {@link Test#run run}
		 * method to learn about what is meant by "successfully terminated".
		 */
		private final boolean good3;
		/**
		 * The amount of used VL file space in bytes.
		 */
		private final long used;
		
		Test4(boolean good3, long used) {
			super(4, false);
			this.good3 = good3;
			this.used = used;
		}

		@Override
		protected final String test(WRStore store) {
			String msg = null;
			if (store.nobsOutrowPtr > 0) {
				if (!good3) {
					throw new ACDPException("Third test did not terminate " +
																					"successfully.");
				}

				if (used != store.vlFileSpace.allocated()) {
					msg = "Used VL file space (" + used + " bytes) doesn't match " +
													"value of allocated VL file space (" +
													store.vlFileSpace.allocated() + ").";
				}
			}
			return msg;
		}
		
		@Override
		protected final void fix(WRStore store) throws FileIOException {
			store.vlDataFile.open();
			try {
				store.vlFileSpace.correctM(used);
			} finally {
				store.vlDataFile.close();
			}
		}
	}
	
	/**
	 * Tests if the references persisted in the table are {@linkplain
	 * IllegalReferenceException illegal}.
	 * <p>
	 * Provides an automatic fix that consists of updating all illegal references
	 * to {@code null}.
	 * <p>
	 * Note that this test requires the second and third test to have
	 * successfully terminated.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class Test5 extends Test {
		/**
		 * Houses a store and provides access to the store's array of gaps via
		 * a cache.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class StoreInfo {
			final WRStore store;
			private final Map<WRStore, long[]> gapsCache;
			private long[] gaps;
			
			StoreInfo(WRStore store, Map<WRStore, long[]> gapsCache) {
				this.store = store;
				this.gapsCache = gapsCache;
				gaps = gapsCache.get(store);
			}
			
			/**
			 * Returns the store's array of gaps.
			 * 
			 * @return The array of gaps, never {@code null}.
			 *
			 * @throws ImplementationRestrictionException If the number of gaps in
			 *         the store's file space is greater than {@code
			 *         Integer.MAX_VALUE}.
			 * @throws FileIOException If an I/O error occurs.
			 */
			final long[] gaps() throws ImplementationRestrictionException,
																					FileIOException {
				if (gaps == null) {
					gaps = store.flFileSpace.gaps();
					gapsCache.put(store, gaps);
				}
				return gaps;
			}
		}
		
		/**
		 * Houses a row reference and a column value.
		 * 
		 * @author Beat Hoermann
		 */
		private final class FixInfo {
			final Ref_ ref;
			final ColVal<?> colVal;
			
			FixInfo(Ref_ ref, ColVal<?> colVal) {
				this.ref = ref;
				this.colVal = colVal;
			}
		}

		/**
		 * The information whether test #2 and #3 have successfully terminated.
		 * See the description of the return value of the {@link Test#run run}
		 * method to learn about what is meant by "successfully terminated".
		 */
		private final boolean good23;
		/**
		 * The information whether an attempt should be made to fix a detected
		 * integrity violation.
		 */
		private final boolean fix;
		/**
		 * The cache for the gaps arrays of the stores.
		 * <p>
		 * The purpose of this cache is to prevent the costly computation of the
		 * gaps from being recomputed each time they are used.
		 */
		private final Map<WRStore, long[]> gapsCache;
		/**
		 * The list of fix info items or {@code null} if fixing is not required.
		 */
		private final List<FixInfo> fiList;
		
		Test5(boolean good23, boolean fix, Map<WRStore, long[]> gapsCache) {
			super(5, true);
			this.good23 = good23;
			this.fix = fix;
			this.gapsCache = gapsCache;
			fiList = fix ? new ArrayList<FixInfo>() : null;
		}
		
		/**
		 * Tests if the specified reference is {@linkplain
		 * IllegalReferenceException illegal}.
		 * 
		 * @param  ref The reference to test, not allowed to be {@code null}.
		 * @param  si The info of the store this reference points to.
		 *
		 * @return The boolean value {@code true} if and only if the reference
		 *         is illegal.
		 * 
		 * @throws ImplementationRestrictionException If the number of gaps in the
		 *         store's file space is greater than {@code Integer.MAX_VALUE}.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final boolean isIllegal(Ref_ ref, StoreInfo si) throws
									ImplementationRestrictionException, FileIOException {
			long ri;
			try {
				ri = si.store.refToRi(ref);
			} catch (IllegalReferenceException e) {
				// Ref is out of range.
				ri = 0;
			}
			return ri == 0 || Arrays.binarySearch(si.gaps(), ri - 1) >= 0;
		}

		@Override
		protected final String test(WRStore store) throws
									ImplementationRestrictionException, FileIOException {
			if (!good23) {
				throw new ImplementationRestrictionException("Second or third " +
													"test did not terminate successfully.");
			}
			
			// Compute list of reference columns and list of store infos.
			final List<Column_<?>> refCols = new ArrayList<>();
			final List<StoreInfo> siList = new ArrayList<>();
			for (WRColInfo ci : store.colInfoArr) {
				final WRStore refdStore = ci.refdStore;
				if (refdStore != null) {
					refCols.add(ci.col);
					siList.add(new StoreInfo(refdStore, gapsCache));
				}
			}
			
			if (refCols.size() == 0) {
				// Table has no reference column.
				return null;
			}
			// Table has at least one reference column.
			
			// Visit all reference values and test whether they are legal or not.
			
			// Count number of illegal references.
			int count = 0;
			
			final Column_<?>[] refColArr = refCols.toArray(
																new Column_<?>[refCols.size()]);
			// Loop over rows of the table.
			for (Iterator<Row> it = store.iterator(refColArr); it.hasNext(); ) {
				final Row row = it.next();
				
				// Loop over reference columns.
				for (int j = 0; j < refColArr.length; j++) {
					final Object val = row.get(j);
					
					if (val != null) {
						final StoreInfo si = siList.get(j);
						if (val.getClass().isArray()) {
							// Column is ArrayOfRefType
							final Ref_[] refArr = (Ref_[]) val;
							boolean found = false;
							for (int k = 0; k < refArr.length; k++) {
								if (isIllegal(refArr[k], si)) {
									count++;
									if (fix) {
										// Fix reference by setting it to null.
										refArr[k] = null;
										found = true;
									}
								}
							}
							if (found) {
								// fix && at least one reference is illegal.
								@SuppressWarnings("unchecked")
								final Column_<Ref_[]> refCol =
																(Column_<Ref_[]>) refColArr[j];
								fiList.add(new FixInfo((Ref_) row.getRef(),
													(ColVal_<Ref_[]>) refCol.value(refArr)));
							}
						}
						else {
							// Column is RefType
							if (isIllegal((Ref_) val, si)) {
								count++;
								if (fix) {
									// Fix reference by setting it to null.
									fiList.add(new FixInfo((Ref_) row.getRef(),
													(ColVal_<?>) refColArr[j].value(null)));
								}
							}
						}
					}
				}
			}
			
			if (count == 0)
				return null;
			else if (count == 1)
				return "1 illegal reference detected.";
			else {
				return count + " illegal references detected.";
			}
		}
		
		@Override
		protected final void fix(WRStore store) throws FileIOException {
			for (FixInfo fi : fiList) {
				store.update(fi.ref, new ColVal[] { fi.colVal });
			}
		}
	}
	
	/**
	 * Tests if the reference counters have wrong values.
	 * <p>
	 * Provides an automatic fix that consists of updating the wrong reference
	 * counters to the correct value.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class Test6 extends Test {
		/**
		 * Houses a mutable counter.
		 * 
		 * @author Beat Hoermann
		 */
		private final class RC {
			long count;
			
			RC() {
				count = 1;
			}
		}
		
		/**
		 * Houses the file position of a row along with the correct value for
		 * the row's reference counter.
		 * 
		 * @author Beat Hoermann
		 */
		private final class FixInfo {
			final long pos;
			final long rc;
			
			FixInfo(long pos, long rc) {
				this.pos = pos;
				this.rc = rc;
			}
		}
		
		/**
		 * The information whether an attempt should be made to fix a detected
		 * integrity violation.
		 */
		private final boolean fix;
		/**
		 * The stores of the database.
		 */
		private final Set<WRStore> stores;
		/**
		 * The list of fix info items or {@code null} if fixing is not required.
		 */
		private final List<FixInfo> fiList;
		/**
		 * The maximum value over all correct reference counters.
		 */
		private long maxRC;
		
		Test6(boolean fix, Set<WRStore> stores) {
			super(6, true);
			this.fix = fix;
			this.stores = stores;
			fiList = fix ? new ArrayList<FixInfo>() : null;
			maxRC = 0;
		}
		
		/**
		 * Updates the specified map with respect to the specified reference.
		 * 
		 * @param ref The reference.
		 * @param rcMap The map.
		 */
		private final void account(Ref_ ref, Map<Long, RC> rcMap) {
			final Long ri = ref.rowIndex();
			final RC rc = rcMap.get(ri);
			if (rc == null)
				rcMap.put(ri, new RC());
			else {
				rc.count++;
			}
		}

		@Override
		protected final String test(WRStore store) throws FileIOException {
			// The successful opening of the database guarantees that if the store
			// is referenced then nobsRefCount >= 1 and <= 8. If the store is not
			// referenced then nobsRefcount == 0.
			if (store.nobsRefCount == 0) {
				return null;
			}
			// The store is referenced.
			
			// Compute the stores referencing store.
			final Set<WRStore> refdByStores = store.refdBy(stores);
			// !refdByCols.isEmpty()
			
			// Create the map that maps the row index to a reference counter.
			final Map<Long, RC> rcMap = new HashMap<>();
			
			// Loop over stores referencing store.
			for (WRStore refdByStore : refdByStores) {
				
				// Compute the columns referencing store.
				final List<Column<?>> refdByCols = new ArrayList<>();
				for (WRColInfo ci : refdByStore.colInfoArr) {
					if (ci.refdStore == store) {
						refdByCols.add(ci.col);
					}
				}
				// !refdByCols.isEmpty()
				
				// Loop over rows of table referencing store.
				for (Iterator<Row> it = refdByStore.iterator(refdByCols.
						toArray(new Column_<?>[refdByCols.size()])); it.hasNext(); ) {
					final Row row = it.next();
					// Loop over reference columns.
					for (Object val : row) {
						if (val != null) {
							if (val.getClass().isArray()) {
								// Column is ArrayOfRefType
								for (Ref_ ref : (Ref_[]) val) {
									if (ref != null) {
										account(ref, rcMap);
									}
								}
							}
							else {
								// Column is RefType
								account((Ref_) val, rcMap);
							}
						}
					}
				}
			}
			
			// Test stored reference counters.
			
			final int nBM = store.nBM;
			final int nobsRefCount = store.nobsRefCount;
			final FLFileSpace flFileSpace = store.flFileSpace;
			
			// File position of current memory block.
			long pos = flFileSpace.indexToPos(0);
			// Ending file position.
			final long endPos = flFileSpace.size();
			// Increment file position by the length of the memory blocks.
			final long n = store.n;
			// Current row index.
			long ri = 1;
			
			final ByteBuffer buf = ByteBuffer.allocate(nBM + nobsRefCount);
			final byte[] bytes = buf.array();
			
			// Count number of wrong rc values.
			int wrongRCs = 0;
			
			final FileIO flDataFile = store.flDataFile;
			flDataFile.open();
			try {
				while (pos < endPos) {
					buf.rewind();
					flDataFile.read(buf, pos);
					if (bytes[0] >= 0) {
						// No gap!
						final long rc = Utils.unsFromBytes(bytes, nBM, nobsRefCount);
						final RC rcObj = rcMap.get(ri);
						final long realRC = rcObj == null ? 0 : rcObj.count;
						if (rc != realRC) {
							// Wrong rc!
							wrongRCs++;
							if (fix) {
								// Fix reference counter by updating it to realRC.
								fiList.add(new FixInfo(pos, realRC));
								if (realRC > maxRC) {
									maxRC = realRC;
								}
							}
						}
					}
					ri++;
					pos += n;
				}
			}
			finally {
				flDataFile.close();
			}
			
			if (wrongRCs == 0)
				return null;
			else if (wrongRCs == 1)
				return "1 wrong reference counter detected.";
			else {
				return wrongRCs + " wrong reference counters detected.";
			}
		}
		
		@Override
		protected final void fix(WRStore store) throws ACDPException,
																					FileIOException {
			final int nBM = store.nBM;
			final int nobsRefCount = store.nobsRefCount;
			if (maxRC > Utils_.bnd8[nobsRefCount]) {
				throw new ACDPException("Reference counters can't be fixed " +
			           		"because the largest value, which is " + maxRC +
			           		", exceeds the maximum allowed value. You can try " +
			           		"to increase the value of the \"nobsRefCount\" " +
			           		"parameter by refactoring the table.");
			}
			
			final ByteBuffer buf = ByteBuffer.allocate(store.nobsRefCount);
			final byte[] bytes = buf.array();
			
			final FileIO flDataFile = store.flDataFile;
			flDataFile.open();
			try {
				for (FixInfo fi : fiList) {
					Utils.unsToBytes(fi.rc, nobsRefCount, bytes);
					buf.rewind();
					flDataFile.write(buf, fi.pos + nBM);
				}
			}
			finally {
				flDataFile.close();
			}
		}
	}
	
	/**
	 * Runs a series of tests to find out whether the integrity of the table
	 * data contained in the specified store is harmed or not, and, provided that
	 * the {@code fix} argument is set to {@code true}, this method makes an
	 * attempt to fix any detected integrity violations, although not all types
	 * of integrity violations can be automatically fixed.
	 * <p>
	 * Note that this method never throws an exception.
	 * Exceptions and any detected violations are reported with the help of
	 * the specified reporter.
	 * <p>
	 * Invoke this method within a session created solely for the purpose of
	 * verifying the integrity of the table data.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1}.
	 * 
	 * @param  store The store housing the table data to be verified, not allowed
	 *         to be {@code null}.
	 * @param  fix The information whether an attempt should be made to fix any
	 *         detected integrity violations.
	 * @param  rp The reporter, not allowed to be {@code null}.
	 * @param  gapsCache The cache for the gaps arrays of the stores.
	 *         The map must contain an entry for each store of the database.
	 *        
	 * @return The boolean value {@code true} if and only if all tests
	 *         successfully terminated, that is, none of the tests was aborted
	 *         and no integrity violation was detected or the {@code fix}
	 *         argument is set to {@code true} and all detected integrity
	 *         violations could be fixed.
	 */
	final boolean run(WRStore store, boolean fix, Reporter rp,
															Map<WRStore, long[]> gapsCache) {
		boolean good = true;
		// Test 1
		good = good && new Test1().run(store, fix, rp);
		// Test 2
		final boolean good2 = new Test2().run(store, fix, rp);
		good = good && good2;
		// Test 3
		final Test3 test3 = new Test3();
		final boolean good3 = test3.run(store, fix, rp);
		good = good && good3;
		// Test 4
		good = good && new Test4(good3, test3.used()).run(store, fix, rp);
		// Test 5
		good = good && new Test5(good2 && good3, fix, gapsCache).run(store, fix,
																									rp);
		// Test 6
		good = good && new Test6(fix, gapsCache.keySet()).run(store, fix, rp);
		
		return good;
	}
}
