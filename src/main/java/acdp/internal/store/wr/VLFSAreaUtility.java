/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import acdp.exceptions.ACDPException;
import acdp.internal.Column_;
import acdp.internal.FileIOException;
import acdp.internal.store.wr.FLDataReader.ColOffset;
import acdp.internal.store.wr.FLDataReader.FLData;
import acdp.internal.store.wr.FLDataReader.IFLDataReader;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.internal.types.ArrayType_;
import acdp.internal.types.Type_;
import acdp.misc.Utils;
import acdp.types.Type.Scheme;

/**
 * Some key classes used when {@linkplain VLCompactor compacting} the VL file
 * space or when {@linkplain VerifyIntegrity testing} whether their exist some
 * non-trivial VL file space areas that overlap.
 *
 * @author Beat Hoermann
 */
final class VLFSAreaUtility {
	/**
	 * An area within a VL file space.
	 * An area is made of the file position within the VL data file (pointer)
	 * along with the number of bytes comprising the area.
	 * 
	 * @author Beat Hoermann
	 */
	static class FSArea {
		/**
		 * The position within the VL data file where the VL file space area
		 * starts, greater than or equal to zero.
		 */
		long ptr;
		/**
		 * The length of the VL file space area in bytes, greater than or equal
		 * to zero.
		 */
		long length;
		
		/**
		 * Constructs a VL file space area.
		 * 
		 * @param ptr The position within the VL data file where the VL file
		 *        space area starts, must be greater than or equal to zero.
		 * @param length The length of the VL file space area in bytes, must be
		 *        greater than or equal to zero.
		 */
		FSArea(long ptr, long length) {
			this.ptr = ptr;
			this.length = length;
		}
	}
	
	/**
	 * Returns the used VL file space areas for each row in the table starting
	 * with the first row.
	 * It is assumed that there exists some outrow data for the table, that is,
	 * the table must have at least one VL column.
	 * The table may be empty or contain row gaps only.
	 * Furthermore, all pointers to outrow data may point to VL file space areas
	 * of a length equal to zero so that, in fact, the table hasn't allocated
	 * any VL file space.
	 * <p>
	 * Invoke the {@link #reset} method if you want to reuse an instance of this
	 * class.
	 * 
	 * @author Beat Hoermann
	 */
	static final class FSAreaAdvancer {
		/**
		 * The fixed length file space.
		 */
		private final FLFileSpace flFileSpace;
		/**
		 * The number of FL data blocks in the FL file space.
		 */
		private final long nofBlocks;
		/**
		 * The array of pointer offsets of the VL columns within a row.
		 */
		private final int[] ptrOffsets;
		/**
		 * The number of bytes used to persist a pointer.
		 */
		private final int ptrLen;
		/**
		 * The number of VL columns, greater than zero.
		 */
		private final int n;
		/**
		 * The FL data reader to be used, never {@code null}.
		 */
		private final IFLDataReader flDataReader;
		/**
		 * The array of file space areas, reused in every call to the {@code next}
		 * method, never {@code null} and never empty.
		 */
		private final FSArea[] fsAreas;
		/**
		 * The index of the current row, equal to or greater than zero.
		 */
		private long index;
		
		/**
		 * Returns the column info objects of the VL columns.
		 * 
		 * @param  store The store, not allowed to be {@code null}.
		 * 
		 * @return The list of column info objects of the VL columns, never
		 *         {@code null}.
		 */
		private final List<WRColInfo> getVLColInfos(WRStore store) {
			final WRColInfo[] colInfoArr = store.colInfoArr;
			final List<WRColInfo> vlColInfos = new ArrayList<>(colInfoArr.length);
			for (WRColInfo ci : colInfoArr) {
				final Type_ type = ci.col.type();
				if (type.scheme() == Scheme.OUTROW || type instanceof ArrayType_ &&
						((ArrayType_) type).elementType().scheme() == Scheme.OUTROW) {
					vlColInfos.add(ci);
				}
			}
			return vlColInfos;
		}
		
		/**
		 * Returns the offsets of the pointer fields of the specified VL columns
		 * within a row.
		 * 
		 * @param  vlColInfos The list of column info objects of the VL columns.
		 * 
		 * @return The array of pointer offsets of the VL columns within a row.
		 */
		private final int[] getPtrOffsets(List<WRColInfo> vlColInfos) {
			final int n = vlColInfos.size();
			final int[] offsets = new int[n];
			for (int i = 0; i < n; i++) {
				WRColInfo ci = vlColInfos.get(i);
				offsets[i] = ci.offset + ci.lengthLen;
			}
			return offsets;
		}
		
		/**
		 * Returns the array of columns converted from the specified list of
		 * column info objects.
		 * 
		 * @param  vlColInfos The list of column info objects of the VL columns.
		 * @return The array of columns.
		 */
		private final Column_<?>[] toColArray(List<WRColInfo> vlColInfos) {
			final int n = vlColInfos.size();
			final Column_<?>[] cols = new Column_<?>[n];
			for (int i = 0; i < n; i++) {
				cols[i] = vlColInfos.get(i).col;
			}
			return cols;
		}
		
		/**
		 * The constructor.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB1}.
		 * 
		 * @param store The store, not allowed to be {@code null}.
		 */
		FSAreaAdvancer(WRStore store) {
			// Precondition: There is at least one VL column.
			flFileSpace = store.flFileSpace;
			nofBlocks = flFileSpace.nofBlocks();
			final List<WRColInfo> vlColInfos = getVLColInfos(store);
			ptrOffsets = getPtrOffsets(vlColInfos);
			ptrLen = store.nobsOutrowPtr;
			n = vlColInfos.size();
			// Since table has outrow data n must be greater than zero.
			flDataReader = FLDataReader.createNextFLDataReader(toColArray(
											vlColInfos), store, 0, nofBlocks, store.gb1);
			fsAreas = new FSArea[n];
			for (int i = 0; i < n; i++) {
				fsAreas[i] = new FSArea(-1, -1);
			}
			index = 0;
		}

		/**
		 * Returns the VL file space areas of the next row within the table or
		 * {@code null} if there is no next row.
		 * <p>
		 * Note that the references to the {@code FSArea} objects of the returned
		 * array are the same for all calls to this method.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB1}.
		 * 
		 * @return The VL file space areas of the next row or {@code null} if
		 *         there is no next row.
		 *         No element in the returned array is {@code null}.
		 * 
		 * @throws ACDPException If the pointer to some outrow data or the length
		 *         of some outrow data is less than zero.
		 *         This exception can happen only if the integrity of the table
		 *         data is violated.
		 * @throws FileIOException If an I/O error occurs while reading the row
		 *         data.
		 */
		final FSArea[] next() throws ACDPException, FileIOException {
			FLData flData = null;
			while (index < nofBlocks && flData == null) {
				// The area advancer is assumed to be invoked within a context
				// where the flDataReader.readFLData method never throws a
				// ShutdownException.
				flData = flDataReader.readFLData(index++);
			}
			// flData == null implies index >= nofRows
			
			if (flData == null)
				return null;
			else {
				// Convert VL pointers to file space areas.
				final byte[] bytes = flData.bytes;
				final ColOffset[] colOffsets = flData.colOffsets;
				
				for (int i = 0; i < n; i++) {
					final ColOffset co = colOffsets[i];
					final int offset = co.offset;
					final int lengthLen = co.ci.lengthLen;
					final long ptr = Utils.unsFromBytes(bytes, offset + lengthLen,
																								ptrLen);
					final long length = Utils.unsFromBytes(bytes, offset, lengthLen);
					if (ptr < 0 || length < 0) {
						throw new ACDPException("Pointer to outrow data or length " +
															"of outrow data less than zero.");
					}
					final FSArea fsArea = fsAreas[i];
					fsArea.ptr = ptr;
					fsArea.length = length;
				}
				
				return fsAreas;
			}
		}
		
		/**
		 * Returns the position within the FL data file of the pointer field
		 * of the VL column with the specified index and of the current row.
		 * The current row is equal to the row of the VL file space areas that
		 * were returned by the {@code next} method.
		 * 
		 * @param  i The index of the VL column.
		 * 
		 * @return The position within the FL data file of the {@code i}th
		 *         pointer of the current row.
		 */
		final long filePos(int i) {
			return flFileSpace.indexToPos(index - 1) + ptrOffsets[i];
		}
		
		/**
		 * Prepares this advancer for being reused.
		 */
		final void reset() {
			index = 0;
		}
	}
	/**
	 * A <em>treap</em> that stores disjoint VL file space areas.
	 * The treap data structure was first introduced by C. Aragon and R. Seidel
	 * in 1989.
	 * The code below was inspired by a work of Mark Allen Weiss which I found
	 * at {@code http://users.cis.fiu.edu/~weiss/dsaajava3/code/Treap.java}.
	 * <p>
	 * The treap is guaranteed to store disjoint VL file space areas only.
	 * Trying to insert a VL file space area that overlaps with a VL file space
	 * area contained in the treap raises an {@code ACDPException}.
	 * <p>
	 * The {@link #computeFSAreaList} method returns a list of disjoint
	 * <em>and</em> unconnected VL file space areas sorted in ascending order
	 * with respect to the values of their pointer fields made of the VL file
	 * space areas stored in the treap.
	 * 
	 * @author Beat Hoermann
	 */
	static final class FSAreaTreap {
		
		/**
		 * A node of a non-empty treap.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class Node {
			private static final Random random = new Random();

			/**
			 * The priority of the node.
			 */
			final int priority;
			/**
			 * The VL file space area, never {@code null}.
			 */
			final FSArea fsArea;
			/**
			 * The left subtreap of the node or {@code null} if this node has no
			 * left subtreap.
			 */
			Node left;
			/**
			 * The right subtreap of the node or {@code null} if this node has no
			 * right subtreap.
			 */
			Node right;
			
			/**
			 * The constructor.
			 * 
			 * @param fsArea The VL file space area, not allowed to be {@code
			 *        null}.
			 */
			Node(FSArea fsArea) {
				this.priority = random.nextInt();
				this.fsArea = fsArea;
				this.left = null;
				this.right = null;
			}
		}
		
		/**
		 * The root node.
		 * The root node can't be declared final because it changes if balancing
		 * the treap propagates to the very top of the treap.
		 */
		private Node root;
		
		/**
		 * The constructor.
		 */
		FSAreaTreap() {
			root = null;
		}
		
		/**
		 * The famous right rotation of a binary tree with the specified root.
		 * 
		 * @param  t The root of the tree.
		 *         The values of {@code t} and {@code t.left} are not allowed to
		 *         be {@code null}.
		 *         
		 * @return The new root of the rotated tree, never {@code null}.
		 */
		private final Node rotateRight(Node t) {
			final Node t1 = t.left;
			t.left = t1.right;
			t1.right = t;
			return t1;
		}
		
		/**
		 * The famous left rotation of a binary tree with the specified root.
		 * 
		 * @param  t The root of the tree.
		 *         The values of {@code t} and {@code t.right} are not allowed to
		 *         be {@code null}.
		 *         
		 * @return The new root of the rotated tree, never {@code null}.
		 */
		private final Node rotateLeft(Node t) {
			final Node t1 = t.right;
			t.right = t1.left;
			t1.left = t;
			return t1;
		}

		/**
		 * Recursively inserts the specified VL file space area into the specified
		 * subtreap.
		 * <p>
		 * Note that if the specified VL file space area is <em>connected</em>
		 * with some file space area found in the treap then the latter one is
		 * just "extended" rather than the first one being inserted into the
		 * treap as a new treap node.
		 * <p>
		 * Note also that if the specified VL file space area overlaps with a
		 * previously inserted VL file space then this method throws an {@code
		 * ACDPException}.
		 * 
		 * @param  fsArea The VL file space area to insert, not allowed to be
		 *         {@code null}.
		 * @param  t The root of the subtreap or {@code null} if the subtreap is
		 *         empty.
		 *         
		 * @return The new root of the subtreap, never {@code null}.
		 * 
		 * @throws ACDPException If the specified VL file space area overlaps
		 *         with a previously inserted VL file space.
		 *         This exception can happen only if the integrity of the table
		 *         data is violated.
		 */
		private final Node insert(FSArea fsArea, Node t) throws ACDPException {
			if (t == null) {
				return new Node(fsArea);
			}
			
			final long newPtr = fsArea.ptr;
			final FSArea tFsArea = t.fsArea;
			final long ptr = tFsArea.ptr;
			
			if (newPtr < ptr) {
				final long endPtr = newPtr + fsArea.length;
				if (endPtr < ptr) {
					// New area is on the "left" side of the node area with a gap in
					// between. Insert left.
					t.left = insert(fsArea, t.left);
					if (t.left.priority < t.priority) {
						t = rotateRight(t);
					}
				}
				else if (endPtr == ptr) {
					// New area is on the "left" side of the node area with no gap
					// in between, hence, they are connected. Merge them.
					tFsArea.ptr = newPtr;
					tFsArea.length += fsArea.length;
				}
				else {
					// New area overlaps with node area!
					throw new ACDPException("Overlapping file space areas!");
				}
			}
			else if (ptr < newPtr) {
				final long endPtr = ptr + tFsArea.length;
				if (endPtr < newPtr) {
					// New area is on the "right" side of the node area with a gap
					// in between. Insert right.
					t.right = insert(fsArea, t.right);
					if (t.right.priority < t.priority) {
						t = rotateLeft(t);
					}
				}
				else if (endPtr == newPtr)
					// New area is on the "right" side of the node area with no gap
					// in between, hence, they are connected. Merge them.
					tFsArea.length += fsArea.length;
				else {
					// New area overlaps with node area!
					throw new ACDPException("Overlapping file space areas!");
				}
			}
			
			return t;
		}
		
		/**
		 * Inserts the specified VL file space area into the treap.
		 * <p>
		 * Note that if the specified VL file space area is <em>connected</em>
		 * with some VL file space area found in the treap then the latter one is
		 * just "extended" rather than the first one being inserted into the
		 * treap as a new treap node.
		 * 
		 * @param  fsArea The VL file space area to insert, not allowed to be
		 *         {@code null}.
		 *         
		 * @throws ACDPException If the specified VL file space area overlaps
		 *         with a previously inserted VL file space.
		 *         This exception can happen only if the integrity of the table
		 *         data is violated.
		 */
		final void insert(FSArea fsArea) throws ACDPException {
			 root = insert(fsArea, root);
		}
		
		/**
		 * Tests if this treap is empty.
		 * 
		 * @return The boolean value {@code true} if and only if this treap is
		 *         empty.
		 */
		final boolean isEmpty() {
			return root == null;
		}
		
		/**
		 * Returns the leftmost node contained in the specified subtreap.
		 * 
		 * @param  t The subtreap, may be {@code null}.
		 * 
		 * @return The leftmost node contained in the specified subtreap of {@code
		 *         null} if and only if the specified subtreap is {@code null}.
		 */
		private final Node getLeftMost(Node t) {
			if (t == null)
				return null;
			else {
				// t != null
				final Node leftMost = getLeftMost(t.left);
				return leftMost == null ? t : leftMost;
			}
		}
		
		/**
		 * Returns the VL file space area with the smallest pointer.
		 *
		 * @return The VL file space area with the smallest pointer or {@code
		 *         null} if and only if the treap is empty.
		 */
		final FSArea getLeftMost() {
			return root == null ? null : getLeftMost(root).fsArea;
		}
		
		/**
		 * Returns the rightmost node contained in the specified subtreap.
		 * 
		 * @param  t The subtreap, may be {@code null}.
		 * 
		 * @return The rightmost node contained in the specified subtreap of
		 *         {@code null} if and only if the specified subtreap is {@code
		 *         null}.
		 */
		private final Node getRightMost(Node t) {
			if (t == null)
				return null;
			else {
				// t != null
				final Node rightMost = getLeftMost(t.left);
				return rightMost == null ? t : rightMost;
			}
		}
		
		/**
		 * Returns the VL file space area with the largest pointer.
		 *
		 * @return The VL file space area with the largest pointer or {@code
		 *         null} if and only if the treap is empty.
		 */
		final FSArea getRightMost() {
			return root == null ? null : getRightMost(root).fsArea;
		}
		
		/**
		 * Recursively merges any connected VL file space areas found in the
		 * treap into a single VL file space area and recursively fills the
		 * specified list with disjoint and unconnected VL file space areas
		 * sorted in ascending order with respect to the values of their pointer
		 * fields.
		 * 
		 * @param  t The root of the subtreap, may be {@code null}.
		 * @param  list The list where the VL file spaces are stored.
		 * @param  last The VL file space area that was last inserted into the
		 *         list or a suitable dummy VL file space area in the case that
		 *         the list is empty.
		 *          
		 * @return The VL file space area that was last inserted into the list,
		 *         never {@code null}.
		 */
		private final FSArea computeFSAreaList(Node t, List<FSArea> list,
																						FSArea last) {
			if(t != null) {
				last = computeFSAreaList(t.left, list, last);
				
				if (last.ptr + last.length == t.fsArea.ptr)
					// Last area in the list is connected with node area. Merge node
					// area into last area.
					last.length += t.fsArea.length;
				else {
					// Last area in the list is not connected with node area.
					last = t.fsArea;
					list.add(last);
				}
				
				last = computeFSAreaList(t.right, list, last);
			}
			return last;
		}
		
		/**
		 * Merges any connected VL file space areas found in the treap into a
		 * single VL file space area and computes a list of disjoint and
		 * unconnected VL file space areas sorted in ascending order with respect
		 * to the values of their pointer fields.
		 * 
		 * @return The sorted list of disjoint and unconnected VL file space
		 *         areas, never {@code null}.
		 *         The list is empty if and only if not a single VL file space
		 *         area was inserted into the treap.
		 */
		final List<FSArea> computeFSAreaList() {
			final List<FSArea> list = new ArrayList<>();
			// Adding a dummy element into the list to simplify the method invoked
			// below is not an option because removing it later is an expensive
			// operation.
			computeFSAreaList(root, list, new FSArea(Long.MAX_VALUE, 1));
			return list;
		}
	}
}
