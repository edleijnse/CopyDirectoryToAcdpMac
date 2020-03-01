/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.misc.array;

import java.util.Iterator;

/**
 * The base class for a class implementing a dynamically growing array with
 * elements being byte arrays.
 * The construction is based on a singly linked sequence of nodes where each
 * node points to a byte array of a carefully chosen size.
 *
 * @author Beat Hoermann
 */
abstract class ByteArray implements Iterable<byte[]> {
	/**
	 * A node housing a byte array and a pointer to the next node.
	 * 
	 * @author Beat Hoermann
	 */
	protected static final class Node {
		protected final byte[] byteArray;
		protected Node next;

		/**
		 * Constructs a new node with the specified node size.
		 * 
		 * @param nodeSize The node size.
		 *        Must be greater than zero.
		 */
		private Node(int nodeSize) {
			this.byteArray = new byte[nodeSize];
			this.next = null;
		}
   }
	
	private final NodeSizer nodeSizer;
	private final GrowthBounder growthBounder;
	private final Rounder rounder;
	
	protected long size;
	protected Node last;
	protected int pos;
	
	/**
	 * Returns the next node size starting from the node size of the first node.
	 * The node size of a node is the maximum number of elements the node can
	 * store.
	 * The exact value of the returned node size {@code s} depends on the type
	 * of the {@link Rounder}.
	 * However, if the {@link #growthBounder} is not set to {@code null} then
	 * this method returns the minimim of the value returned by the
	 * {@link GrowthBounder#bound() bound} method and {@code s}.
	 * 
	 * @return The next node size.
	 */
	protected final int nextNodeSize() {
		int n = rounder.round(nodeSizer.getNextSize());
		return growthBounder == null ? n : Math.min(n, growthBounder.bound());
	}
	
	/**
    * Constructs an empty byte array.
    * 
	 * @param  t The initial capacity.
	 *         Set the value to the exact or estimated minimum number of bytes
	 *         in the array.
	 *         If you do not know such a number then set the value to be less
	 *         than or equal to zero.
	 * @param  c The size.
	 *         Set the value to the <em>estimated</em> maximum number of bytes
	 *         in the array.
	 *         An estimation of the order of magnitude of the size is sufficient.
	 *         For example, if the real size is 100'000 then an estimation
	 *         between 1'000 and 10'000'000 is sufficient.
	 *         If you do not know such a number then set the value to be less
	 *         than or equal to zero.
	 *         In such a case the capacity is internally set to half of the
	 *         current amount of total free memory.
	 * @param  growthBounder The {@link GrowthBounder} or {@code null}.
	 * @param  rounder The {@link Rounder} to round the node sizes.
	 * 
    * @throws IllegalArgumentException If the specified value for {@code t} is
    *         too large.
	 */
	protected ByteArray(int t, long c, GrowthBounder growthBounder,
																				Rounder rounder) {
		this.nodeSizer = new NodeSizer(t, c, 28.0);
		this.growthBounder = growthBounder;
		this.rounder = rounder;
		
		size = 0;
		last = new Node(nextNodeSize());
		last.next = last;
		pos = 0;
	}
	
	/**
	 * Creates a new node of the specified node size and correctly links it
	 * with the other nodes.
	 * As a side effect, updates the value of the {@link #last} property.
	 * 
	 * @param nodeSize The node size of the new node.
	 */
	protected final void addNodeUpdateLast(int nodeSize) {
		Node lLast = last;
		last = new Node(nodeSize);
		last.next = lLast.next;
		lLast.next = last;
	}
	
	/**
    * Appends the specified byte array to the end of this array.
    * The values of the individual bytes of the specified byte array are copied
    * into the array (<em>call by value</em>).
    *
    * @param  e The byte array to be appended to this array.
    * 
	 * @throws NullPointerException If this method is called after a call to the
	 *         {@link #iterator()} method.
	 */
	public abstract void add(byte[] e) throws NullPointerException;
	
	/**
	 * A {@linkplain DominoIterator domino iterator}.
	 * The {@link Iterator#remove() remove} method is not supported.
	 * 
	 * @author Beat Hoermann
	 */
	protected abstract class DominoItr implements Iterator<byte[]>,
																					DominoIterator {
		protected Node curNode;
		protected int curPos;
		
		protected DominoItr() {
			curNode = last.next;
			curPos = 0;
			
			// Expose the nodes to the garbage collector if they are no longer
			// used by the iterator, thus destroying the array. Exposing a node to
			// the garbage collector means to remove any references to it.
			
			// Set last.next to null to break the cycle. This has the effect
			// that the first node is referenced by the local variable curNode
			// only.
			last.next = null;
			
			// Remove the global reference to the last node so that the last
			// node can be garbage collected as soon as the iterator is
			// exposed to the garbage collector.
			last = null;
		}
		
		@Override
		public final boolean hasNext() {
			return curNode.next != null || curPos < pos;
		}
		
		@Override
		public final void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
	/**
	 * Returns a {@linkplain DominoIterator domino iterator} over the byte
	 * arrays in this array.
    *
    * @return A domino iterator over the byte arrays in this array.
    */
	@Override
	public abstract Iterator<byte[]> iterator();
	
	/**
	 * Returns the number of byte arrays in this array.
	 * 
	 * @return The number of byte arrays, always greater than or equal to
	 *         zero.
	 */
	public final long size() {
		return size;
	}
}
