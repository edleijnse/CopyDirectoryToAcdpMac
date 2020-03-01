/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.misc.array;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Implements a dynamically growing array with elements of a parameterizable
 * type.
 * <p>
 * It should be straightforward to refactor this class to satisfy the
 * {@link List} interface and thus obtaining an alternative solution to the
 * {@link ArrayList} and {@link java.util.LinkedList LinkedList} implementations
 * for large arrays.
 * 
 * @param <E> The type of elements in this array.
 *
 * @author Beat Hoermann
 */
public final class GenericArray<E> implements Iterable<E> {
	/**
	 * Size of Java's object pointer.
	 * Should be 4 for machines with less than or equal to 32 GiB of physical
	 * memory.
	 * It is not clear to me how to programmatically find out the amount of
	 * physical memory installed on a machine or to directly find out the size
	 * of Java's object pointer in a platform independent manner.
	 */
	private static final int REF_SIZE = 4;
	
	private final NodeSizer nodeSizer;
	private final GrowthBounder growthBounder;
	private final List<Object[]> nodes;
	
	private final Rounder rounder;
	
	private long size;
	private int pos;
	private Object[] last;
	
	/**
	 * Constructs an empty array with an initial capacity of three elements.
	 */
	public GenericArray() {
		this(3, 0, null);
	}
	
	/**
	 * Returns the next node size starting from the node size of the first node.
	 * The node size of a node is the maximum number of elements the node can
	 * store.
	 * The returned number {@code s} is such that 12 + {@code s}*4 is divisible
	 * by 8 without remainder thus respecting Java's array-object alignment
	 * restriction for a size of Java's object pointer equal to 4 bytes.
	 * <p>
	 * However, if the {@link #growthBounder} is not set to {@code null} then
	 * this method returns the minimim of the value returned by the
	 * {@link GrowthBounder#bound() bound} method and {@code s}.
	 * 
	 * @return The next node size.
	 */
	private int nextNodeSize() {
		int n = rounder.round(nodeSizer.getNextSize()) / 4;
		return growthBounder == null ? n : Math.min(n, growthBounder.bound());
	}
	
	/**
    * Constructs an empty array with the specified values for the initial
    * capacity, the size and the {@linkplain GrowthBounder growth bounder}.
    *
	 * @param  t The initial capacity.
	 *         Set the value to the exact or estimated minimum number of
	 *         elements in the array.
	 *         If you do not know such a number then set the value to be less
	 *         than or equal to zero.
	 * @param  c The size.
	 *         Set the value to the <em>estimated</em> maximum number of
	 *         elements in the array.
	 *         An estimation of the order of magnitude of the size is sufficient.
	 *         For example, if the real size is 100'000 then an estimation
	 *         between 1'000 and 10'000'000 is sufficient.
	 *         If you do not know such a number then set the value to be less
	 *         than or equal to zero.
	 *         In such a case the capacity is internally set to half of the
	 *         current amount of total free memory.
	 * @param  growthBounder a growth bounder or {@code null}.
    * @throws IllegalArgumentException If the specified value for {@code t} is
    *         too large.
	 */
	public GenericArray(int t, long c, GrowthBounder growthBounder) {
		if (c > 0 && t > c) {
			throw new IllegalArgumentException();
		}
		
		if (t <= 0)
			t = 3 * REF_SIZE;
		else {
			t *= REF_SIZE;
		}
		if (c > 0) {
			c = c <= Long.MAX_VALUE / REF_SIZE ? Math.max(t, c * REF_SIZE) :
																					Long.MAX_VALUE;
		}
		
		this.nodeSizer = new NodeSizer(t, c, 17.0);
		this.growthBounder = growthBounder;
		
		// Change if {@link #REF_SIZE} is not equal to 4.
		this.rounder = new Rounder4();
		
		this.size = 0;
		this.pos = 0;
		this.nodes = new ArrayList<Object[]>(1);
		this.last = new Object[nextNodeSize()];
		
		nodes.add(last);
	}
	
   /**
    * Appends the specified element to the end of this array.
    *
    * @param e The element to be appended to this array.
    */
	public final void add(E e) {
		if (pos == last.length) {
			// Add new node.
			last = new Object[nextNodeSize()];
			nodes.add(last);
			pos = 0;
		}
		last[pos] = e;
		pos++;
		size++;
	}
	
	/**
	 * An iterator.
	 * The {@link Iterator#remove() remove} method is not supported.
	 * 
	 * @author Beat Hoermann
	 */
	private class Itr implements Iterator<E> {
		private final Iterator<Object[]> nodesIt;
		private Object[] curNode;
		private boolean hasNextNode;
		private int curPos;
		
		private Itr() {
			nodesIt = nodes.iterator();
			curNode = nodesIt.next();
			hasNextNode = nodesIt.hasNext();
			curPos = 0;
		}

		@Override
		public boolean hasNext() {
			return hasNextNode || curPos < pos; 
		}

		@Override
		public E next() {
			// Precondition: hasNextNode || curPos < pos;
			if (curPos == curNode.length) {
				curNode = nodesIt.next();
				hasNextNode = nodesIt.hasNext();
				curPos = 0;
			}
			
			@SuppressWarnings("unchecked")
			final E result = (E) curNode[curPos++];
			
			return result;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
	/**
	 * Returns an iterator over the elements in this array.
    *
    * @return An iterator over the elements in this array.
    */
	@Override
	public final Iterator<E> iterator() {
		return new Itr();
	}
	
	/**
	 * A domino iterator, see the description of the {@linkplain DominoIterator}
	 * interface.
	 * The {@link Iterator#remove() remove} method is not supported.
	 * 
	 * @author Beat Hoermann
	 */
	private class DominoItr implements Iterator<E>, DominoIterator {
		private int curNodeIndex;
		private Object[] curNode;
		private boolean hasNextNode;
		private int curPos;
		
		private DominoItr() {
			curNodeIndex = 0;
			curNode = nodes.get(0);
			hasNextNode = curNodeIndex < nodes.size() - 1;
			curPos = 0;
			
			// Remove the global reference to the last node so that the last
			// node can be garbage collected as soon as the domino iterator is
			// exposed to the garbage collector.
			last = null;
		}

		@Override
		public boolean hasNext() {
			return hasNextNode || curPos < pos; 
		}

		@Override
		public E next() {
			// Precondition: hasNextNode || curPos < pos;;
			if (curPos == curNode.length) {
				nodes.set(curNodeIndex, null);
				curNodeIndex++;
				curNode = nodes.get(curNodeIndex);
				hasNextNode = curNodeIndex < nodes.size() - 1;
				curPos = 0;
			}
			
			@SuppressWarnings("unchecked")
			final E result = (E) curNode[curPos++];
			
			return result;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
	/**
	 * Returns a {@linkplain DominoIterator domino iterator} over the elements
	 * in this array.
    *
    * @return A domino iterator over the elements in this array.
    */
	public final Iterator<E> dominoIterator() {
		return new DominoItr();
	}
	
	/**
	 * Returns the number of elements in this array.
	 * 
	 * @return The number of elements, always greater than or equal to zero.
	 */
	public final long size() {
		return size;
	}

}
