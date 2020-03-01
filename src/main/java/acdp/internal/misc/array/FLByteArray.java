/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.misc.array;

import java.util.Iterator;


/**
 * Implements a dynamically growing array with elements being byte arrays of
 * a fixed length.
 *
 * @author Beat Hoermann
 */
public class FLByteArray extends ByteArray {
	private final int length;
	
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
	 * @param  growthBounder A {@link GrowthBounder} or {@code null}.
	 * @param  length The length of the byte arrays stored in this array.
	 * 
    * @throws IllegalArgumentException If the specified value for {@code t} is
    *         too large or if the specified value for {@code length} is less
    *         than or equal to zero.
	 */
	public FLByteArray(int t, long c, GrowthBounder growthBounder, int length) {
		super(t, c, growthBounder, (length == 1 || length == 2 || length == 4) ?
												new Rounder4() : new RounderLen(length));
		if (length <= 0) {
			throw new IllegalArgumentException();
		}
		this.length = length;
	}
	
	/**
    * Appends the specified byte array to the end of this array.
    * The values of the individual bytes of the specified byte array are copied
    * into the array (<em>call by value</em>).
    *
    * @param  e The byte array to be appended to this array.
    *         The length of the byte array must be equal to the value of the
    *         {@code length} parameter passed to the constructor.
    *         
	 * @throws NullPointerException If this method is called after a call to the
	 *         {@link #iterator()} method.
	 */
	@Override
	public final void add(byte[] e) throws NullPointerException {
		// Precondition: e.length == length and node sizes of nodes are equal
		//               to m*length for an integer m >= 1.
		if (pos == last.byteArray.length) {
			// Add new node and update last.
			addNodeUpdateLast(nextNodeSize());
			pos = 0;
		}
		// pos + length <= last.byteArray.length
		System.arraycopy(e, 0, last.byteArray, pos, length);
		pos += length;
		size++;
	}
	
	/**
	 * A {@linkplain DominoIterator domino iterator}.
	 * The {@link Iterator#remove() remove} method is not supported.
	 * 
	 * @author Beat Hoermann
	 */
	private final class FLDominoItr extends DominoItr {
		
		private FLDominoItr() {
			super();
		}

		@Override
		public byte[] next() {
			// Precondition: curNode.next != null || curPos < pos.
			if (curPos == curNode.byteArray.length) {
				// Implies curNode.next != null because pos <= length.
				curNode = curNode.next;
				curPos = 0;
			}
			// Precondition: curPos + length <= curNode.byteArray.length.
			byte[] e = new byte[length];
			System.arraycopy(curNode.byteArray, curPos, e, 0, length);
			curPos += length;
			
			return e;
		}
	}
	
	/**
	 * Returns a {@linkplain DominoIterator domino iterator} over the byte 
	 * arrays in this array.
    *
    * @return A domino iterator over the byte arrays in this array.
    */
	@Override
	public final Iterator<byte[]> iterator() {
		return new FLDominoItr();
	}
}
