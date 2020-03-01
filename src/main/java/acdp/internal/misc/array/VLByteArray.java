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
 * a variable length.
 *
 * @author Beat Hoermann
 */
public class VLByteArray extends ByteArray {
	private final int sizeLength;
	private final byte[] buffer;
	
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
	 * @param  growthBounder a {@linkplain GrowthBounder growth bounder} or
	 *         {@code null}.
	 * @param  sizeLength The length of the size information of the byte arrays
	 *         stored in this array.
	 *         This value must be greater than zero and less than or equal to
	 *         8.
    * @throws IllegalArgumentException If the specified value for {@code t} is
    *         too large or if the specified value for {@code sizeLength} is less
    *         than or equal to zero or greater than 8.
	 */
	public VLByteArray(int t, long c, GrowthBounder growthBounder,
																					int sizeLength) {
		super(t, c, growthBounder, new Rounder4());
		
		if (sizeLength <= 0 || 8 < sizeLength) {
			throw new IllegalArgumentException();
		}
		this.sizeLength = sizeLength;
		this.buffer = new byte[sizeLength];
	}
	
	/**
    * Appends the specified byte array to the end of this array.
    * The values of the individual bytes of the specified byte array are copied
    * into the array (<em>call by value</em>).
    *
    * @param  e The byte array to be appended to this array.
    *         The length of the byte array must be greater zero and less than
    *         or equal to 256<sup>{@code n}</sup> - 1 where {@code n} denotes
    *         the value passed via the {@code sizeLength} parameter of the
    *         constructor.
	 * @throws NullPointerException If this method is called after a call to the
	 *         {@link #iterator()} method.
	 */
	@Override
	public void add(byte[] e) throws NullPointerException {
		Unsigned.toBytes(e.length, sizeLength, buffer);
		addSized(buffer);
		addSized(e);
	}
	
	/**
    * Appends the specified <em>sized byte array</em> to the end of this array.
    * A <em>sized byte array</em> is a byte array that starts with the
    * information about its size.
    * Therefore, if {@code n} denotes the value passed via the
    * {@code sizeLength} parameter of the constructor then the specified byte
    * array must start with an unsigned integer of length {@code n} specifying
    * the size of the byte array.
    * <p>
    * The values of the individual bytes of the specified sized byte array are
    * copied into the array (<em>call by value</em>).
    *
    * @param  e The sized byte array to be appended to this array.
	 * @throws NullPointerException If this method is called after a call to the
	 *         {@link #iterator()} method.
	 */
	public final void addSized(byte[] e) throws NullPointerException {
		// Precondition: e.length > 0.
		final int len = e.length;
		int copyPos = 0;
		
		int avail = last.byteArray.length - pos;
		// Copy n >= 0 bytes at most.
		int n = Math.min(len - copyPos, avail);
		System.arraycopy(e, copyPos, last.byteArray, pos, n);
		copyPos += n;
		pos += n;
		while (copyPos < len) {
			// Add new node.
			avail = nextNodeSize();
			addNodeUpdateLast(avail);
			pos = 0;
			// Copy n > 0 bytes at most.
			n = Math.min(len - copyPos, avail);
			System.arraycopy(e, copyPos, last.byteArray, pos, n);
			copyPos += n;
			pos += n;
		}
		size++;
	}
	
	/**
	 * A {@linkplain DominoIterator domino iterator}.
	 * The {@link Iterator#remove() remove} method is not supported.
	 * 
	 * @author Beat Hoermann
	 */
	private final class VLDominoItr extends DominoItr {
		
		private VLDominoItr() {
			super();
		}
		
		private void copy(final int len, final byte[] arr) {
			// Precondition: len > 0 && arr.length >= len.
			// Precondition: hasNextNode || curPos < pos where
			//               hasNextNode <-> curNode.next != null
			// Precondition: !hasNextNode && curPos < pos implies
			//               curPos + len <= pos.
			// Remember: !hasNextNode implies pos <= curNode.byteArray.length so
			//           that curNode.byteArray.length - curPos >= len.
			int copyPos = 0;
			
			int avail = curNode.byteArray.length - curPos;
			// Copy n >= bytes at most.
			int n = Math.min(len, avail);
			System.arraycopy(curNode.byteArray, curPos, arr, copyPos, n);
			copyPos += n;
			curPos += n;
			while (copyPos < len) {
				// Go to next node.
				curNode = curNode.next;
				curPos = 0;
				avail = curNode.byteArray.length;
				// Copy n > 0 bytes at most.
				n = Math.min(len - copyPos, avail);
				System.arraycopy(curNode.byteArray, curPos, arr, copyPos, n);
				copyPos += n;
				curPos += n;
			}
		}

		@Override
		public final byte[] next() {
			// Precondition: curNode.next != null || curPos < pos.
			copy(sizeLength, buffer);
			final int len = (int) Unsigned.fromBytes(buffer, sizeLength);
			final byte[] e = new byte[len];
			copy(len, e);
			
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
		return new VLDominoItr();
	}
}
