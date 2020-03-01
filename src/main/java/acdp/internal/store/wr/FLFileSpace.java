/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.nio.ByteBuffer;
import java.nio.file.OpenOption;

import static java.nio.file.StandardOpenOption.*;

import java.util.Arrays;

import acdp.exceptions.CreationException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.FileSpaceStateTracker;
import acdp.internal.IUnit;
import acdp.internal.FileSpaceStateTracker.IFileSpace;
import acdp.internal.FileSpaceStateTracker.IFileSpaceState;
import acdp.misc.Utils;

/**
 * For a given file and a positive integer {@code n} &ge; 8 this class allocates
 * and deallocates file space in blocks of {@code n} bytes.
 * <p>
 * A client requests a memory block by invoking the {@link #allocate} method
 * which returns the position {@code p} within the file where the memory block
 * starts.
 * Once the memory block is no longer needed, the client invokes the {@link
 * #deallocate} method on {@code p}.
 * <p>
 * Internally, the memory space is organized as an array of memory blocks all
 * having the same size of {@code n} bytes.
 * The first memory block has an <em>index</em> equal to zero.
 * <p>
 * A deallocated memory block is called a <em>gap</em>.
 * Gaps can be reused.
 * For this purpose, this class keeps a chain of gaps, organised as a linked
 * list whereby the value of the pointer to a gap is identical to the index of
 * that gap.
 * We call the index of the first gap within the chain of gaps the <em>root
 * index</em> and the corresponding gap the <em>root gap</em>.
 * Each gap within the chain of gaps stores the index of the next gap where the
 * last gap stores as index the number of memory blocks (allocated and
 * deallocated ones) currently managed by the file space.
 * If there are no gaps then the root index is equal to the number of allocated
 * memory blocks.
 * <p>
 * A client requesting a memory block gets the position that corresponds to the
 * root index.
 * Note that if the chain of gaps is empty then this position is equal to or
 * greater than the size of the underlying file.
 * (This class relies on the convention that a file grows if data is written
 * beyond the end of the file.)
 * <p>
 * If a client deallocates a memory block then this memory block is inserted at
 * the head of the chain of gaps, hence, this memory block becomes the root gap.
 * <p>
 * The first bit of a gap, the so called <em>gap flag</em>, is equal to 1.
 * Thus, a client that guarantees that the first bit of an allocated memory
 * block is always set to 0 is able to easily identify a gap.
 * <p>
 * The size of a memory block, hence the value of {@code n}, must be large
 * enough to keep both, the gap flag and the index of the next gap.
 * This is why the value of {@code n} must be at least 8 bytes, allowing for
 * a file space whose size is beyond the practical maximum size of {@code z}
 * bytes where {@code z} is equal to the value of the {@code Long.MAX_Value}
 * constant.
 * <p>
 * The number of gaps is stored at the beginning of the file just before the
 * root index.
 * This enables a fast computation of both, the rate of FL file space
 * fragmentation as well as the number of allocated memory blocks.
 * The last one is important for quickly finding out the number of rows in a
 * table.
 *
 * @author Beat Hoermann
 */
final class FLFileSpace implements IFileSpace {
	
	/**
	 * The internal state of an FL file space.
	 * 
	 * @author Beat Hoermann
	 */
	private final class FLFileSpaceState implements IFileSpaceState {
		final long size;
		final long gaps;
		final long root;
		
		/**
		 * The constructor.
		 * 
		 * @param size See {@link FLFileSpace#size}.
		 * @param gaps See {@link FLFileSpace#gaps}.
		 * @param root See {@link FLFileSpace#root}.
		 */
		FLFileSpaceState(long size, long gaps, long root) {
			this.size = size;
			this.gaps = gaps;
			this.root = root;
		}
	}
	
	/**
	 * The underlying file.
	 */
	private final FileIO file;
	
	/**
	 * The file space tracker, never {@code null}.
	 */
	private final FileSpaceStateTracker fssTracker;
	
	/**
	 * The number of bytes of the memory blocks, greater than or equal to 8.
	 */
	private final int n;
	
	/**
	 * The position within the underlying file where the first memory block
	 * starts.
	 */
	private final int start = 16;
	
	/**
	 * A byte buffer with a capacity of 16 bytes.
	 */
	private final ByteBuffer buf16;
	
	/**
	 * A byte buffer with a capacity of 8 bytes.
	 */
	private final ByteBuffer buf8;
	
	/**
	 * The backing array of {@code buf16} and {@code buf8}.
	 */
	private final byte[] arr;
	
	/**
	 * The size of the underlying file.
	 */
	private long size;
	
	/**
	 * The number of gaps.
	 */
	private long gaps;
	
	/**
	 * The index of the root gap.
	 * This value is equal to the number of allocated memory blocks if there are
	 * no gaps.
	 */
	private long root;
	
	/**
	 * Constructs the file space manager for the specified file.
	 * 
	 * @param  file The {@linkplain FileIO#FileIO(java.nio.file.Path,
	 *         acdp.internal.misc.FileChannelProvider) closed} underlying file.
	 * @param  fssTracker The file space state tracker.
	 *         If this value is equal to {@code null} then the file space manager
	 *         is constructed in read-only mode which means that the {@code
	 *         allocate} and {@code deallocate} methods cannot be used.
	 * @param  n The size in bytes of the fixed length memory blocks, must be
	 *         greater than or equal to 8.
	 *         
	 * @throws CreationException If I/O error occurs or if there is any reason
	 *         that prevents the file space manager from being created.
	 * 
	 */
	FLFileSpace(FileIO file, FileSpaceStateTracker fssTracker, int n) throws
																				CreationException {
		this.file = file;
		this.fssTracker = fssTracker;
		
		if (n < 8) {
			throw new CreationException("File \"" + file.path + "\": The " +
												"size of the fixed length memory " +
												"blocks is less than 8 bytes: " + n + ".");
		}
		this.n = n;
	
		this.arr = new byte[16];
		this.buf16 = ByteBuffer.wrap(arr);
		this.buf8 = ByteBuffer.wrap(arr);
		buf8.limit(8);
		
		// Initialize size, gaps, root.
		// We don't want to request the file channel from the file channel
		// provider at this stage.
		try (FileIO tempFile = new FileIO(file.path, fssTracker != null ?
					new OpenOption[] { READ, WRITE } : new OpenOption[] { READ })) {
			this.size = tempFile.size();
			if (size == 0) {
				this.size = start;
				this.gaps = 0;
				this.root = 0;
				if (fssTracker != null) {
					// writable mode
					buf16.putLong(gaps);
					buf16.putLong(root);
					buf16.rewind();
					tempFile.write(buf16, 0);
				}
			}
			else if ((size - start) % n != 0) {
				throw new CreationException("File \"" + file.path + "\": " +
								"Wrong size: " + size + ". Size of memory blocks is " +
								n + " bytes.");
			}
			else {
				tempFile.read(buf16, 0);
				buf16.rewind();
				this.gaps = buf16.getLong();
				if (gaps < 0) {
					throw new CreationException("File \"" + file.path + "\": " +
																"Number of gaps is negative.");
				}
				this.root = buf16.getLong();
				if (root < 0 || root > nofBlocks()) {
					throw new CreationException("File \"" + file.path + "\": " +
										"Wrong value of root index: " + root +
										". Size of memory blocks is " + n +
										" bytes and size of file is " + size + " bytes.");
				}
			}
		} catch (FileIOException e) {
			throw new CreationException(e);
		}
	}
	
	/**
	 * Returns the size of the underlying file.
	 * 
	 * @return The size of the underlying file.
	 */
	final long size() {
		return size;
	}
	
	/**
	 * Converts the specified index of a memory block to a file position.
	 * 
	 * @param  index The index of the memory block.
	 * @return The file position.
	 */
	final long indexToPos(long index) {
		return index * n + start;
	}
	
	/**
	 * Converts the specified file position to an index of a memory block.
	 * 
	 * @param  pos The file position.
	 * @return The index of the memory block.
	 */
	final long posToIndex(long pos) {
		return (pos - start) / n;
	}
	
	/**
	 * Returns the number of memory blocks in the FL file space.
	 * To get the number of <em>allocated</em> memory blocks in the FL file
	 * space subtract the number of deallocated memory blocks, returned by the
	 * {@code nofGaps} method, from the value returned by this method.
	 *         
	 * @return The number of memory blocks.
	 */
	final long nofBlocks() {
		return (size - start) / n;
	}
	
	/**
	 * Returns the number of gaps (deallocated memory blocks) in the FL file
	 * space.
	 * 
	 * @return The number of gaps.
	 */
	final long nofGaps() {
		return gaps;
	}
	
	/**
	 * Returns the indices of the memory blocks that are gaps sorted in
	 * ascending order.
	 * 
	 * @return The gaps sorted in ascending order, never {@code null}.
	 *         The array has length zero if and only if there are no gaps.
	 * 
	 * @throws ImplementationRestrictionException If the number of gaps in the
	 *         file space is greater than {@code Integer.MAX_VALUE}.
	 * @throws FileIOException If an I/O error occurs.
	 */
	final long[] gaps() throws ImplementationRestrictionException,
																					FileIOException {
		if (gaps > Integer.MAX_VALUE) {
			throw new ImplementationRestrictionException("File \"" + file.path +
													"\": " + "Too many gaps: " + gaps + ".");
		}
		final long[] gapsArr = new long[(int) gaps];
		
		file.open();
		try {
			// Read from chain of gaps.
			int i = 0;
			long gap = root;
			while (i < gaps) {
				gapsArr[i++] = gap;
				gap = readNextGap(indexToPos(gap));
			}
		} finally {
			file.close();
		}
		
		// Sort the gaps.
		Arrays.sort(gapsArr);
		
		return gapsArr;
	}
	
	/**
	 * Writes the index of the next gap to the gap that starts at the specified
	 * position and sets the first bit of this gap to 1.
	 * <p>
	 * Ensure that the file given to the constructor is {@linkplain
	 * FileIO#open() open}.
	 * 
	 * @param  next The index of the memory block which is the next gap in the
	 *         chain of gaps.
	 *         It is assumed that {@code next} is less than or equal to
	 *         {@code maxGaps}.
	 * @param  pos The position of the gap.
	 *         The index is written starting at this position.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void writeNextGap(long next, long pos) throws FileIOException {
		// No recording of before data, since it is assumed that before data,
		// including this place here, was already recorded prior to invoking the
		// deallocate method.
		
		// Precondition: index <= maxGaps
		Utils.unsToBytes(next, 8, arr);
		// Set first bit to 1.
		arr[0] = (byte) (arr[0] | 0x80);
		buf8.rewind();
		file.write(buf8, pos);
	}
	
	/**
	 * Reads the index of the next gap from the gap that starts at the specified
	 * position, taking care about the fact, that the first bit of this gap is
	 * set to 1.
	 * <p>
	 * Ensure that the file given to the constructor is {@linkplain
	 * FileIO#open() open}.
	 * 
	 * @param  pos The position of the gap.
	 *         This is the position where the index is read from.
	 *         
	 * @return The index of the next gap.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final long readNextGap(long pos) throws FileIOException {
		buf8.rewind();
		file.read(buf8, pos);
		// Set first bit to 0.
		arr[0] = (byte) (arr[0] & 0x7F);
		return Utils.unsFromBytes(arr, 8);
	}
	
	/**
	 * Sets the {@link #size} to the specified value.
	 * 
	 * @param value The value.
	 */
	private final void setSize(long value) {
		fssTracker.reportOldState(this, new FLFileSpaceState(size, gaps, root));
		size = value;
	}
	
	/**
	 * Sets the {@link #gaps} property to the specified value.
	 * 
	 * @param value The value.
	 */
	private final void setGaps(long value) {
		fssTracker.reportOldState(this, new FLFileSpaceState(size, gaps, root));
		gaps = value;
	}
	
	/**
	 * Sets the {@link #root} to the specified value.
	 * 
	 * @param value The value.
	 */
	private final void setRoot(long value) {
		fssTracker.reportOldState(this, new FLFileSpaceState(size, gaps, root));
		root = value;
	}
	
	/**
	 * Returns the position of an allocated memory block.
	 * The returned value may be equal to or greater than the size of the file.
	 * <p>
	 * Ensure that the file given to the constructor is {@linkplain
	 * FileIO#open() open}.
	 * 
	 * @param  unit The unit, may be {@code null}.
	 * 
	 * @return The position of the allocated memory block.
	 * 
	 * @throws ImplementationRestrictionException If the maximum capacity of
	 *         the file space is reached.
	 *         This exception happens if and only if the chain of gaps is empty
	 *         and the size of the file space would exceed {@link Long#MAX_VALUE}
	 *         if a new memory block was allocated.
	 * @throws UnitBrokenException If recording before data fails.
	 * @throws FileIOException If an I/O error occurs.
	 */
	final long allocate(IUnit unit) throws ImplementationRestrictionException,
														UnitBrokenException, FileIOException {
		final long pos = indexToPos(root);
		if (pos < size) {
			// The chain of gaps has at least one element.
			setRoot(readNextGap(pos));
			// Record before data.
			if (unit != null) {
				// arr filled when readNextGap(pos, file) was executed.
				// Set first bit to 1.
				arr[0] = (byte) (arr[0] | 0x80);
				unit.record(file, pos, arr);
			}
			// We just reused the file space of a gap.
			setGaps(gaps - 1);
		}
		else {
			// pos == size. The chain of gaps is empty, root equals the number
			// of allocated memory blocks.
			setSize(size + n);
			if (size < 0) {
				// size > Long.MAX_VALUE
				throw new ImplementationRestrictionException("File \"" +
						file.path + "\": Maximum capacity of file space exceeded.");
			}
			// Record before data.
			if (unit != null) {
				unit.record(file, pos, new byte[0]);
			}
			// The number of allocated memory blocks has increased by one.
			setRoot(root + 1);
		}
		return pos;
	}
	
	/**
	 * Deallocates a previously allocated memory block.
	 * The first bit of the memory block is set to 1.
	 * <p>
	 * Ensure that the file given to the constructor is {@linkplain
	 * FileIO#open() open}.
	 * 
	 * @param  pos The position of the memory block.
	 *         The value <em>must</em> be equal to the value returned by a
	 *         previous call to the {@link #allocate} method.
	 *         
	 * @throws IllegalArgumentException If the value of {@code pos} is illegal.
	 *         This exception never happens if {@code pos} is equal to the value
	 *         returned by a previous call to the {@link #allocate} method.
	 * @throws FileIOException If an I/O error occurs.
	 */
	final void deallocate(long pos) throws IllegalArgumentException,
																					FileIOException {
		// It is assumed that the before data spanning all n bytes has already
		// been recorded.
		if (pos < start || size <= pos) {
			throw new IllegalArgumentException("File \"" + file.path +
														"\": Illegal position: " + pos + ".");
		}
		// This memory block should now point to the current root gap. Note that,
		// as a side effect, the writeNextGap method sets the first bit of this
		// memory block to 1.
		writeNextGap(root, pos);
		// This memory block becomes the first gap of the chain of gaps, hence,
		// the new root gap.
		setRoot(posToIndex(pos));
		// We just got a new gap.
		setGaps(gaps + 1);
	}
	
	/**
	 * Sets the state of the FL file space equal to the state of an FL file
	 * space with no gaps and a size equal to the specified size.
	 * 
	 * @param  newSize The new size of the FL file space.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	final void reset(long newSize) throws FileIOException {
		size = newSize;
		gaps = 0;
		root = nofBlocks();
		writeState();
	}
	
	/**
	 * {@linkplain #reset Resets} the FL file space to its starting size and
	 * truncates the underlying file accordingly.
	 * <p>
	 * All changes to the underlying file are forced.
	 * <p>
	 * Ensure that the file given to the constructor is {@linkplain
	 * FileIO#open() open}.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	final void clearAndTruncate() throws FileIOException {
		if (size > start) {
			reset(start);
			file.truncate(start);
			file.force(true);
		}
	}
	
	/**
	 * Computes the position of the next memory block being a gap starting from
	 * the specified position.
	 * <p>
	 * As a side effect, this method increments the {@link #gaps} property by
	 * one if and only if there exists a next gap.
	 * <p>
	 * Ensure that the file given to the constructor is {@linkplain
	 * FileIO#open() open}.
	 * 
	 * @param  pos The starting position, must be greater than or equal to the
	 *         value of the {@link #start} property and less than or equal to
	 *         the value of the {@link #size} property.
	 *         If the value is less than the value of the {@code size} property
	 *         then it must be equal to the starting position of a memory block.
	 * @param  buf The buffer to be reused.
	 * @param  bytes The byte array of the specified buffer.
	 * 
	 * @return The position of the next memory block being gap.
	 *         This value is equal to the {@code pos} argument if {@code pos} is
	 *         equal to the starting position of a memory block being a gap.
	 *         If there is no next memory block being a gap then the value is
	 *         equal to the value of the {@code size} property.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final long nextGap(long pos, ByteBuffer buf, byte[] bytes) throws
																					FileIOException {
		final long oldGaps = gaps;
		while (pos < size && oldGaps == gaps) {
			buf.rewind();
			file.read(buf, pos);
			if (bytes[0] < 0)
				gaps++;
			else {
				pos += n;
			}
		}
		// pos >= size || pos equals starting position of a gap.
		
		return pos;
	}
	
	/**
	 * Rebuilds the chain of gaps.
	 * <p>
	 * Ensure that the file given to the constructor is {@linkplain
	 * FileIO#open() open}.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	final void rebuildChainOfGaps() throws FileIOException {
		final ByteBuffer buf = ByteBuffer.allocate(1);
		final byte[] bytes = buf.array();
		
		gaps = 0;
		long pos = nextGap(start, buf, bytes);
		root = posToIndex(pos);
		
		while (pos < size) {
			final long gapPos = pos;
			pos = nextGap(pos + n, buf, bytes);
			writeNextGap(posToIndex(pos), gapPos);
		}
		
		// Save gaps and root.
		writeState();
	}

	@Override
	public final void writeState() throws FileIOException {
		buf16.rewind();
		buf16.putLong(gaps);
		buf16.putLong(root);
		buf16.rewind();
		file.open();
		try {
			file.write(buf16, 0);
		} finally {
			file.close();
		}
	}

	@Override
	public final void adoptState(IFileSpaceState state) {
		size = ((FLFileSpaceState) state).size;
		gaps = ((FLFileSpaceState) state).gaps;
		root = ((FLFileSpaceState) state).root;
	}
}