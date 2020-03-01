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

import acdp.exceptions.CreationException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.FileSpaceStateTracker;
import acdp.internal.IUnit;
import acdp.internal.FileSpaceStateTracker.IFileSpace;
import acdp.internal.FileSpaceStateTracker.IFileSpaceState;

/**
 * For a given file this class allocates and deallocates file space in blocks
 * of a variable length.
 * A client requests a memory block by invoking the {@link #allocate} method
 * which returns the position within the file where the memory block starts.
 * Once the memory block is no longer needed the client invokes the {@link
 * #deallocate} method.
 * (The {@code deallocate} method should also be called if the full length
 * of a memory block is no longer needed, see the method description.)
 * <p>
 * The position returned by the {@code allocate} method is equal to or even
 * larger than the size of the file.
 * (This class relies on the convention that a file grows if data is written
 * beyond the end of the file.)
 * <p>
 * The allocation strategy used by this class is very simple: New memory blocks
 * are always appended at the end of the underlying file.
 * <em>A deallocated memory block is never reused</em>.
 * However, the size of all deallocated memory blocks, hence, the number of
 * unused bytes within the underlying file is recorded at the beginning of the
 * underlying file.
 * Thus, this number, along with the size of the file, can be used to find out
 * if the underlying file should be compacted.
 *
 * @author Beat Hoermann
 */
final class VLFileSpace implements IFileSpace {
	
	/**
	 * The internal state of a VL file space.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class VLFileSpaceState implements IFileSpaceState {
		final long m;
		final long pos;
		
		/**
		 * The constructor.
		 * 
		 * @param m See {@link VLFileSpace#m}.
		 * @param pos See {@link VLFileSpace#pos}.
		 */
		VLFileSpaceState(long m, long pos) {
			this.m = m;
			this.pos = pos;
		}
	}
	
	/**
	 * The underlying file.
	 */
	private final FileIO file;
	
	/**
	 * The file space tracker, never <code>null</code>.
	 */
	private final FileSpaceStateTracker fssTracker;
	
	/**
	 * The position within the file where the first memory block starts.
	 */
	final int start = 8;
	
	/**
	 * A byte buffer with a capacity of 8 bytes.
	 */
	private final ByteBuffer buf8 = ByteBuffer.allocate(8);
	
	/**
	 * The number of deallocated bytes.
	 * This can be later used to find out if it is reasonable to compact the
	 * file space.
	 */
	private long m;
	
	/**
	 * The current position within the file where to allocate a new memory block.
	 */
	private long pos;
	
	/**
	 * Sets the property {@link #m} to the specified value.
	 * 
	 * @param value The value.
	 */
	private final void setM(long value) {
		fssTracker.reportOldState(this, new VLFileSpaceState(m, pos));
		m = value;
	}
	
	/**
	 * Sets the property {@link #pos} to the specified value.
	 * 
	 * @param value The value.
	 */
	private final void setPos(long value) {
		fssTracker.reportOldState(this, new VLFileSpaceState(m, pos));
		pos = value;
	}
	
	/**
	 * Constructs the file space manager for a given file.
	 * 
	 * @param  file The {@linkplain FileIO#FileIO(java.nio.file.Path,
	 *         acdp.internal.misc.FileChannelProvider) closed} underlying file.
	 * @param  fssTracker The file space state tracker.
	 *         If this value is equal to {@code null} then the file space manager
	 *         is constructed in read-only mode which means that the {@code
	 *         allocate} and {@code deallocate} methods cannot be used.
	 * 
	 * @throws CreationException If the size of the file is greater than the
	 *         value of the {@link #start} constant and the first {@code start}
	 *         bytes converted to a long integer represent a negative number or
	 *         if an I/O error occurs.
	 */
	VLFileSpace(FileIO file, FileSpaceStateTracker fssTracker) throws
																				CreationException {
		this.file = file;
		this.fssTracker = fssTracker;
		
		// Initialize m, pos.
		// We don't want to request the file channel from the file channel
		// provider at this stage.
		try (FileIO tempFile = new FileIO(file.path, fssTracker != null ?
					new OpenOption[] { READ, WRITE } : new OpenOption[] { READ })) {
			long size = tempFile.size();
			if (size < start) {
				// File considered empty.
				m = 0;
				if (fssTracker != null) {
					buf8.putLong(m);
					buf8.rewind();
					tempFile.write(buf8, 0);
				}
				pos = start;
			}
			else {
				tempFile.read(buf8, 0);
				buf8.rewind();
				m = buf8.getLong();
				if (m < 0) {
					throw new CreationException("File \"" + file.path + "\": " +
																				"Invalid format.");
				}
				pos = size;
			}
		} catch (FileIOException e) {
			throw new CreationException(e);
		}
	}
	
	/**
	 * Returns the number of allocated bytes.
	 * 
	 * @return The number of allocated bytes.
	 */
	final long allocated() {
		return pos - start - m;
	}
	
	/**
	 * Returns the number of deallocated bytes.
	 * 
	 * @return The number of deallocated bytes.
	 */
	final long deallocated() {
		return m;
	}
	
	/**
	 * Returns the position of an allocated memory block of the specified size.
	 * 
	 * @param  n The size of the new memory block, greater than or equal to zero.
	 * @param  unit The unit.
	 * 
	 * @return The position of the allocated memory block or 1 if {@code n} is
	 *         zero.
	 * 
	 * @throws IllegalArgumentException If {@code n} is less than zero.
	 * @throws UnitBrokenException If recording before data fails.
	 * @throws ImplementationRestrictionException If the specified size is too
	 *         large.
	 *         This exception happens if and only if the underlying file would
	 *         grow beyond {@link Long#MAX_VALUE} bytes.
	 */
	final long allocate(long n, IUnit unit) throws IllegalArgumentException,
							UnitBrokenException, ImplementationRestrictionException {
		if (n < 0)
			throw new IllegalArgumentException("File \"" + file.path + "\": " +
																			"Invalid size: " + n);
		else if (n == 0) {
			return 1;
		}
		// Record before data.
		if (unit != null) {
			unit.record(file, pos, new byte[0]);
		}
		long oldPos = pos;
		setPos(pos + n);
		if (pos < 0) {
			throw new ImplementationRestrictionException("File \"" + file.path +
														"\": Memory block too large: " + n);
		}
		return oldPos;
	}
	
	/**
	 * Deallocates the specified number of bytes from a previously allocated
	 * memory block.
	 * 
	 * @param  n The number of bytes to deallocate, must be greater than or equal
	 *         to zero.
	 *         This value is less than or equal to the value given to the 
	 *         {@link #allocate} method by a corresponding previous call.
	 *         If it is equal then the previously allocated memory block should
	 *         be considered being completely deallocated.
	 *         Otherwise, the memory block should be considered being shrinked
	 *         in its size.
	 * @param  unit The unit.
	 *        
	 * @throws IllegalArgumentException If {@code n} is less than zero.
	 */
	final void deallocate(long n, IUnit unit) throws IllegalArgumentException {
		if (n < 0)
			throw new IllegalArgumentException("File \"" + file.path + "\": " +
																			"Invalid size: " + n);
		else if (n > 0) {
			// We do not record before data. Instead explicitely remember the
			// file to force. (This is implicitely done when invoking the
			// unit.record method.)
			if (unit != null) {
				unit.addToForceList(file);
			}
			setM(m + n);
		}
	}
	
	/**
	 * Sets the state of the VL file space equal to the state of a VL file space
	 * with no deallocated memory blocks and a size equal to the specified size.
	 * 
	 * @param  newSize The new size of the VL file space.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	final void reset(long newSize) throws FileIOException {
		m = 0;
		pos = newSize;
		writeState();
	}
	
	/**
	 * {@linkplain #reset Resets} the VL file space to its starting size and
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
		if (pos > start) {
			reset(start);
			file.truncate(start);
			file.force(true);
		}
	}
	
	/**
	 * Corrects the number of deallocated bytes.
	 * <p>
	 * Ensure that the file given to the constructor is {@linkplain
	 * FileIO#open() open}.
	 * 
	 * @param  allocated The number of allocated bytes. 
	 * @throws FileIOException If an I/O error occurs.
	 */
	final void correctM(long allocated) throws FileIOException {
		m = pos - start - allocated;
		
		// Save m.
		writeState();
	}
	
	@Override
	public final void writeState() throws FileIOException {
		buf8.rewind();
		buf8.putLong(m);
		file.open();
		try {
			buf8.rewind();
			file.write(buf8, 0);
		} finally {
			file.close();
		}
	}

	@Override
	public final void adoptState(IFileSpaceState state) {
		m = ((VLFileSpaceState) state).m;
		pos = ((VLFileSpaceState) state).pos;
	}
}
