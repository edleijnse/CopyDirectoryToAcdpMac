/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.nio.ByteBuffer;

import acdp.internal.Buffer;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;
import acdp.internal.store.Bag;

/**
 * A streamer reading its data from a file and applying a buffer.
 * The file streamer ensures that only large amounts of data instead of many
 * small pieces are read from the file in order to increase read performance.
 * 
 * @author Beat Hoermann
 */
final class FileStreamer implements IStreamer {
	/**
	 * A buffer loader loads the internal buffer of the file streamer.
	 *
	 * @author Beat Hoermann
	 */
	private static interface IBufferLoader {
		/**
		 * Loads the internal buffer of the file streamer.
		 * 
		 * @param  buf The byte buffer, not allowed to be {@code null}.
		 * 
		 * @throws FileIOException If the end of the file is reached before the
		 *         byte buffer is completely filled or if an I/O error occurs
		 *         while reading the file.
		 */
		void load(ByteBuffer buf) throws FileIOException;
	}
	
	/**
	 * This buffer loader sets the file channel's position once at the beginning.
	 * The {@link #load} method relies on the file channel's position not to be
	 * changed from outside of this class.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class BufferLoader implements IBufferLoader {
		private final FileIO file;
		
		/**
		 * The constructor.
		 * Sets the file channel's position of the specified {@code FileIO} object
		 * to the specified position.
		 * 
		 * @param  file {@code FileIO} object, not allowed to be {@code null}.
		 * @param  pos The position within the file where to start loading the
		 *         data from.
		 * 
		 * @throws IllegalArgumentException If {@code pos} is negative.
		 * @throws FileIOException If an I/O error occurs while setting the
		 *         position of the file channel.
		 */
		BufferLoader(FileIO file, long pos) throws IllegalArgumentException,
																					FileIOException {
			this.file = file;
			file.position(pos);
		}
		
		@Override
		public final void load(ByteBuffer buf) throws FileIOException {
			file.read(buf);
		}
	}
	
	/**
	 * This buffer loader does not set the file channel's position.
	 * The {@link #load} method does not rely on the file channel's position.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class BufferLoader_ implements IBufferLoader {
		private final FileIO file;
		/**
		 * The current position.
		 */
		private long pos;
		
		/**
		 * The constructor.
		 * 
		 * @param  file The file, not allowed to be {@code null}.
		 * @param  pos The position within the file where to start loading the
		 *         data from.
		 * 
		 * @throws IllegalArgumentException If {@code pos} is negative.
		 */
		BufferLoader_(FileIO file, long pos) throws IllegalArgumentException {
			this.file = file;
			if (pos < 0) {
				throw new IllegalArgumentException("File position is negatve: " +
																							pos + ".");
			}
			this.pos = pos;
		}
		
		@Override
		public final void load(ByteBuffer buf) throws FileIOException {
			pos = file.read_(buf, pos);
		}
	}
	
	/**
	 * The buffer loader to load the bytes from.
	 */
	private final IBufferLoader bufferLoader;
	/**
	 * The buffer with a maximum limit of {@code maxLimit} bytes.
	 */
	private final ByteBuffer buf;
	/**
	 * Just a pointer to the buffer's internal byte array.
	 */
	private final byte[] bufArr;
	/**
	 * The number of bytes left that this streamer must be able to return.
	 */
	private long left;
	
	/**
	 * Constructs a file streamer.
	 * <p>
	 * Uses the specified buffer to reduce the frequency of file reads.
	 * <p>
	 * We distinguish two cases depending on the value of the {@code channelPos}
	 * parameter.
	 * 
	 * <h1>1. Case: {@code channelPos} is equal to {@code true}</h1>
	 * In this case the file streamer relies on the current position of the file
	 * channel kept in the specified {@code FileIO} instance.
	 * This method sets the position of the file channel to the value of the
	 * {@code pos} parameter and the {@code pull} method increments the position
	 * of the file channel accordingly each time it is invoked.
	 * The client must therefore ensure that the position of the file channel is
	 * not changed throughout the lifetime of this streamer.
	 * 
	 * <h1>2. Case: {@code channelPos} is equal to {@code false}</h1>
	 * In this case the current position of the file channel kept in the
	 * specified {@code FileIO} instance is left untouched.
	 * <p>
	 * In any case, the buffer's limit is set equal to either the value of the
	 * {@code length} parameter or the value returned by the {@link
	 * Buffer#maxCap} method, whichever is less.
	 * <p>
	 * The client must ensure that the buffer remains untouched throughout the
	 * lifetime of this streamer.
	 * The {@code pull} method changes the buffer's position each time it is
	 * invoked.
	 * It may also change the limit of the buffer.
	 * 
	 * @param  file The file, not allowed to be {@code null}.
	 * @param  pos The position within the file where to start streaming.
	 * @param  length The total number of bytes this streamer has to return,
	 *         must be greater than zero.
	 * @param  buffer The buffer, not allowed to be {@code null}.
	 * @param  channelPos The information whether the current position of the
	 *         specified file can be used.
	 *         If this value is equal to {@code true} then this method makes use
	 *         of the current position of the file channel kept by the specified
	 *         {@code FileIO} instance.
	 * 
	 * @throws IllegalArgumentException If {@code pos} is less than zero or if
	 *         {@code length} is less than or equal to zero.
	 * @throws FileIOException If an I/O error occurs while setting the position
	 *         of the file channel.
	 *         This exception never happens if {@code channelPos} is {@code
	 *         false}.
	 */
	FileStreamer(FileIO file, long pos, long length, Buffer buffer,
																		boolean channelPos) throws
												IllegalArgumentException, FileIOException {
		if (length <= 0) {
			throw new IllegalArgumentException("Invalid length: " + length);
		}
		// length > 0
		
		this.bufferLoader = channelPos ? new BufferLoader(file, pos) :
																new BufferLoader_(file, pos);
		this.buf = buffer.buf(length);
		
		this.bufArr = buf.array();
		this.left = length;
		buf.position(buf.limit());
		// left > 0 && !buf.hasRemaining() && buf.limit > 0
	}
	
	/**
	 * Loads the buffer.
	 * <p>
	 * For the last block of data, the number of bytes actually needed to be
	 * loaded from the data source is likely to be smaller than the limit of
	 * the buffer.
	 * 
	 * @throws FileIOException If the end of the file is reached before the byte
	 *         buffer is completely filled or if an I/O error occurs while
	 *         reading the file.
	 */
	private final void loadBuffer() throws FileIOException {
		// left >= buf.remaining() == 0
		if (left > 0) {
			if (left < buf.limit()) {
				buf.limit((int) left);
			}
			buf.rewind();
			bufferLoader.load(buf);
			buf.rewind();
			left -= buf.remaining();
		}
		else {
			buf.rewind();
		}
		// buf.remaining() > 0
	}

	@Override
	public final void pull(int len, Bag bag) throws NullPointerException,
																					FileIOException {
		if (!buf.hasRemaining()) {
			loadBuffer();
		}
		
		// buf.remaining() > 0
		final int bufLeft = buf.remaining();
		final int bufPos = buf.position();
		if (len <= bufLeft) {
			// The length of the requested data is less than or equal to the
			// number of bytes left in the buffer.
			// Prepare result.
			bag.bytes = bufArr;
			bag.offset = bufPos;
			// Forward position of buffer.
			buf.position(bufPos + len);
		}
		else {
			// The length of the requested data is greater than the number of
			// bytes left in the buffer.
			final byte[] data = new byte[len];
			// Prepare result.
			bag.bytes = data;
			bag.offset = 0;
			// Copy first portion from the buffer.
			System.arraycopy(bufArr, bufPos, data, 0, bufLeft);
			int dataPos = bufLeft;
			int dataLeft = len - bufLeft;
			do {
				// dataLeft > 0 && must load buffer
				loadBuffer();
				// buf.remaining() > 0
				int m = Math.min(dataLeft, buf.remaining());
				// Copy next portion from the buffer.
				System.arraycopy(bufArr, 0, data, dataPos, m);
				buf.position(buf.position() + m);
				dataPos += m;
				dataLeft -= m;
			} while (dataLeft > 0);
		}
		// buf.remaining() >= 0;
	}
	
	/**
	 * Returns the internal buffer's limit.
	 * 
	 * @return The internal buffer's limit.
	 */
	public final int bufLimit() {
		return buf.limit();
	}
}
