/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import java.lang.AutoCloseable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;

import acdp.exceptions.ShutdownException;
import acdp.internal.misc.FileChannelProvider;

/**
 * Keeps together a {@linkplain FileChannel file channel} along with its
 * {@linkplain Path file path} and centralizes most of the read and write
 * methods used in ACDP where file channels are involved.
 * The methods below consistently report the file path via the {@code
 * FileIOException} class if the underlying file channel's methods throw an
 * {@code IOException}.
 * <p>
 * An instance of this class can be created in two ways:
 * 
 * <ol>
 *    <li>Invoke the {@link #FileIO(Path, OpenOption...)} constructor with a
 *    particular file path and a particular set of open options.
 *    This constructor opens the file channel for that file path.
 *    </li>
 *    <li>Invoke the {@link #FileIO(Path, FileChannelProvider)} constructor
 *    with a particular instance of the {@link FileChannelProvider} class and a
 *    particular file path if opening and closing the file channel should be
 *    delegated to the file channel provider.</li>
 * </ol>
 * <p>
 * Don't forget to invoke the {@link #close} method if you are done with your
 * {@code FileIO} instance.
 * Since this class implements the {@link AutoCloseable} interface, you may
 * want to apply the <em>try-with-resources</em> statement like this:
 * 
 * <pre>
 * try (FileIO file = new FileIO(path, READ, WRITE)) {
 *    <em>do something with</em> file
 * }</pre>
 * 
 * where {@code path} denotes some instance of the {@link Path} class.
 * <p>
 * Note that the second constructor creates a <em>closed</em> {@code FileIO}
 * instance which must first be opened before it can be used.
 * This leads to the following usage-pattern:
 * 
 * <pre>
 * file.open();
 * try {
 *    <em>do something with</em> file
 * } finally {
 *    file.close();
 * }</pre>
 * 
 * where {@code file} denotes a {@code FileIO} instance created with the second
 * constructor.
 * <p>
 * Since this pattern can be executed zero, one or more than one times, you can
 * apply it in cases where you are not sure if the {@code FileIO} instance is
 * ever going to be used or if you want to <em>reuse</em> the {@code FileIO}
 * instance after it was closed.
 * Note, however, that this pattern cannot be applied if the {@code FileIO}
 * instance is created with the first constructor.
 *
 * @author Beat Hoermann
 */
public final class FileIO implements AutoCloseable {
	/**
	 * The file channel, may be {@code null}.
	 */
	private FileChannel fc;
	
	/**
	 * The path of the file channel, may be {@code null}.
	 */
	public final Path path;
	
	/**
	 * The file channel provider if this instance was created by invoking the
	 * {@link #FileIO(Path, FileChannelProvider)} constructor or {@code null} if
	 * this instance was created by invoking the {@link #FileIO(Path,
	 * OpenOption...)} constructor.
	 */
	private final FileChannelProvider fcProvider;
	
	/**
	 * The constructor opening the file channel.
	 * Invokes the {@link FileChannel#open} method with the specified path and
	 * the specified options to get an instance of a file channel.
	 * <p>
	 * Since the {@code FileIO} instance will already be open, invoking the
	 * {@link #open} method does not make sense.
	 * The {@link #open} method throws a {@code NullPointerException} if you
	 * invoke it with a {@code FileIO} instance created by this constructor.
	 * 
	 * @param  path The path of the file channel, not allowed to be {@code
	 *         null}.
	 * @param  options Options specifying how the file is opened.
	 *        
	 * @throws IllegalArgumentException If the set of open options contains an
	 *         invalid combination.
    * @throws UnsupportedOperationException If the {@code path} is associated
    *         with a file system provider that does not support creating file
    *         channels, or an unsupported open option is specified.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public FileIO(Path path, OpenOption... options) throws
								IllegalArgumentException, UnsupportedOperationException,
																					FileIOException {
		this.fcProvider = null;
		try {
			this.fc = FileChannel.open(path, options);
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
		this.path = path;
	}
	
	/**
	 * This constructor creates a <em>closed</em> {@code FileIO} instance which
	 * has to be {@linkplain #open opened} before it can be used.
	 * Another difference to the {@link #FileIO(Path, OpenOption...)} constructor
	 * is that an instance of the file channel is obtained from the specified
	 * file channel provider rather than by creating it with the {@code
	 * FileChannel.open} method.
	 * <p>
	 * Since the file channel won't be requested from the specified file channel
	 * provider until the {@link #open} method is invoked, the associated file
	 * won't be opened by invoking this constructor.
	 * <p>
	 * Use this constructor if you are not sure if the {@code FileIO} instance
	 * is ever going to be used or if you plan to <em>reuse</em> it by
	 * opening and closing the same {@code FileIO} instance several times.
	 * 
	 * @param  path The path of the file channel, may be {@code null}.
	 * @param  fcProvider The file channel provider, not allowed to be {@code
	 *         null}.
	 */
	public FileIO(Path path, FileChannelProvider fcProvider) {
		this.fc = null;
		this.path = path;
		this.fcProvider = fcProvider;
	}
	
	/**
	 * Opens the {@code FileIO} instance by requesting the file channel from the
	 * file channel provider passed to this instance via the {@link #FileIO(Path,
	 * FileChannelProvider)} constructor.
	 * <p>
	 * The current position of the {@code FileIO} instance as returned by the
	 * {@link #position()} method is equal to zero.
	 * <p>
	 * Do not invoke this method if this {@code FileIO} instance was created
	 * with the {@link #FileIO(Path, OpenOption...)} constructor.
	 * 
	 * @throws NullPointerException If this {@code FileIO} instance was created
	 *         with the {@link #FileIO(Path, OpenOption...)} constructor.
	 * @throws ShutdownException If the file channel provider is shut down.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void open() throws NullPointerException, ShutdownException,
																					FileIOException {
		fc = fcProvider.request(path);
	}
	
	/**
	 * Invokes {@link FileChannel#tryLock() fc.tryLock} with {@code fc} denoting
	 * the file channel.
	 *
	 * @return A lock object representing the newly-acquired lock, or {@code
	 *         null} if the lock could not be acquired because another program
	 *         holds an overlapping lock.
	 * 
	 * @throws OverlappingFileLockException If a lock that overlaps the
	 *         requested region is already held by this Java virtual machine, or
	 *         if another thread is already blocked in this method and is
	 *         attempting to lock an overlapping region.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final FileLock tryLock() throws OverlappingFileLockException,
																					FileIOException {
		try {
			return fc.tryLock();
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
	}
	
	/**
	 * Invokes {@link FileChannel#tryLock(long, long, boolean)
	 * fc.tryLock(position, size, shared)} with {@code fc} denoting the file
	 * channel.
	 * 
	 * @param  position The position at which the locked region is to start; must
	 *         benon-negative.
	 * @param  size The size of the locked region; must be non-negative, and the
	 *         sum {@code position} + {@code size} must be
	 *         non-negative.
	 * @param  shared {@code true} to request a shared lock, {@code false} to
	 *         request an exclusive lock
	 *
	 * @return A lock object representing the newly-acquired lock, or {@code
	 *         null} if the lock could not be acquired because another program
	 *         holds an overlapping lock.
	 * 
	 * @throws OverlappingFileLockException If a lock that overlaps the
	 *         requested region is already held by this Java virtual machine, or
	 *         if another thread is already blocked in this method and is
	 *         attempting to lock an overlapping region.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final FileLock tryLock(long position, long size,
			boolean shared) throws OverlappingFileLockException, FileIOException {
		try {
			return fc.tryLock(position, size, shared);
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
	}
	
	/**
	 * Returns the size of the file channel.
	 * 
	 * @return The size of the file channel.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final long size() throws FileIOException {
		try {
			return fc.size();
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
	}
	
	/**
	 * Invokes {@link FileChannel#position() fc.position()} with {@code
	 * fc} denoting the file channel.
	 *
	 * @return The position of the file channel.

	 * @throws FileIOException If an I/O error occurs.
	 */
	public final long position() throws FileIOException {
		try {
			return fc.position();
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
	}
	
	/**
	 * Invokes {@link FileChannel#position(long) fc.position(pos)} with {@code
	 * fc} denoting the file channel.
	 *
	 * @param  pos The new position of the file channel.
	 * 
	 * @throws IllegalArgumentException If {@code pos} is negative.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void position(long pos) throws IllegalArgumentException,
																					FileIOException {
		try {
			fc.position(pos);
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
	}
	
	/**
	 * Invokes {@link FileChannel#truncate(long) fc.truncate(size)} with {@code
	 * fc} denoting the file channel.
	 *
	 * @param  size The new size.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void truncate(long size) throws FileIOException {
		try {
			fc.truncate(size);
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
	}
	
	/**
	 * Invokes {@link FileChannel#force(boolean) fc.force(metaData)} with {@code
	 * fc} denoting the file channel.
	 *
	 * @param  metaData If {@code true} then this method is required to force
	 *         changes to both the file's content and metadata to be written to
	 *         storage; otherwise, it need only force content changes to be
	 *         written.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void force(boolean metaData) throws FileIOException {
		try {
			fc.force(metaData);
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
	}
	
	/**
	 * Writes the content of the buffer to the file channel starting at the
	 * file channel's current position.
	 * <p>
	 * Invokes {@link FileChannel#write(ByteBuffer) fc.write(buf)} with {@code
	 * fc} denoting the file channel. 
	 * 
	 * @param  buf The byte buffer.
	 * 
	 * @throws NullPointerException If the byte buffer is {@code null}.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void write(ByteBuffer buf) throws NullPointerException,
																					FileIOException {
		try {
			fc.write(buf);
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
	}
	
	/**
	 * Writes the content of the buffer to the file channel starting at the
	 * specified file position.
	 * <p>
	 * Invokes {@link FileChannel#write(ByteBuffer, long) fc.write(buf, pos)}
	 * with {@code fc} denoting the file channel. 
	 * 
	 * @param  buf The byte buffer.
	 * @param  pos The file position.
	 * 
	 * @throws NullPointerException If the byte buffer is {@code null}.
	 * @throws IllegalArgumentException If {@code pos} is negative.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void write(ByteBuffer buf, long pos) throws
					NullPointerException, IllegalArgumentException, FileIOException {
		try {
			fc.write(buf, pos);
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
	}
	
	/**
	 * Reads {@code n} bytes from the file channel into the specified buffer
	 * starting at the file channel's current position where {@code n} denotes
	 * the number of bytes between the buffer's current position and its limit.
	 * 
	 * @param  buf The byte buffer.
	 * 
	 * @throws NullPointerException If the byte buffer is {@code null}.
	 * @throws FileIOException If the end of the file is reached before the byte
	 *         buffer is completely filled or if an I/O error occurs while
	 *         reading the file.
	 */
	public final void read(ByteBuffer buf) throws
													NullPointerException, FileIOException {
		do {
			int n;
			try {
				n = fc.read(buf);
			} catch (IOException e) {
				throw new FileIOException(path, e);
			}
			if (n < 0) {
				throw new FileIOException(path, null, true);
			}
		} while (buf.hasRemaining());
	}
	
	/**
	 * Reads {@code n} bytes from the file channel into the specified buffer
	 * starting at the specified position where {@code n} denotes the number
	 * of bytes between the buffer's current position and its limit.
	 * 
	 * @param  buf The byte buffer.
	 * @param  pos The file position.
	 * 
	 * @throws NullPointerException If the byte buffer is {@code null}.
	 * @throws IllegalArgumentException If {@code pos} is negative.
	 * @throws FileIOException If the end of the file is reached before the byte
	 *         buffer is completely filled or if an I/O error occurs while
	 *         reading the file.
	 */
	public final void read(ByteBuffer buf, long pos) throws
					NullPointerException, IllegalArgumentException, FileIOException {
		try {
			fc.position(pos);
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
		read(buf);
	}
	
	/**
	 * Reads {@code n} bytes from the file channel into the specified buffer
	 * starting at the specified file position where {@code n} denotes the number
	 * of bytes between the buffer's current position and its limit.
	 * <p>
	 * This method does not change the file channel's current position.
	 * 
	 * @param  buf The byte buffer.
	 * @param  pos The file position.
	 * 
	 * @return The new file position.
	 * 
	 * @throws NullPointerException If the byte buffer is {@code null}.
	 * @throws IllegalArgumentException If {@code pos} is negative.
	 * @throws FileIOException If the end of the file is reached before the byte
	 *         buffer is completely filled or if an I/O error occurs while
	 *         reading the file.
	 */
	public final long read_(ByteBuffer buf, long pos) throws
					NullPointerException, IllegalArgumentException, FileIOException {
		do {
			int n;
			try {
				n = fc.read(buf, pos);
			} catch (IOException e) {
				throw new FileIOException(path, e);
			}
			if (n < 0)
				throw new FileIOException(path, null, true);
			else {
				pos += n;
			}
		} while (buf.hasRemaining());
		return pos;
	}
	
	/**
	 * Copies the data block of the specified size from the specified position
	 * to the current position.
	 * <p>
	 * The current position is incremented by the value of {@code n}.
	 * <p>
	 * The size of the file must be greater than or equal to {@code pos} +
	 * {@code n}.
	 * 
	 * @param  pos The position of the data block to be moved, must be greater
	 *         than or equal to zero.
	 * @param  n The size of the data block to be moved, must be greater than or
	 *         equal to zero.
	 * @param  buffer The buffer to be used, not allowed to be {@code null}.
	 * 
	 * @throws NullPointerException If the target {@code FileIO} instance is
	 *         {@code null}.
	 * @throws IllegalArgumentException If the preconditions on the parameters
	 *         {@code pos} and {@code n} do not hold.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void copyTo(long pos, long n, Buffer buffer) throws
					NullPointerException, IllegalArgumentException, FileIOException {
		// Experiments on my desktop computer (Windows 7, SSD) revealed that the
		// "FileChannel.transferTo" method performs not so well if a small amount
		// of data (< 20000 bytes) is involved. Furthermore, the
		// "FileChannel.transferTo" and the "FileChannel.transferFrom" methods
		// have problems with overlapping regions.
		long filePos = position();
		if (filePos != pos && n > 0) {
			final ByteBuffer buf = buffer.buf(n);
			int limit = buf.limit();
			// n >= limit
			
			if (n == limit) {
				read_(buf, pos);
				buf.rewind();
				write(buf);
			}
			else if (filePos < pos || pos + n  <= filePos) {
				// No critical overlapping && n > 0 && n > limit
				do {
					if (n < limit) {
						buf.limit((int) n);
					}
					buf.rewind();
					read_(buf, pos);
					buf.rewind();
					write(buf);
					pos += limit;
					n -= limit;
				} while (n > 0);
			}
			else {
				// Critical overlapping && n > 0 && n > limit
				pos += n;
				filePos += n;
				do {
					if (n < limit) {
						limit = (int) n;
						buf.limit(limit);
					}
					pos -= limit;
					filePos -= limit;
					buf.rewind();
					read_(buf, pos);
					buf.rewind();
					write(buf, filePos);
					n -= limit;
				} while (n > 0);
				position(filePos + n);
			}
		}
	}
	
	/**
	 * Copies the content of this file to the specified output stream.
	 * <p>
	 * The output stream is not closed.
	 * <p>
	 * This method does not change the file channel's current position.
	 * 
	 * @param  os The output stream, not allowed to be {@code null}.
	 * @param  buffer The buffer to be used, not allowed to be {@code null}.
	 * 
	 * @throws NullPointerException If one of the arguments is {@code null}.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void copyFile(OutputStream os, Buffer buffer) throws
													NullPointerException, FileIOException {
		try {
			long left = fc.size();
			if (left > 0) {
				final ByteBuffer buf = buffer.buf(left);
				int limit = buf.limit();
				long pos = 0;
				do {
					pos = read_(buf, pos);
					os.write(buf.array(), 0, limit);
				
					left -= limit;
					
					if (left > 0) {
						if (left < limit) {
							limit = (int) left;
							buf.limit(limit);
						}
						buf.rewind();
					}
				} while (left > 0);
		
				os.flush();
			}
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
	}

   /**
    * Closes this {@code FileIO} instance.
    * If this {@code FileIO} instance was created by invoking the {@link
    * #FileIO(Path, OpenOption...)} constructor then this method closes the
    * file channel.
    * Otherwise, this method invokes the {@link FileChannelProvider#release}
    * method of the file channel provider for the file path.
    * Note that this does not necessarily close the file channel.
    *
    * @throws FileIOException If an I/O error occurs.
    */
	@Override
	public final void close() throws FileIOException {
		if (fcProvider == null) {
			try {
				fc.close();
			} catch (IOException e) {
				throw new FileIOException(path, e);
			}
		}
		else {
			fcProvider.release(path);
		}
	}
	
	/**
	 * Invokes the {@link FileChannelProvider#forceClose(Path)} method to close
	 * the idle file channel.
	 * 
	 * @throws NullPointerException If this {@code FileIO} instance was created
	 *         with the {@link #FileIO(Path, OpenOption...)} constructor.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void closeFileChannel() throws NullPointerException,
																					FileIOException {
		fcProvider.forceClose(path);
	}
	
	/**
	 * Invokes {@link Files#copy(Path, Path, CopyOption...)
	 * Files.copy(path, target, options)} with {@code path} denoting the path of
	 * this {@code FileIO} instance.
	 * 
	 * @param  target The path to the target file (may be associated with a
	 *         different provider to the source path).
	 * @param  options The options specifying how the copy should be done.
	 * 
	 * @return The path to the target file.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final Path copyFile(Path target, CopyOption... options) throws
																					FileIOException {
		try {
			return Files.copy(this.path, target, options);
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
	}
	
	/**
	 * Invokes {@link Files#move Files.move(source, target, options)} with
	 * {@code source} denoting the path of this {@code FileIO} instance and
	 * {@code target} denoting the path of the {@code target} argument.
	 * 
	 * @param  target The target file.
    * @param  options The options specifying how the move should be done.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void move(FileIO target, CopyOption... options) throws
																					FileIOException {
		try {
			Files.move(path, target.path, options);
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
	}
	
	/**
	 * Invokes {@link Files#delete Files.delete(path)} with {@code path}
	 * denoting the path of this {@code FileIO} instance.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void delete() throws FileIOException {
		try {
			Files.delete(path);
		} catch (IOException e) {
			throw new FileIOException(path, e);
		}
	}
	
	/**
	 * Returns the result of {@code Channels.newOutputStream(fc)} with {@code
	 * fc} denoting the file channel.
	 * 
	 * @return A new output stream.
	 */
	public final OutputStream getOutputStream() {
		return Channels.newOutputStream(fc);
	}
	
	/**
	 * Returns the result of {@code Channels.newInputStream(fc)} with {@code
	 * fc} denoting the file channel.
	 * 
	 * @return A new output stream.
	 */
	public final InputStream getInputStream() {
		return Channels.newInputStream(fc);
	}
	
	@Override
	public final int hashCode() {
		return path.hashCode();
	}
}
