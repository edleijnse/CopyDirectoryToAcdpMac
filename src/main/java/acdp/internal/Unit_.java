/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.nio.file.StandardOpenOption.*;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import acdp.Unit;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CreationException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.FileSpaceStateTracker.FSSTrackerState;

/**
 * Implements a {@linkplain Unit unit}.
 * <p>
 * Since units only make sense if the database is writable, it is assumed that
 * the database is writable whenever an instance of this class is constructed.
 * <p>
 * A single instance of a unit is exclusively created by the
 * {@link SyncManager}.
 * <p>
 * Units are not allowed to be shared among different threads.
 * All public methods but the {@code record} method fail if they are executed
 * in a thread different from the thread in which the unit was issued by
 * the {@code SyncManager}.
 * (It is supposed that the only operations that invoke the {@code record}
 * method, namely write operations, fail prior to the first execution of the
 * {@code record} method if they are invoked in a foreign thread.)
 * <p>
 * All public methods may break the unit in the following situations:
 * <dl>
 *    <dt>record</dt>
 *    <dd>I/O error when writing the recorder file.</dd>
 *    <dt>commit</dt>
 *    <dd>I/O error when writing the states of the file spaces or I/O error
 *    when truncating the recorder file.</dd>
 *    <dt>close executing rollback</dt>
 *    <dd>I/O error when reading or truncating the recorder file or I/O error
 *    when reverting data in the file spaces.</dd>
 * </dl>
 *
 * @author Beat Hoermann
 */
final class Unit_ implements Unit, IUnit {
	/**
	 * A stack that stores the back position of nested units within the
	 * recorder file and the states of the {@link FileSpaceStateTracker}.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class Stack {
		
		/**
		 * An element of the stack.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class Element {
			private final long backPos;
			private final FSSTrackerState state;
			
			private Element(long backPos, FSSTrackerState state) {
				this.backPos = backPos;
				this.state = state;
			}
		}
		
		/**
		 * The stack is organized as a singly linked linear list of nodes.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class Node {
			private final Node prev;
			private final Element e;
			
			private Node(Node prev, Element e) {
				this.prev = prev;
				this.e = e;
			}
		}
		
		/**
		 * The current node.
		 */
		private Node cur;
		
		/**
		 * Constructs the stack.
		 */
		private Stack() {
			cur = null;
		}
		
		/**
		 * Tests if the stack is empty.
		 * 
		 * @return The boolean value {@code true} if the stack is empty, {@code
		 *         false} otherwise.
		 */
		private boolean isEmpty() {
			return cur == null;
		}
		
		/**
		 * Pushes the specified element on the stack.
		 * 
		 * @param e The element.
		 */
		private void push(Element e) {
			cur = new Node(cur, e);
		}
		
		/**
		 * Returns the current element from the stack.
		 * 
		 * @return The current element.
		 * @throws NullPointerException If the stack is empty.
		 */
		private Element pop() throws NullPointerException {
			Element e = cur.e;
			cur = cur.prev;
			return e;
		}
	}
	
	/**
	 * The charset used for recording the file identifier.
	 */
	private static final Charset utf = Charset.forName("UTF-8");
		
	/**
	 * A non-corrupted recorder file ends with an <em>end marker</em>, that is,
	 * a sequence of 8 predefined byte values.
	 * The end marker gives a bit of security that the recorder file is not
	 * corrupted.
	 */
	private static final byte[] endMarker = { -113, 56, 5, -12, 93, 124, -89,
																									-5 };
	
	/**
	 * Creates the recorder file and initializes it to contain the {@link
	 * #endMarker} which is 8 bytes long.
	 * 
	 * @param  recFilePath The absolute path of the recorder file (file name
	 *         included), not allowed to be {@code null}.
	 * 
	 * @throws IOFailureException If the file already exists or if another I/O
	 *         error occurs.
	 */
	static final void establishRecorderFile(Path recFilePath) throws
																				IOFailureException {
		final ByteBuffer buf = ByteBuffer.allocate(8);
		buf.put(endMarker);
		buf.rewind();
		
		try (FileIO recFile = new FileIO(recFilePath, CREATE_NEW, WRITE)) {
			recFile.write(buf);
		} catch (FileIOException e) {
			throw new IOFailureException(e);
		}
	}
	
	/**
	 * Finds out if the specified recorder file contains any uncommitted write
	 * operations from an earlier session.
	 * <p>
	 * This method throws an {@code ACDPException} if the recorder file is
	 * corrupted which is the case if and only if the recorder file does not end
	 * with the end marker.
	 * Thus, a non-corrupted recorder file has a size of at least 8 bytes.
	 * 
	 * @param  recFileIO The recorder file, not allowed to be {@code null} and
	 *         opened at least for reading.
	 * 
	 * @return The boolean value {@code true} if there exists any uncommitted
	 *         write operations from an earlier session, {@code false} otherwise.
	 *         
	 * @throws NullPointerException If {@code recFile} is equal to {@code null}.
	 * @throws ACDPException If the recorder file is corrupted.
	 * @throws FileIOException If an I/O error occurs.
	 */
	static final boolean existsUncommittedWrites(FileIO recFileIO) throws
								NullPointerException, ACDPException, FileIOException {
		final long size = recFileIO.size();
		
		if (size < 8)
			throw new ACDPException("Recorder file is corrupted.");
		else {
			final ByteBuffer buf = ByteBuffer.allocate(8);
			recFileIO.read(buf, size - 8);
			if (!Arrays.equals(buf.array(), endMarker)) {
				throw new ACDPException("Recorder file is corrupted: Bad end " +
																							"marker.");
			}
			// Recorder file doesn't seem to be corrupted.
			return size > 8;
		}
	}
	
	private final Buffer buffer = new Buffer();
	
	private final ByteBuffer bufEndMarker = ByteBuffer.wrap(endMarker);
	private final ByteBuffer buf8 = ByteBuffer.allocate(8);
	
	private final FileSpaceStateTracker fssTracker;
	private final Database_ db;
	private final boolean force;
	private final SyncManager syncManager;
	private final Map<Path, FileIO> forceMap;
	
	private final FileIO recFile;
	private final Stack stack;
	
	/**
	 * The position where to write the next entry into the recorder file.
	 * This is equal to the size of the recorder file.
	 */
	private long pos;
	/**
	 * The position within the recorder file of the first entry of a series
	 * of entries that must be processed (backwards) when the {@code rollback}
	 * method is invoked.
	 * If the unit is a top-level unit then this value is always equal to zero.
	 */
	private long backPos;
	/**
	 * The identifier of the thread which has opened the unit.
	 * If the value equals {@code -1} then the top-level unit is closed.
	 */
	private long threadId;
	/**
	 * If the unit breaks then this is the original exception thrown at the
	 * time the unit got broken.
	 * This value is {@code null} if the database is not broken.
	 */
	private UnitBrokenException unitBrokenException;
		
	/**
	 * Constructs the unit.
	 * <p>
	 * This method opens the recorder file and acquires an exclusive lock on
	 * the corresponding file channel.
	 * After that, this method checks if the recorder file is corrupted.
	 * If this is not the case and if the recorder file contains some entries
	 * from uncommitted write operations of an earlier session then this method
	 * tries to rollback the recorded before data.
	 * <p>
	 * The database is supposed to be writable.
	 * 
	 * @param  recFilePath The absolute path of the recorder file (file name
	 *         included), not allowed to be {@code null}.
	 * @param  db The database.
	 * @param  syncManager The database's synchronization manager.
	 * 
	 * @throws CreationException If the unit can't be created due to any reason,
	 *         including the corruption of the recorder file or a failed attempt
	 *         to rollback uncommitted write operations from an earlier session.
	 */
	Unit_(Path recFilePath, Database_ db, SyncManager syncManager) throws
																				CreationException {
		this.fssTracker = db.fssTracker();
		this.db = db;
		this.force = db.forceWriteCommit();
		// db.syncManager() returns null at this stage.
		this.syncManager = syncManager;
		this.forceMap = new HashMap<>();
		
		try {
			this.recFile = new FileIO(recFilePath, READ, WRITE);
			this.stack = new Stack();
			this.pos = recFile.size();
			this.backPos = 0;
			this.threadId = -1;
			this.unitBrokenException = null;
			
			if (recFile.tryLock() == null) {
				throw new ACDPException("Cannot acquire lock for recorder file.");
			}
			
			if (existsUncommittedWrites(recFile)) {
				// Last unit neither committed nor rolled back! Recorder file can
				// be considered non-corrupted. Try a rollback.
				try {
					// Imitate open unit.
					this.threadId = Thread.currentThread().getId();
					// Try rollback.
					rollback();
				} finally {
					this.threadId = -1;
				}
			}
		} catch (Exception e) {
			throw new CreationException(db, e);
		}
		// unit not broken, pos == backPos + 8, pos == 8, backPos == 0
	}
	
	/**
	 * Copies the content of the locked recorder file to the specified output
	 * stream.
	 * <p>
	 * The output stream is not closed.
	 * 
	 * @param  os The output stream, not allowed to be {@code null}.
	 * @param  buffer The buffer to be used, not allowed to be {@code null}.
	 * 
	 * @throws NullPointerException If one of the arguments is {@code null}.
	 * @throws FileIOException if an I/O error occurs.
	 */
	final void copyRecFile(OutputStream os, Buffer buffer) throws
													NullPointerException, FileIOException {
		recFile.copyFile(os, buffer);
	}
	
	/**
	 * Breaks the unit.
	 * 
	 * @param  message The message of the exception.
	 * @param  brokenCause The cause of the exception or {@code null}.
	 * 
	 * @throws UnitBrokenException Always thrown.
	 */
	private final void breakUnit(String message, Throwable brokenCause) throws
																			UnitBrokenException {
		unitBrokenException = new UnitBrokenException(db, message, brokenCause);
		throw unitBrokenException;
	}
	
	/**
	 * Ensures that the unit is not broken.
	 * 
	 * @throws UnitBrokenException If the unit is broken.
	 */
	final void ensureNotBroken() throws UnitBrokenException {
		if (unitBrokenException != null) {
			throw unitBrokenException;
		}
		// unitBrokenException == null
	}
	
	/**
	 * Tests if the unit is currently opened and if it was opened in the
	 * specified thread.
	 * 
	 * @param  threadId The identifier of the thread.
	 * 
	 * @return The boolean value {@code true} if the unit is opened and if it
	 *         was opened in the specified thread, {@code false} otherwise.
	 */
	final boolean openedInThread(long threadId) {
		return this.threadId == threadId;
	}
	
	/**
	 * Opens the top-level unit in the specified thread.
	 * <p>
	 * Use the {@link nest} method to "open" a nested unit.
	 * 
	 * @param threadId The identifier of the thread.
	 */
	final void open(long threadId) {
		this.threadId = threadId;
		// pos == 8, backPos == 0, fssTracker reset
	}
	
	/**
	 * "Opens" a nested unit.
	 */
	final void nest() {
		stack.push(new Stack.Element(backPos, fssTracker.getCopyOfState()));
		backPos = pos - 8;
		fssTracker.reset();
		// pos == backPos + 8, fssTracker reset
	}
	
	@Override
	public final void record(FileIO file, long pos, byte[] data, int offset,
				int length) throws IndexOutOfBoundsException, UnitBrokenException {
		ensureNotBroken();
		try {
			final byte[] filePathBytes = file.path.toString().getBytes(utf);
			final int len = filePathBytes.length + 1 + 8 + length + 8 + 8;
			final ByteBuffer buf = len <= buffer.maxCap() ? buffer.buf(len) :
																		ByteBuffer.allocate(len);
			long start = this.pos - 8;
			buf.put(filePathBytes).put((byte) '\t').putLong(pos).
								put(data, offset, length).putLong(start).put(endMarker);
			buf.rewind();
			recFile.write(buf, start);
			if (force) {
				recFile.force(true);
			}
			// If there is a need to record before data for file then file will be
			// changed right after.
			forceMap.put(file.path, file);
			this.pos = start + len;
		} catch (Exception e) {
			breakUnit("Recording before data failed. File and position: \"" +
																file.path + "\", " + pos, e);
		}
	}
	
	@Override
	public final void record(FileIO file, long pos, byte[] data) throws
																			UnitBrokenException {
		record(file, pos, data, 0, data.length);
	}
	
	@Override
	public final void addToForceList(FileIO file) {
		forceMap.put(file.path, file);
	}
	
	/**
	 * Truncates the recorder file to the size equal to the specified position
	 * plus eight bytes.
	 * The last eight bytes of the recorder file are equal to the bytes of the
	 * end marker.
	 * No truncation takes place if the specified position plus eight is greater
	 * than or equal to the size of the recorder file.
	 * 
	 * @param  pos The position.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void trunc(long pos) throws FileIOException {
		if (pos + 8 < this.pos) {
			bufEndMarker.rewind();
			recFile.write(bufEndMarker, pos);
			this.pos = pos + 8;
			recFile.truncate(this.pos);
			if (force) {
				recFile.force(true);
			}
			// this.pos == pos + 8
		}
	}
	
	/**
	 * Invokes {@link FileIO#force} for every file contained in the map of files
	 * and clears the map.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void force() throws FileIOException {
		for (FileIO file : forceMap.values()) {
			file.open();
			try {
				file.force(true);
			} finally {
				file.close();
			}
		}
		forceMap.clear();
	}
	
	@Override
	public final void commit() throws ACDPException, UnitBrokenException {
		if (Thread.currentThread().getId() != threadId) {
			throw new ACDPException(db, "Unit was originally opened in a " +
																			"different thread.");
		}
		ensureNotBroken();
		try {
			if (stack.isEmpty()) {
				// Top level unit.
				fssTracker.writeStates();
				if (force) {
					force();
				}
				trunc(0);
			}
			else {
				// Nested unit.
				backPos = pos - 8;
				fssTracker.stashPristineStates();
			}
			fssTracker.clearPristineStates();
		} catch (Exception e) {
			breakUnit("Commit failed!", e);
		}
		// top-level: pos == 8, backPos == 0, fssTracker reset
		// nested:    pos == backPos + 8, fssTracker old states cleared
	}
	
	/**
	 * Tests if the recorder file is <em>corrupted</em>.
	 * The recorder file is corrupted if and only if it does not end with the
	 * array of bytes equal to the value of the {@link #endMarker} property.
	 * Thus, a non-corrupted recorder file has a size of at least 8 bytes.
	 * 
	 * @return The boolean value {@code true} if the recorder file is corrupted,
	 *         {@code false} otherwise.
	 *         
	 * @throws FileIOException If an I/O exception occurs.
	 */
	private final boolean corrupted() throws FileIOException {
		if (pos < 8)
			return true;
		else {
			if (recFile.size() != pos)
				return true;
			else {
				buf8.rewind();
				recFile.read(buf8, pos - 8);
				return !Arrays.equals(buf8.array(), endMarker);
			}
		}
	}
		
	/**
	 * Reverts the entry of the recorder file.
	 * If the specified before data is empty then this method truncates the
	 * file associated with the specified file to the size given by the
	 * specified position.
	 * Otherwise, this method writes the specified before data to the file
	 * associated with the specified file at the specified
	 * file position.
	 * 
	 * @param  file The file.
	 * @param  pos The new size of the file or the file position where to write
	 *         the before data.
	 * @param  buf The buffer containing the before data.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void revertEntry(FileIO file, long pos,
													ByteBuffer buf) throws FileIOException {
		file.open();
		try {
			if (buf.remaining() == 0)
				file.truncate(pos);
			else {
				file.write(buf, pos);
			}
		}
		finally {
			file.close();
		}
	}
		
	/**
	 * Reads the entry defined by the specified starting and ending file
	 * positions from the recorder file, extracts from the entry the file path,
	 * the file position and the before data and reverts the entry.
	 * <p>
	 * It is supposed that the entry is not corrupted in one way or the other.
	 * If this is not the case this method may throw an exception of a type
	 * not described in this method description.
	 * 
	 * @param  startPos The position within the recorder file where the entry
	 *         starts.
	 * @param  endPos The position within the recorder file where the entry ends.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void processEntry(long startPos, long endPos) throws
																				FileIOException {
		// Read entry and convert to byte array.
		final int len = (int) (endPos - startPos);
		final ByteBuffer buf = len <= buffer.maxCap() ? buffer.buf(len) :
																		ByteBuffer.allocate(len);
		recFile.read(buf, startPos);
		final byte[] entry = buf.array();
		
		// Find the index of the tab character which separates the qualified
		// file name from the position and the array of bytes of the before data.
		int i = 0;
		while (entry[i] != '\t') {
			i++;
		}
		
		// Set the position of the buffer one after the tab character.
		buf.position(i + 1);
			
		// Revert!
		revertEntry(forceMap.get(Paths.get(new String(entry, 0, i, utf))),
																				buf.getLong(), buf);
	}
	
	/**
	 * Reads backwards one entry after the other from the recorder file and
	 * reverts the entries until the file position is equal to the value of the
	 * {@link #backPos} property.
	 * Starts at the position equal to the value of the {@link pos} property
	 * minus eight.
	 * <p>
	 * It is supposed that the entries in the recorder file are not corrupted in
	 * one way or the other.
	 * If this is not the case this method may throw an exception of a type
	 * not described in this method description.
	 *         
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void processEntries() throws FileIOException {
		long entryStart = pos - 8;
		while (entryStart > backPos) {
			long cur = entryStart - 8;
			buf8.rewind();
			recFile.read(buf8, cur);
			buf8.rewind();
			entryStart = buf8.getLong();
			processEntry(entryStart, cur);
		}
		// entryStart == backPos
	}
		
	/**
	 * Reverts the effects of a sequence of write operations onto the total set
	 * of data persisted in the database.
	 * This method affects all write operations executed within this unit that
	 * are not yet committed, no matter if this unit was opened as a nested unit
	 * within a parent unit or if this unit is a top-level unit.
	 * In addition, this method also affects all (even committed!) write
	 * operations executed within any nested unit.
	 * (Of course, any write operation executed within this or a nested unit
	 * that was previously rolled back is not affected.)
	 * Therefore, if none of the write operations executed within this unit are
	 * yet committed then this method completely reverts the effects onto the
	 * total set of persisted data of all write operations since this unit was
	 * opened, including all committed write operations executed within any
	 * nested unit.
	 * 
	 * @throws ACDPException If the unit was originally opened in a thread
	 *         different from the current thread.
	 *         This exception never happens if you consequently apply the usage
	 *         pattern described in the class description.
	 * @throws UnitBrokenException If rolling back fails due to a different
	 *         reason, including the case that the unit was already broken
	 *         <em>and</em> the recorder file was corrupted before this method
	 *         was able to start doing its job.
	 */
	private final void rollback() throws ACDPException, UnitBrokenException {
		if (Thread.currentThread().getId() != threadId) {
			throw new ACDPException(db, "Unit was originally opened in a " +
																			"different thread.");
		}
		
		// If the unit is broken now then it is broken because a write operation
		// executed within this unit OR a failed commit OR a nested unit broke
		// this unit. As long as the recorder file is not corrupted we try to
		// rollback, in particular, a commit that failed because it failed to
		// persist the states of the file spaces should not prevent us from
		// trying to rollback the unit.
		if (unitBrokenException != null) {
			// DB is broken.
			try {
				if (corrupted()) {
					throw unitBrokenException;
				}
			} catch (FileIOException e1) {
				throw new UnitBrokenException(db, "I/O error occurred accessing " +
													"recorder file.", unitBrokenException);
			}
		}
		
		// Rollback. Note that unit may be broken.
		try {
			processEntries();
			if (force && stack.isEmpty()) {
				// Top level unit.
				force();
			}
			trunc(backPos);
			fssTracker.adoptPristineStates();
			fssTracker.clearPristineStates();
		} catch (Exception e) {
			breakUnit("Rollback failed!", unitBrokenException != null ?
																		unitBrokenException : e);
		}
		// top-level: pos == 8, backPos == 0, fssTracker reset
		// nested:    pos == backPos + 8, fssTracker old states cleared
		// The unit may be broken!
	}

	/**
	 * Closes the unit.
	 * <p>
	 * Note that even if the unit is broken or even if this method throws an
	 * exception the unit is correctly closed, including unblocking the
	 * synchronization manager if the unit is a top-level unit.
	 * Of course, a nested unit which is broken or throws an exception when it
	 * is closed is supposed to trigger a rollback operation in its parent unit.
	 * 
	 * @throws ACDPException If the unit was originally opened in a thread
	 *         different from the current thread or if the unit contains an open
	 *         read zone.
	 *         This exception never happens if you consequently apply the usage
	 *         pattern described in the {@link Unit} class description and if
	 *         you consequently close inside the unit any read zone that was
	 *         opened within the unit.
	 * @throws UnitBrokenException If the unit was broken before this method was
	 *         invoked or if rolling back any uncommitted write operations
	 *         failed.
	 */
	@Override
	public final void close() throws ACDPException, UnitBrokenException {
		// Rollback any uncommitted write operations.
		// The unit should also be properly closed if the rollback fails.
		RuntimeException rollBackException = null;
		if (pos - 8 > backPos) {
			// There are uncommitted write operations.
			try {
				rollback();
			} catch (RuntimeException e) {
				rollBackException = e;
			}
		}
		
		// Finalize the unit. Should not throw an exception.
		final long savedThreadId = threadId;
		final boolean topLevel = stack.isEmpty();
		if (topLevel)
			threadId = -1;
		else {
			Stack.Element e = stack.pop();
			backPos = e.backPos;
			fssTracker.digestState(e.state);
		}
		
		// Now that the unit is correctly finalized we examine if there are some
		// errors which need to be propagated. Note that a nested unit that has
		// the following errors is supposed to trigger a rollback operation in
		// the unit where it was invoked.
		try {
			if (Thread.currentThread().getId() != savedThreadId) {
				throw new ACDPException(db, "Unit was originally opened in a " +
																			"different thread.");
			}
			if (syncManager.readZoneOpenedInThread(savedThreadId)) {
				throw new ACDPException(db, "Unit contains an open read zone.");
			}
			if (rollBackException != null) {
				throw rollBackException;
			}
			if (unitBrokenException != null) {
				throw unitBrokenException;
			}
		} finally {
			if (topLevel) {
				syncManager.unblock();
			}
		}
		// top-level:  pos == 8, backPos == 0, fssTracker reset
		// nested:     pos == backPos + 8
	}
	
	/**
	 * Forces any changes being materialized.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	final void forceWrite() throws FileIOException {
		recFile.force(true);
		force();
	}
	
	/**
	 * Shuts down the unit by forcing any changes being materialized, if
	 * necessary, and closing the recorder file.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	final void shutdown() throws FileIOException {
		if (!force) {
			forceWrite();
		}
		recFile.close();
	}
}