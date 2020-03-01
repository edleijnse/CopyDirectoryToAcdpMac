/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.misc;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import acdp.exceptions.IOFailureException;
import acdp.exceptions.ShutdownException;
import acdp.internal.FileIOException;

/**
 * The file channel provider provides references to {@link FileChannel}
 * instances.
 * If not stated otherwise the file channels are open, that is, they return
 * {@code true} upon calling the {@link FileChannel#isOpen} method.
 * Clients invoke the {@link #request} method to get a reference to an instance
 * of a file channel for a particular file path and they invoke the
 * {@link #release} method as soon as they are done with that file channel.
 * <p>
 * The file channel provider internally keeps a collection of file channels,
 * <em>at most one instance per path</em>.
 * At startup this collection is empty.
 * If a file channel for a particular file path is requested for the first time
 * or that file channel was requested before but was closed in the meantime then
 * the file channel provider opens the file channel, includes it into the
 * collection and returns a reference to the newly created file channel
 * If the collection already contains a file channel for that file path then
 * the file channel provider returns a reference to this instance.
 * <p>
 * The file channel provider remembers how many times a file channel for a
 * particular file path was requested and how many times it was released in
 * order to find out if there are clients around actually using that file
 * channel.
 * If there are no clients around using the file channel then this file channel
 * is said to be <em>idle</em>.
 * <p>
 * At the time a file channel is released it may become idle.
 * In such a case the file channel provider applies one of three different
 * <em>closing strategies</em> for closing the idle file channel:
 * 
 * <ol>
 * 	<li>Keep the idle file channel open.</li>
 * 	<li>Close the idle file channel immediately.</li>
 * 	<li>Close the idle file channel after a certain delay.</li>
 * </ol>
 * <p>
 * The value of the {@code delay} parameter of the constructor implicitly
 * defines the closing strategy:
 * If it is a negative number, equal to zero or a positive number then
 * the file channel provider applies the first, the second or the third closing
 * strategy, respectively.
 * In the case of the third closing strategy, the positive number specifies the
 * closing delay in milliseconds.
 * For instance, if the value of the {@code delay} parameter is equal to
 * {@code 60000} then the file channel provider closes an idle file channel
 * after one minute.
 * If the file channel for the same file path is requested again within one
 * minute then the file channel in the collection changes its state from idle
 * to non-idle and the file channel provider just returns a reference to the
 * file channel kept in the collection.
 * Next time the file channel becomes idle the delay restarts.
 * <p>
 * If the value of the {@code delay} parameter is less than {@code 10} then the
 * closing delay is set to {@code 10} milliseconds.
 * <p>
 * If a file channel is closed then it is removed from the collection of file
 * channels.
 * <p>
 * To periodically search for idle file channels which need to be closed, the
 * file channel provider starts a so called <em>closing thread</em> at the
 * moment the first file channel becomes idle.
 * The closing thread terminates if all idle file channels are closed and it is
 * restarted by the file channel provider next time a file channel becomes idle.
 * (Of course, the closing thread is never started if the file channel provider
 * applies the first or second closing strategy.)
 * <p>
 * If the file channel provider applies the first closing strategy or if there
 * is a chance that the client has "forgotten" to release a previously
 * requested file channel then the file channels should eventually be closed
 * invoking the {@link #shutdown() shutdown} method.
 * Note that the shutdown method also terminates the closing thread, if it is
 * running at all.
 * <p>
 * The file channel provider is safe for use by multiple concurrent threads.
 *
 * @author Beat Hoermann
 */
public final class FileChannelProvider {
	/**
	 * A wrapper class for a file channel used as the value class for the map.
	 * Keeps a file channel along with its reference count and its time stamp.
	 * (The reference count and the time stamp are used for the third
	 * <em>closing strategy</em> only.)
	 * 
	 * @author Beat Hoermann
	 */
	private static final class Value {
		/**
		 * The file path.
		 */
		final Path filePath;
		/**
		 * The file channel.
		 */
		final FileChannel fileChannel;
		/**
		 * The reference counter.
		 * This value is equal to the number of clients which currently use the
		 * {@code FileIO} instance.
		 * It is equal to zero if and only if the {@code FileIO} instance is idle.
		 */
		int refCount;
		/**
		 * The time stamp.
		 * This value is greater than zero if and only if the {@code FileIO}
		 * instance is idle.
		 */
		long timeStamp;
		
		/**
		 * The constructor.
		 * 
		 * @param fp The file path.
		 * @param fc The file channel.
		 */
		Value(Path fp, FileChannel fc) {
			filePath = fp;
			fileChannel = fc;
			refCount = 1;
			timeStamp = -1;
		}
		
		/**
		 * Stores the current time for later retrieval to find out how long a
		 * file channel is already idle.
		 * <p>
		 * This method is invoked when the {@code FileIO} instance becomes idle.
		 */
		void stamp() {
			timeStamp = System.currentTimeMillis();
		}
		
		/**
		 * Increments the reference count by one and resets the time stamp if
		 * necessary.
		 */
		final void incAndResetTimeStamp() {
			refCount++;
			timeStamp = -1;
		}
		
		/**
		 * Decrements the reference count by one provided that the reference
		 * is greater than zero.
		 */
		final void dec() {
			if (refCount > 0) {
				refCount--;
			}
		}
	}
	
	/**
	 * The delay applied for closing idle file channels.
	 * The value of this field implicitly defines the closing strategy:
	 * If it is a negative number, equal to zero or a positive number then
	 * the file channel applies the first, the second or the third closing
	 * strategy, respectively. 
	 */
	private final int delay;
	private final OpenOption[] options;
	private final int interval;
	
	/**
	 * The backing map.
	 * At any time this map is either empty or contains values that wrap open
	 * file channels.
	 * Entries are added by the {@link #request} method only.
	 */
	private final Map<Path, Value> map;
	
	private boolean shutdown;
	private Thread closingThread;
	
	/**
	 * The constructor.
	 * 
	 * @param delay The delay applied for closing idle file channels.
	 *        For details read the class description.
	 * @param options The options specifying how the file is opened.
	 */
	public FileChannelProvider(int delay, OpenOption... options) {
		this.delay = delay > 0 ? Math.max(10, delay) : delay;
		this.options = options;
		this.interval = this.delay / 10;
		this.map = new HashMap<>();
		this.shutdown = false;
		this.closingThread = null;
	}
	
	/**
	 * Returns a {@link FileChannel} for the specified path.
	 * If the internal collection does not contain a file channel for the
	 * specified path, then this method first opens a new file channel invoking
	 * the {@link FileChannel#open} method with the specified path and the
	 * options passed via the constructor.
	 * Afterwards, this method puts the newly created file channel into the
	 * internal collection and returns a reference to it.
	 * If the file channel already exists in the internal collection for the
	 * specified path then this method returns a reference to this stored file
	 * channel.
	 * <p>
	 * Unless the file channel provider applies the first closing strategy (see
	 * the class description), it is not guaranteed that calling this method
	 * {@code n} times ({@code n} &gt; 1) with the same path returns identical
	 * references, hence, it is not guaranteed that r<sub>1</sub> {@code ==}
	 * r<sub>2</sub> {@code ==} ... {@code ==} r<sub>n</sub> for any file
	 * channel returned by this method.
	 * <p>
	 * The position of the returned file channel is equal to zero.
	 * <p>
	 * This method fails if the file channel provider is <em>shut down</em>.
	 * 
	 * @param  path The path of the file.
	 * 
	 * @return The file channel for the specified path or {@code null} if
	 *         {@code path} is {@code null}.
	 * 
	 * @throws IllegalArgumentException If the set of open options contains an
	 *         invalid combination.
    * @throws UnsupportedOperationException If the {@code path} is associated
    *         with a file system provider that does not support creating file
    *         channels, or an unsupported open option is specified.
	 * @throws ShutdownException If the file channel provider is shut down.
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final synchronized FileChannel request(Path path) throws
													IllegalArgumentException,
													UnsupportedOperationException,
													ShutdownException, FileIOException {
		if (!shutdown) {
			final FileChannel fc;
			if (path == null)
				fc = null;
			else {
				Value val = map.get(path);
				if (val == null) {
					try {
						fc = FileChannel.open(path, options);
					} catch (IOException e) {
						throw new FileIOException(path, e);
					}
					map.put(path, new Value(path, fc));
				}
				else {
					fc = val.fileChannel;
					// The position of the file channel may be used by a previous
					// client.
					try {
						fc.position(0);
					} catch (IOException e) {
						throw new FileIOException(path, e);
					}
					val.incAndResetTimeStamp();
				}
			}
			return fc;
		}
		else {
			throw new ShutdownException("FileChannel provider shut down. " +
										"Path of requested file channel: " + path + ".");
		}
	}
	
	/**
	 * Closes the file channel wrapped by the specified value.
	 * 
	 * @param  val The value, not allowed to be {@code null}.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void close(Value val) throws FileIOException {
		try {
			val.fileChannel.close();
		} catch (IOException e) {
			throw new FileIOException(val.filePath, e);
		}
	}
	
	/**
	 * Closes all file channels with an idle time exceeding {@link #delay}.
	 * This method also removes the corresponding values from the {@link #map}.
	 * <p>
	 * This method must be synchronized because otherwise the {@code while} loop
	 * may run concurrently with the {@code release} method which may lead to a
	 * situation where the closing thread terminates despite of the released
	 * file channel which got idle just a blink of an eye before.
	 * 
	 * @return The boolean value {@code true} if there exists at least one idle
	 *         file channel whose life time has not yet elapsed, {@code false}
	 *         otherwise.
	 *         
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final synchronized boolean closeElapsedIdle() throws FileIOException{
		boolean nonElapsedIdleExist = false;
		for (Iterator<Value> it = map.values().iterator(); it.hasNext(); ) {
			Value val = it.next();
			if (val.refCount == 0) {
				// File channel is idle.
				if (System.currentTimeMillis() -  val.timeStamp > delay) {
					close(val);
					it.remove();
				}
				else {
					// Life time of idle file channel has not yet elapsed.
					nonElapsedIdleExist = true;
				}
			}
		}
		return nonElapsedIdleExist;
	}
	
	/**
	 * Signals that the file channel for the specified path is no longer used.
	 * Note that it is up to the file channel provider to decide when a file
	 * channel is actually closed.
	 * Consult the class description for further details.
	 * <p>
	 * This method has no effect if the specified path is {@code null} or if the
	 * file channel provider is <em>shut down</em> or applies the first
	 * <em>closing strategy</em> or if the internal collection does not contain
	 * a file channel based on the specified path.
	 * 
	 * @param  path The path of the file channel, may be {@code null}.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final synchronized void release(Path path) throws FileIOException {
		if (!shutdown && map.containsKey(path)) {
			Value val = map.get(path);
			val.dec();
			if (val.refCount == 0) {
				// The file channel was used before but is idle now.
				if (delay == 0) {
					// Second closing strategy: Close idle file channel.
					close(val);
					map.remove(path);
				}
				else if (delay > 0) {
					// Third closing strategy: Close idle file channel only after a
					// delay.
					val.stamp();
					// closingThread == null if and only if there are no other idle
					// file channels.
					if (closingThread == null) {
						closingThread = new Thread(new Runnable() {
									@Override
									public void run() {
										// Precondition: There exists exactly one idle
										// file channel and this file channel just became
										// idle.
										try {
											Thread.sleep(delay - interval);
										} catch (InterruptedException e) {
											return;
										}
										boolean idleExists;
										do {
											// There exists at least one idle file channel.
											try {
												Thread.sleep(interval);
											} catch (InterruptedException e) {
												return;
											}
											try {
												idleExists = closeElapsedIdle();
											} catch (IOException e) {
												throw new IOFailureException(e);
											}
										} while (idleExists);
										// There exist no idle file channels.
										closingThread = null;
									}});
						closingThread.start();
					}
				}
			}
		}
	}
	
	/**
	 * Closes the specified <em>idle</em> file channel.
	 * <p>
	 * Invoking the {@link #release} method is the usual way to indicate that a
	 * file channel is no longer used.
	 * However, invoke this method if you need to rely on the file channel
	 * actually being closed even when the file channel provider applies the
	 * first or third <em>closing strategy</em>.
	 * <p>
	 * This method has no effect if the file channel is closed or if it is not
	 * idle or if the file channel provider is <em>shut down</em>.
	 * 
	 * @param  path The path of the file channel, may be {@code null}.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final synchronized void forceClose(Path path) throws FileIOException {
		Value val = map.get(path);
		if (!shutdown && val!= null && val.refCount == 0) {
			close(val);
			map.remove(path);
		}
	}
	
	/**
	 * Closes all file channels contained in {@link #map}.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void closeAllFCs() throws FileIOException {
		for (Value val : map.values()) {
			close(val);
		}
	}
	
	/**
	 * Shuts down the file channel provider.
	 * This method closes all remaining file channels in the internal collection
	 * and terminates the closing thread, if it is running.
	 * <p>
	 * After this method has finished execution the file channel provider is
	 * <em>shut down</em> meaning that clients neither should access any file
	 * channels received from this file channel provider nor should they invoke
	 * any methods of this class.
	 * <p>
	 * You may want to add this method as part of a
	 * {@link Runtime#addShutdownHook shutdown hook} in order to close all
	 * remaining file channels at the time the JVM shuts down.
	 * If you do so then even if the file channel provider applies the first
	 * closing strategy or in cases where a client "forgets" releasing a file
	 * channel, any file channels eventually get closed.
	 * Note, however, that there are cases where the JVM is not able to execute
	 * the registered shutdown hooks.
	 * But even in such cases there is a chance that the operating system
	 * eventually closes all open files.
	 * <p>
	 * If the file channel provider applies the second closing strategy
	 * <em>and</em> it is guaranteed that the clients compensate each call to
	 * the {@code request} method by a corresponding call to the {@code release}
	 * method then there is no need for calling this method.
	 * <p>
    * If the file channel provider is already shut down then invoking this
    * method has no effect.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final synchronized void shutdown() throws FileIOException {
		if (!shutdown) {
			shutdown = true;
			if (closingThread != null) {
				try {
					closingThread.interrupt();
				} catch (NullPointerException e) {
					// closingThread just set to null by the closing thread itself!
				}
			}
			closeAllFCs();
			map.clear();
		}
	}
}