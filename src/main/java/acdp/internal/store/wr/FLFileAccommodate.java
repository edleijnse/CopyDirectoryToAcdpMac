/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import acdp.exceptions.ImplementationRestrictionException;
import acdp.internal.Buffer;
import acdp.internal.FileIO;
import acdp.internal.FileIOException;

/**
 * Adjusts the FL data file at some selective positions within the FL data
 * file with respect to the content and/or the size of the FL data blocks.
 * <p>
 * All FL data blocks stored in the FL data file are processed, thus,
 * accommodating the FL data file to be again in accordance with a new value
 * of a particular table parameter, with a change in the number of the table's
 * columns or with a change in the type of a particular column.
 * <p>
 * The points where the various adjustments within the FL data file take place,
 * are expressed by the positions within the FL data block.
 * This is possible due to the same sizes and format of all FL data blocks.
 * These points are called <em>spots</em>.
 * <p>
 * As an example, invoking
 * 
 * <pre>
 * FLFileAccommodate.spot(store.nBM, -store.nobsRefCount).run(store);</pre>
 * 
 * deletes the reference counter from all rows stored in the FL data file of the
 * {@code store} by cutting out {@code store.nobsRefCount} bytes at positions
 * equal to the values of {@code store.nBM}, {@code store.nBM} + {@code
 * store.n}, {@code store.nBM} + 2{@code store.n}, ... thus decreasing the size
 * of the FL data file by {@code nofBlocks} * {@code store.nobsRefCount} bytes,
 * where {@code nofBlocks} denotes the number of FL data blocks, hence, the
 * sum of the number of rows and the number of row gaps contained in the FL
 * data file.
 * <p>
 * In the example above, the accommodation was executed around a single spot
 * only which is actually the minimum number of spots allowed.
 * By chaining the {@code spot} methods
 * 
 * <pre>
 * FLFileAccommodate.spot(p1, ...).spot(p2, ...).spot(p3, ...)</pre>
 *    
 * accommodation can be done around more than one spot.
 * Note, however, that the spots' positions must be in ascending order ({@code
 * p1} &lt; {@code p2} &lt; {@code p3}, ...).
 * <p>
 * Since an FL data block houses the data of a row, spot positions typically
 * reflect the beginning of the bitmap (which is zero), the beginning of the
 * reference counter, as in the example above, or the beginning of the data of
 * a particular column.
 * <p>
 * A spot not only specifies the position where an adjustement takes place but
 * the type of adjustement as well.
 * Invoking
 * 
 * <pre>
 * FLFileAccommodate.spot(pos, cLen);</pre>
 * 
 * with {@code cLen} not equal to zero specifies a <em>contraction-only
 * spot</em>.
 * If {@code cLen} is less than zero then the contraction is said to be
 * <em>concentric</em>.
 * If {@code cLen} is greater than zero then the contraction is said to be
 * <em>excentric</em>.
 * Accommodating the FL data file at a concentric contraction spot results in
 * cutting out {@code -cLen} bytes from each FL data block while accommodating
 * the FL data file at an excentric contraction spot inserts a byte array
 * filled with zeros and of length equal to the value of {@code cLen} into each
 * FL data block.
 * <p>
 * Note that accommodating the FL data file in the presence of one or more
 * contraction spots does not necessarily result in a reduction or an increase
 * of the size of the FL data file.
 * It depends on the values of the {@code cLen} property of the spots and
 * whether the row has an <em>excess</em> or not.
 * <p>
 * Invoking
 * 
 * <pre>
 * FLFileAccommodate.spot(pos, updater);</pre>
 * 
 * with {@code updater} not equal to {@code null} specifies an <em>update-only
 * spot</em>.
 * Accommodating the FL data file at an update spot results in changing the
 * content within a section of the FL data blocks specified by the spot's
 * position and the value of the {@code Updater.len} property.
 * By implementing the {@link Updater} abstract class, clients specify the new
 * content.
 * <p>
 * Although it is possible to merely update the values of a table's column by
 * accommodating the FL data file around a single update-only spot that reflects
 * the beginning of the data of a particular column, this is not the preferred
 * way.
 * (Updating the values of a column can be done more efficiently by invoking
 * one of the {@code updateAll} methods declared in the {@link
 * acdp.Table Table} interface.)
 * Typical use cases of update spots involve changing the bitmaps and
 * accommodating the FL data file upon changing the type of a column that
 * normally requires the contraction of the column data followed by saving a
 * new column value.
 * <p>
 * Accommodating the FL data file around a spot specified by
 * 
 * <pre>
 * FLFileAccommodate.spot(pos, clen, updater);</pre>
 *    
 * with {@code cLen} not equal to zero and {@code updater} not equal to {@code
 * null} effectively results in a contraction <em>followed</em> by an update of
 * the FL data blocks at the same position.
 * <p>
 * Sometimes a client needs to know the content of the FL data blocks.
 * For this, a client implements the {@link Presenter} interface and passes
 * an instance to the {@link Spec#run(FLFileAccommodate.Presenter, WRStore)}
 * method.
 * Thus, invoking
 * 
 * <pre>
 * FLFileAccommodate.spot(...)....run(presenter, store);</pre>
 * 
 * with {@code presenter} not equal to {@code null} sends the content of the
 * stored and yet unchanged FL data block to the client at the time this FL
 * data block is going to be accommodated.
 * We call this FL data block the <em>current</em> FL data block.
 * Note that the current FL data block is presented to the client
 * <em>before</em> the FL data block is accommodated around the first spot.
 *
 * @author Beat Hoermann
 */
final class FLFileAccommodate {
	/**
	 * Clients implementing a presenter and passing an instance of a presenter
	 * to the {@link Spec#run(FLFileAccommodate.Presenter, WRStore)} method
	 * receive the current FL data block.
	 * 
	 * @author Beat Hoermann
	 */
	interface Presenter {
		/**
		 * Clients receive the current FL data block contained in the specified
		 * byte array at the specified offset.
		 * <p>
		 * Consider the specified byte array to be read-only.
		 * <p>
		 * Be aware that the format of the FL data file may become corrupted if
		 * this method throws an exception.
		 * 
		 * @param  flDataBlock The byte array housing the current FL data block,
		 *         never {@code null}.
		 * @param  offset The position within the byte array where the current
		 *         FL data block starts.
		 *        
		 * @throws FileIOException If an I/O error occurs.
		 *        
		 */
		void rowData(byte[] flDataBlock, int offset) throws FileIOException;
	}
	
	/**
	 * Clients change the content of the current FL data block by implementing
	 * an updater and connecting the updater to an update spot by passing it to
	 * one of the methods {@link FLFileAccommodate#spot(int, Updater)} and
	 * {@link FLFileAccommodate#spot(int, int, Updater)}.
	 * <p>
	 * Note that the update section of the FL data block starts at the update
	 * {@linkplain Spot#pos spot's position} and extends {@link #len} bytes.
	 * 
	 * @author Beat Hoermann
	 */
	static abstract class Updater {
		/**
		 * The update length, greater than zero.
		 */
		final int len;
		
		/**
		 * The constructor.
		 * 
		 * @param  len The update length, must be greater than zero.
		 * 
		 * @throws IllegalArgumentException If {@code len} is less than or equal
		 *         to zero.
		 */
		Updater(int len) throws IllegalArgumentException {
			if (len <= 0) {
				throw new IllegalArgumentException();
			}
			
			this.len = len;
		}
		
		/**
		 * By changing the content of the specified byte array, clients
		 * selectively change the content of the current FL data block.
		 * <p>
		 * Consider the specified byte array to be write-only.
		 * Do not change the byte array outside the range that starts at {@code
		 * offset}, inclusive, and ends at {@code offset} + {@link #len},
		 * exclusive.
		 * <p>
		 * Be aware that the format of the FL data file may become corrupted if
		 * this method throws an exception.
		 * 
		 * @param bytes The byte array to change, never {@code null}.
		 * @param offset The position where changing the contents of the byte
		 *        array should take place.
		 *        
		 * @throws FileIOException If an I/O error occurs.
		 */
		abstract void newData(byte[] bytes, int offset) throws FileIOException;
	}

	/**
	 * The spot as described in the {@linkplain FLFileAccommodate class
	 * description}.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class Spot {
		/**
		 * The position of the spot within the stored FL data block, &ge; 0.
		 */
		final int pos;
		/**
		 * The contraction length: negative or positive if the contraction is
		 * concentric or excentric, respectively.
		 * <p>
		 * If this value is equal to zero then the value of {@link #updater} is
		 * not {@code null} and this spot is an update-only spot.
		 */
		final int cLen;
		/**
		 * The client's updater, may be {@code null}.
		 * <p>
		 * If this value is {@code null} then the value of {@link #cLen} is not
		 * equal to zero and this spot is a contraction-only spot.
		 */
		final Updater updater;
		/**
		 * The update length.
		 * <p>
		 * This value is zero if and only if {@code updater} is {@code null}.
		 */
		final int uLen;
   
		/**
		 * The constructor.
		 * 
		 * @param  pos The spot's position within the FL data block, must be
		 *         greater than or equal to zero.
		 * @param  cLen The contraction length, may be zero.
		 * @param  updater The updater, may be {@code null}.
		 * 
		 * @throws IllegalArgumentException If {@code pos} is less than zero or
		 *         if {@code cLen} is zero and at the same time {@code updater}
		 *         is {@code null}.
		 */
		Spot(int pos, int cLen, Updater updater) throws IllegalArgumentException {
			if (pos < 0 || cLen == 0 && updater == null) {
				throw new IllegalArgumentException();
			}
			this.pos = pos;
			this.cLen = cLen;
			this.updater = updater;
			this.uLen = updater != null ? updater.len : 0;
		}
	}
	
	/**
	 * This class is threefold:
	 * 
	 * <ol>
	 *    <li>Defines the specification of an accommodation.</li>
	 *    <li>Provides the {@code spot} methods for adding more spots to
	 *        the specification.</li>
	 *    <li>Provides the {@code run} methods for optionally adding a
	 *        {@linkplain Presenter presenter} to the specification and executing
	 *        the accommodation.</li>
	 * </ol>
	 * <p>
	 * Ensures that the spot positions are in ascending order.
	 * 
	 * @author Beat Hoermann
	 */
	static final class Spec {
		/**
		 * The list of spots, never {@code null}, but may be empty.
		 * No element is equal to {@code null}.
		 */
		private final List<Spot> spots;
		/**
		 * The position of the last spot that was added to the list of spot or
		 * -1 if the list of spots is empty.
		 */
		private int lastPos;
		
		/**
		 * The constructor.
		 */
		private Spec() {
			this.spots = new ArrayList<>();
			lastPos = -1;
		}
		
		/**
		 * Checks the position and sets the {@code lastPos} property.
		 * 
		 * @param  pos The position, must be greater than the position of the
		 *         spot most recently added.
		 *         
		 * @throws IllegalArgumentException If {@code pos} is less than or equal
		 *         to the spot most recently added.
		 */
		private final void checkAndSetLastPos(int pos) throws
																		IllegalArgumentException {
			if (pos <= lastPos) {
				throw new IllegalArgumentException();
			}
			lastPos = pos;
		}
		
		/**
		 * Adds a contraction-only spot to the specification.
		 * 
		 * @param  pos The spot's position, must be greater than or equal to zero
		 *         and must be greater than the position of the spot most recently
		 *         added.
		 * @param  cLen The contraction length, not allowed to be zero.
		 * 
		 * @return This specification object.
		 *         
		 * @throws IllegalArgumentException If {@code pos} is less than zero or
		 *         less than or equal to the spot most recently added or if {@code
		 *         cLen} is zero.
		 */
		final Spec spot(int pos, int cLen) throws IllegalArgumentException {
			checkAndSetLastPos(pos);
			spots.add(new Spot(pos, cLen, null));
			return this;
		}
		
		/**
		 * Adds an update-only spot to the specification.
		 * 
		 * @param  pos The spot's position, must be greater than or equal to zero
		 *         and must be greater than the position of the spot most recently
		 *         added.
		 * @param  updater The client's updater, not allowed to be {@code null}.
		 * 
		 * @return This specification object.
		 *         
		 * @throws IllegalArgumentException If {@code pos} is less than zero or
		 *         less than or equal to the spot most recently added or if {@code
		 *         updater} is {@code null}.
		 */
		final Spec spot(int pos, Updater updater) throws
																		IllegalArgumentException {
			checkAndSetLastPos(pos);
			spots.add(new Spot(pos, 0, updater));
			return this;
		}
		
		/**
		 * Adds a spot to the specification.
		 * 
		 * @param  pos The spot's position, must be greater than or equal to zero
		 *         and must be greater than the position of the spot most recently
		 *         added.
		 * @param  cLen The contraction length, may be zero.
		 * @param  updater The client's updater, may be {@code null}.
		 * 
		 * @return This specification object.
		 *         
		 * @throws IllegalArgumentException If {@code pos} is less than zero or
		 *         less than or equal to the spot most recently added or if
		 *         {@code cLen} is zero and at the same time {@code updater}
		 *         is {@code null}.
		 */
		final Spec spot(int pos, int cLen, Updater updater) throws
																		IllegalArgumentException {
			checkAndSetLastPos(pos);
			spots.add(new Spot(pos, cLen, updater));
			return this;
		}
		
		/**
		 * Accommodates the FL data file around the specified spots, optionally
		 * presenting the current FL data block.
		 * This method has no effect if the FL data file contains no FL data
		 * blocks.
		 * <p>
		 * To run properly, the FL data file's {@code FileIO} instance {@code
		 * flDataFile} is not allowed to be already open at the time this method
		 * is invoked.
		 * <p>
		 * Note that the FL file space and hence the whole database won't work
		 * properly anymore after this method has been executed.
		 * The database should be closed as soon as possible.
		 * <p>
		 * Note also that the format of the FL data file may be corrupted if this
		 * method throws an exception different from the listed {@code
		 * IllegalArgumentException} and {@code
		 * ImplementationRestrictionException}.
		 * (Different exceptions from the listed {@code FileIOException} may be
		 * thrown by client implementations of the {@link Presenter#rowData} or
		 * {@link Updater#newData} methods.)
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB1}.
		 * 
		 * @param  presenter The client's presenter, may be {@code null}.
		 * @param  store The WR store, not allowed to be {@code null}.
		 * 
		 * @throws IllegalArgumentException If {@code spots} is empty.
		 * @throws ImplementationRestrictionException If the new size of the FL
		 *         data blocks exceeds {@code Integer.MAX_VALUE}.
		 * @throws FileIOException If an I/O error occurs.
		 */
		final void run(Presenter presenter, WRStore store) throws
									ImplementationRestrictionException, FileIOException {
			final FileIO vlDataFile = store.vlDataFile;
			vlDataFile.open();
			try {
				new FLFileAccommodate().run(store, presenter, spots);
				// The state of the VL file space may have changed.
				store.table.db().fssTracker().writeStates();
			} finally {
				vlDataFile.close();
			}
		}
		
		/**
		 * Accommodates the FL data file around the specified spots without
		 * presenting the current FL data block.
		 * This method has no effect if the FL data file contains no FL data
		 * blocks.
		 * <p>
		 * To run properly, the FL data file's {@code FileIO} instance {@code
		 * flDataFile} is not allowed to be already open at the time this method
		 * is invoked.
		 * <p>
		 * Note that the FL file space and hence the whole database won't work
		 * properly anymore after this method has been executed.
		 * The database should be closed as soon as possible.
		 * <p>
		 * Note also that the format of the FL data file may be corrupted if this
		 * method throws an exception different from the listed {@code
		 * IllegalArgumentException} and {@code
		 * ImplementationRestrictionException}.
		 * (Different exceptions from the listed {@code FileIOException} may be
		 * thrown by client implementations of the {@link Updater#newData}
		 * method.)
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB1}.
		 * 
		 * @param  store The WR store, not allowed to be {@code null}.
		 * 
		 * @throws IllegalArgumentException If {@code spots} is empty.
		 * @throws ImplementationRestrictionException If the new size of the FL
		 *         data blocks exceeds {@code Integer.MAX_VALUE}.
		 * @throws FileIOException If an I/O error occurs.
		 */
		final void run(WRStore store) throws ImplementationRestrictionException,
																					FileIOException {
			run(null, store);
		}
	}
	
	/**
	 * Creates the specification of the accommodation and adds the first spot to
	 * the specification.
	 * <p>
	 * The added spot is a contraction-only spot.
	 * 
	 * @param  pos The spot's position, must be greater than or equal to zero.
	 * @param  cLen The contraction length, not allowed to be zero.
	 * 
	 * @return The specification object to specify additional spots and finally
	 *         to execute the accommodation of the FL data file based on that
	 *         specification.
	 *         
	 * @throws IllegalArgumentException If {@code pos} is less than zero or if
	 *         {@code cLen} is zero.
	 */
	static final Spec spot(int pos, int cLen) throws IllegalArgumentException {
		return new Spec().spot(pos, cLen, null);
	}
	
	/**
	 * Creates the specification of the accommodation and adds the first spot to
	 * the specification.
	 * <p>
	 * The added spot is an update-only spot.
	 * 
	 * @param  pos The spot's position, must be greater than or equal to zero.
	 * @param  updater The client's updater, not allowed to be {@code null}.
	 * 
	 * @return The specification object to specify additional spots and finally
	 *         to execute the accommodation of the FL data file based on that
	 *         specification.
	 *         
	 * @throws IllegalArgumentException If {@code pos} is less than zero or if
	 *         {@code updater} is {@code null}.
	 */
	static final Spec spot(int pos, Updater updater) throws
																		IllegalArgumentException {
		return new Spec().spot(pos, 0, updater);
	}
	
	/**
	 * Creates the specification of the accommodation and adds the first spot to
	 * the specification.
	 * <p>
	 * The added spot is a contraction spot and an update spot at the same time.
	 * 
	 * @param  pos The spot's position, must be greater than or equal to zero.
	 * @param  cLen The contraction length, not allowed to be zero.
	 * @param  updater The client's updater, not allowed to be {@code null}.
	 * 
	 * @return The specification object to specify additional spots and finally
	 *         to execute the accommodation of the FL data file based on that
	 *         specification.
	 *         
	 * @throws IllegalArgumentException If {@code pos} is less than zero or if
	 *         {@code cLen} is zero and at the same time {@code updater} is
	 *         {@code null}.
	 */
	static final Spec spot(int pos, int cLen, Updater updater) throws
																		IllegalArgumentException {
		return new Spec().spot(pos, cLen, updater);
	}
	
	/**
	 * Creates the specification with an empty list of spots.
	 * <p>
	 * Note that a specification must contain at least one spot before it can
	 * be executed.
	 * 
	 * @return The specification, never {@code null}.
	 */
	static final Spec newSpec() {
		return new Spec();
	}
	
	/**
	 * Provides the {@link #run} method which accommodates the FL data file
	 * around a single concentric contraction-only spot.
	 * <p>
	 * The accommodation implemented in this class is superior to the one
	 * implemented in the {@link General} class but works only for the single
	 * case where the specification consists of a single concentric
	 * contraction-only spot without a presenter.
	 *
	 * @author Beat Hoermann
	 */
	private static final class SingleConcentric {
		/**
		 * Defines a contractor based on a {@link ByteBuffer}.
		 * <p>
		 * A byte buffer contractor contracts the FL data blocks stored in the
		 * byte array of a globally agreed {@code ByteBuffer} instance by cutting
		 * out a subarray of bytes from each FL data block.
		 * 
		 * @author Beat Hoermann
		 */
		private static interface IByteBufferContractor {
			/**
			 * Contracts the FL data blocks stored in the byte array of a globally
			 * agreed {@code ByteBuffer} instance.
			 * <p>
			 * Precondition: The FL data blocks to be contracted reside in the
			 * subarray of the byte buffer's byte array starting at position 0 and
			 * having a total length equal to the value of the byte buffer's limit.
			 * The byte buffer's limit must be greater than zero and divisible by
			 * the size of an FL data block without a remainder.
			 * <p>
			 * Postcondition: The contracted FL data blocks reside in the subarray
			 * of the byte buffer's byte array starting at position 0 and having a
			 * total size equal to the value of the byte buffer's current position.
			 * <p>
			 * Invariant: This method changes the current position of the byte
			 * buffer only.
			 * The current position is less than or equal to the limit of the byte
			 * buffer.
			 */
			void contract();
		}
		
		/**
		 * This type of {@linkplain IByteBufferContractor byte buffer contractor}
		 * does not make a distinction between an allocated memory block and a
		 * gap.
		 * Furthermore, this type of contractor assumes that there exists no
		 * excess and that there is no need for producing any excess.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class Simple implements IByteBufferContractor {
			private final int cutPos;
			private final int length;
			private final ByteBuffer buf;
			private final int n;
			private final byte[] bytes;
			private final int m;
			
			/**
			 * Constructs this type of byte buffer contractor.
			 * <p>
			 * Precondition: {@code cutPos} &ge; 8.
			 * 
			 * @param cutPos The cutting position within an FL data block.
			 * @param length The length of the byte array to cut out.
			 * @param buf The byte buffer housing the FL data blocks to be
			 *        contracted.
			 * @param n The size of the FL data blocks, greater than or equal to 8.
			 */
			Simple(int cutPos, int length, ByteBuffer buf, int n) {
				this.cutPos = cutPos;
				this.length = length;
				this.buf = buf;
				this.n = n;
				this.bytes = buf.array();
				this.m = n - length;
			}

			@Override
			public final void contract() {
				// The ByteBuffer.put(byte[], ...)-methods are not well implemented.
				final int size = buf.limit();
				int bufPos = cutPos;
				int off = cutPos + length;
				while (off + m <= size) {
					System.arraycopy(bytes, off, bytes, bufPos, m);
					bufPos += m;
					off += n;
				}
				final int rest = size - off;
				System.arraycopy(bytes, off, bytes, bufPos, rest);
				buf.position(bufPos + rest);
			}
		}
		
		/**
		 * This type of {@linkplain IByteBufferContractor byte buffer contractor}
		 * makes a distinction between an allocated memory block and a gap.
		 * Furthermore, this type of contractor assumes that there exists no
		 * excess and that there is no need for producing any excess.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class RespectGap implements IByteBufferContractor {
			private final int cutPos;
			private final int length;
			private final ByteBuffer buf;
			private final int n;
			private final byte[] bytes;
			
			/**
			 * Constructs this type of byte buffer contractor.
			 * <p>
			 * Precondition: {@code cutPos} &lt; 8 {@literal &&} {@code n} - {@code
			 * length} &ge; 8
			 * 
			 * @param cutPos The cutting position within an FL data block.
			 * @param length The length of the byte array to cut out.
			 * @param buf The byte buffer housing the FL data blocks to be
			 *        contracted.
			 * @param n The size of the FL data blocks, greater than or equal to 8.
			 */
			RespectGap(int cutPos, int length, ByteBuffer buf, int n) {
				this.cutPos = cutPos;
				this.length = length;
				this.buf = buf;
				this.n = n;
				this.bytes = buf.array();
			}
			
			@Override
			public final void contract() {
				// The ByteBuffer.put(byte[], ...)-methods are not well implemented.
				final int size = buf.limit();
				int pos = bytes[0] < 0 ? 8 : cutPos;
				int bufPos = pos;
				int off = pos + length;
				int nextBlock = n;
				while (nextBlock < size) {
					pos = nextBlock + (bytes[nextBlock] < 0 ? 8 : cutPos);
					int l = pos - off;
					System.arraycopy(bytes, off, bytes, bufPos, l);
					bufPos += l;
					off = pos + length;
					nextBlock += n;
				}
				final int rest = size - off;
				System.arraycopy(bytes, off, bytes, bufPos, rest);
				buf.position(bufPos + rest);
			}
		}
		
		/**
		 * This type of {@linkplain IByteBufferContractor byte buffer contractor}
		 * makes a distinction between an allocated memory block and a gap.
		 * Furthermore, this type of contractor assumes that there may exist an
		 * excess and that there may be a need for producing any (more) excess.
		 * <p>
		 * Under certain circumstances this type of byte buffer contractor
		 * retains the size of the FL data blocks and therefore the size of the
		 * FL data file.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class RespectGapExcess implements
																			IByteBufferContractor {
			private final int cutPos;
			private final ByteBuffer buf;
			private final int n;
			private final int nE;
			private final byte[] bytes;
			private final int l;
			private final int incOff;
			private final boolean sizeReduction;
			
			/**
			 * Constructs this type of byte buffer contractor.
			 * <p>
			 * If {@code n} is equal to 8 then the size of the FL data blocks
			 * and therefore the size of the FL data file will be retained.
			 * <p>
			 * Precondition: {@code cutPos} &lt; 8 {@literal &&} {@code n} - {@code
			 * length} &lt; 8
			 * 
			 * @param cutPos The cutting position within an FL data block.
			 * @param length The length of the byte array to cut out.
			 * @param buf The byte buffer housing the FL data blocks to be
			 *        contracted.
			 * @param n The size of the FL data blocks, greater than or equal to 8.
			 */
			RespectGapExcess(int cutPos, int length, ByteBuffer buf, int n) {
				this.cutPos = cutPos;
				this.buf = buf;
				this.n = n;
				final int diff = n - length;
				this.nE = 8 - diff;
				this.bytes = buf.array();
				this.l = diff - cutPos;
				this.incOff = cutPos + length;
				this.sizeReduction = nE < length;
			}
			
			@Override
			public final void contract() {
				// The ByteBuffer.put(byte[], ...)-methods are not well implemented.
				final int size = buf.limit();
				int bufPos = 0;
				int off = 0;
				while (off < size) {
					if (bytes[off] < 0) {
						if (sizeReduction) {
							System.arraycopy(bytes, off, bytes, bufPos, 8);
						}
						bufPos += 8;
						off += n;
					}
					else {
						if (sizeReduction) {
							System.arraycopy(bytes, off, bytes, bufPos, cutPos);
						}
						bufPos += cutPos;
						off += incOff;
						System.arraycopy(bytes, off, bytes, bufPos, l);
						bufPos += l + nE;
						off += l;
					}
				}
				buf.position(bufPos);
			}
		}
		
		/**
		 * The WR store as given via the constructor
		 */
		private final WRStore store;
		/**
		 * The underlying file of the FL file space.
		 */
		private final FileIO flDataFile;
		/**
		 * The size of an FL data block, greater than or equal to 8.
		 */
		private final int n;
		/**
		 * The position within the {@code flDataFile} where the first FL data
		 * block starts.
		 */
		private final int start;
		/**
		 * The size of the {@code flDataFile}.
		 */
		private final long size;
		
		/**
		 * The constructor.
		 * 
		 * @param  store The store, not allowed to be {@code null}.
		 */
		SingleConcentric(WRStore store) {
			this.store = store;
			this.flDataFile = store.flDataFile;
			this.n = store.n;
			final FLFileSpace flFileSpace = store.flFileSpace;
			this.start = (int) flFileSpace.indexToPos(0);
			this.size = flFileSpace.size();
		}
		
		/**
		 * Contracts the FL data blocks stored in the underlying FL data file of
		 * the FL file space.
		 * <p>
		 * It is assumed that the file contains at least one FL data block.
		 * 
		 * @param  cutPos The position within an FL data block where to cut out
		 *         the subarray of bytes.
		 * @param  length The length of the byte array to cut out, assumed to be
		 *         greater than zero.
		 * @param  buf The buffer used to reduce the frequency of file IO, not
		 *         allowed to be {@code null}.
		 *         The buffer's limit must be greater than zero and divisible by
		 *         the size of an FL data block without a remainder.
		 * 
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void contract(int cutPos, int length, ByteBuffer buf) throws
																					FileIOException {
			// Create the contractor.
			IByteBufferContractor bbc = null;
			if (cutPos >= 8)
				bbc = new Simple(cutPos, length, buf, n);
			else if (cutPos < 8 && n - length >= 8)
				// Contract gaps at 8 instead of cutPos.
				bbc = new RespectGap(cutPos, length, buf, n);
			else {
				// cutPos < 8 && n - length < 8
				// Contract n - 8 bytes only.
				bbc = new RespectGapExcess(cutPos, length, buf, n);
			}
			
			final int limit = buf.limit();
			// limit > 0 && limit % n == 0
			
			flDataFile.position(start);
			long filePos = start;
			// Precondition: filePos < size
			do {
				// Load buf
				final long remaining = size - filePos;
				if (buf.limit() > remaining)
					buf.limit((int) remaining);
				else {
					buf.limit(limit);
				}
				buf.rewind();
				flDataFile.read_(buf, filePos);
				filePos += buf.limit();
					
				// Contract
				// buf.limit() > 0 && buf.limit() % n == 0
				// buf.position() == buf.limit() equals size of the byte array to be
				// contracted.
				bbc.contract();
					
				// Write buf
				// buf contains contracted byte array.
				// buf.position() equals size of the contracted byte array.
				// Note that flip changes limit.
				buf.flip();
				flDataFile.write(buf);
			} while (filePos < size);
			
			// Truncate FL data file.
			flDataFile.truncate(flDataFile.position());
		}
		
		/**
		 * Accommodates the FL data file around the specified concentric
		 * contraction-only spot.
		 * <p>
		 * If the size of an FL data block is equal to 8 bytes then the size of
		 * the FL data blocks remains unchanged.
		 * Otherwise, this method reduces the size of the FL data blocks and
		 * therefore the size of the FL data file.
		 * <p>
		 * Note that the FL file space and hence the whole database won't work
		 * properly anymore after this method has been executed.
		 * The database should be closed as soon as possible.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB1}.
		 * 
		 * @param  spot The concentric contraction-only spot.
		 *         (The value of the {@code cLen} property of a concentric
		 *         contraction spot is less than zero.)
		 * 
		 * @throws FileIOException If an I/O error occurs.
		 */
		final void run(Spot spot) throws FileIOException {
			// Precondition: size > start && spot.cLen < 0
			final long netSize = size - start;
			// netSize % n == 0
			final Buffer buffer = store.gb1;
			// netSize >= n
			final ByteBuffer buf;
			final int maxCap = buffer.maxCap();
			if (maxCap < n) 
				buf = ByteBuffer.allocate(n);
			else if (maxCap >= netSize)
				buf = buffer.buf(netSize);
			else {
				buf = buffer.buf(maxCap / n * n);
			}
			// buf.limit() > 0 && buf.limit() % n == 0
			flDataFile.open();
			try {
				contract(spot.pos, -spot.cLen, buf);
			} finally {
				flDataFile.close();
			}
		}
	}
	
	/**
	 * Provides the {@link #run} method which accommodates the FL data file
	 * around a single excentric contraction-only spot.
	 * <p>
	 * The accommodation implemented in this class is superior to the one
	 * implemented in the {@link General} class but works only for the single
	 * case where the specification consists of a single excentric
	 * contraction-only spot without a presenter.
	 *
	 * @author Beat Hoermann
	 */
	private static final class SingleExcentric {
		/**
		 * Defines an expander based on a {@link ByteBuffer}.
		 * <p>
		 * A byte buffer expander expands the FL data blocks stored in the byte
		 * array of a globally agreed {@code ByteBuffer} instance by inserting an
		 * array of bytes filled with zeros into each FL data block.
		 * 
		 * @author Beat Hoermann
		 */
		private static interface IByteBufferExpander {
			/**
			 * Expands the FL data blocks stored in the byte array of a globally
			 * agreed {@code ByteBuffer} instance.
			 * <p>
			 * Precondition: Let {@code p} &gt; 0 denote the number of FL data
			 * blocks stored in the byte buffer and let {@code m} &ge; 0 denote
			 * the additional number of bytes required to save an expanded FL data
			 * block.
			 * The byte buffer's current position and limit are equal to
			 * {@code p} * {@code m} and {@code p} * ({@code m} + {@code n}),
			 * respectively, with {@code n} denoting the size of an FL data block.
			 * Furthermore, the FL data blocks to be expanded reside in the
			 * subarray of the byte buffer's byte array starting at the byte
			 * buffer's current position.
			 * <p>
			 * Postcondition: The expanded FL data blocks reside in the byte
			 * buffer's byte array.
			 * <p>
			 * Invariant: This method neither changes the current position nor the
			 * limit of the byte buffer.
			 */
			void expand();
		}
		
		/**
		 * This type of {@linkplain IByteBufferExpander byte buffer expander}
		 * does not make a distinction between an allocated memory block and a
		 * gap.
		 * Furthermore, this type of expander assumes that there exists no excess.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class Simple implements IByteBufferExpander {
			private final int insertPos;
			private final int length;
			private final ByteBuffer buf;
			private final int n;
			private final byte[] bytes;
			private final byte zero = (byte) 0;
			
			/**
			 * Constructs this type of byte buffer expander.
			 * <p>
			 * Precondition: {@code insertPos} &ge; 8.
			 * 
			 * @param insertPos The position within an FL data block where to
			 *        insert the array of zero bytes.
			 * @param length The length of the byte array to insert, assumed to be
			 *        greater than zero.
			 * @param buf The byte buffer housing the FL data blocks to be
			 *        expanded.
			 * @param n The size of the FL data blocks, greater than or equal to 8.
			 */
			Simple(int insertPos, int length, ByteBuffer buf, int n) {
				this.insertPos = insertPos;
				this.length = length;
				this.buf = buf;
				this.n = n;
				this.bytes = buf.array();
			}
			
			@Override
			public final void expand() {
				// The ByteBuffer.put(byte[], ...)-methods are not well implemented.
				int bufPos = 0;
				int off = buf.position();
				System.arraycopy(bytes, off, bytes, bufPos, insertPos);
				bufPos += insertPos;
				off += insertPos;
				int newOff = off + n;
				final int size = buf.limit();
				while (newOff < size) {
					final int newBufPos = bufPos + length;
					Arrays.fill(bytes, bufPos, newBufPos, zero);
					bufPos = newBufPos;
					System.arraycopy(bytes, off, bytes, bufPos, n);
					bufPos += n;
					off = newOff;
					newOff += n;
				}
				Arrays.fill(bytes, bufPos, bufPos + length, zero);
			}
		}
		
		/**
		 * This type of {@linkplain IByteBufferExpander byte buffer expander}
		 * makes a distinction between an allocated memory block and a gap.
		 * Furthermore, this type of expander assumes that there exists no excess.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class RespectGap implements IByteBufferExpander {
			private final int insertPos;
			private final int length;
			private final ByteBuffer buf;
			private final int n;
			private final byte[] bytes;
			private final byte zero = (byte) 0;
			
			/**
			 * Constructs this type of byte buffer expander.
			 * <p>
			 * Precondition: {@code insertPos} &lt; 8 and there exists no excess.
			 * 
			 * @param insertPos The position within an FL data block where to
			 *        insert the array of zero bytes.
			 * @param length The length of the byte array to insert, assumed to be
			 *        greater than zero.
			 * @param buf The byte buffer housing the FL data blocks to be
			 *        expanded.
			 * @param n The size of the FL data blocks, greater than or equal to 8.
			 */
			RespectGap(int insertPos, int length, ByteBuffer buf, int n) {
				this.insertPos = insertPos;
				this.length = length;
				this.buf = buf;
				this.n = n;
				this.bytes = buf.array();
			}
			
			@Override
			public final void expand() {
				// The ByteBuffer.put(byte[], ...)-methods are not well implemented.
				int bufPos = 0;
				int off = buf.position();
				int nextBlock = off;
				final int size = buf.limit();
				while (nextBlock < size) {
					final int pos = nextBlock + (bytes[nextBlock] < 0 ? 8 :
																							insertPos);
					final int l = pos - off;
					System.arraycopy(bytes, off, bytes, bufPos, l);
					bufPos += l;
					final int newBufPos = bufPos + length;
					Arrays.fill(bytes, bufPos, newBufPos, zero);
					bufPos = newBufPos;
					off = pos;
					nextBlock += n;
				}
			}
		}
		
		/**
		 * This type of {@linkplain IByteBufferExpander byte buffer expander}
		 * makes a distinction between an allocated memory block and a gap.
		 * Furthermore, this type of expander assumes that there exists an excess
		 * and that the length of the byte array to insert is greater than the
		 * excess so that the size of the FL data blocks and therefore the size of
		 * the FL data file increase.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class RespectGapLengthGtExcess implements
																			IByteBufferExpander {
			private final int insertPos;
			private final int length;
			private final ByteBuffer buf;
			private final byte[] bytes;
			private final int q;
			private final int m;
			private final int diff;
			private final byte zero = (byte) 0;
			
			/**
			 * Constructs this type of byte buffer expander.
			 * <p>
			 * Precondition: {@code length} &gt; {@code nE}.
			 * 
			 * @param insertPos The position within an FL data block where to
			 *        insert the array of zero bytes.
			 * @param length The length of the byte array to insert, assumed to be
			 *        greater than zero.
			 * @param buf The byte buffer housing the FL data blocks to be
			 *        expanded.
			 * @param nE The number of excess bytes, greater than zero and less
			 *        than or equal to 6.
			 */
			RespectGapLengthGtExcess(int insertPos, int length, ByteBuffer buf,
																							int nE) {
				this.insertPos = insertPos;
				this.length = length;
				this.buf = buf;
				this.bytes = buf.array();
				this.q = 8 - insertPos;
				this.m = q - nE;
				this.diff = length - nE;
			}
			
			@Override
			public final void expand() {
				// The ByteBuffer.put(byte[], ...)-methods are not well implemented.
				int bufPos = 0;
				int off = buf.position();
				final int size = buf.limit();
				while (off < size) {
					final int l = (bytes[off] < 0 ? 8 : insertPos);
					System.arraycopy(bytes, off, bytes, bufPos, l);
					bufPos += l;
					off += l;
					if (l == insertPos) {
						final int newBufPos = bufPos + length;
						System.arraycopy(bytes, off, bytes, newBufPos, m);
						Arrays.fill(bytes, bufPos, newBufPos, zero);
						bufPos = newBufPos + m;
						off += q;
					}
					else {
						bufPos += diff;
					}
				}
			}
		}
		
		/**
		 * This type of {@linkplain IByteBufferExpander byte buffer expander}
		 * makes a distinction between an allocated memory block and a gap.
		 * Furthermore, this type of expander assumes that there exists an excess
		 * and that the length of the byte array to insert is less than or equal
		 * to the excess so that the size of the FL data blocks and therefore the
		 * size of the FL data file remains unchanged.
		 * 
		 * @author Beat Hoermann
		 */
		private static final class RespectGapLengthLeExcess implements
																			IByteBufferExpander {
			private final int insertPos;
			private final int length;
			private final ByteBuffer buf;
			private final byte[] bytes;
			private final int m;
			private final byte zero = (byte) 0;
			
			/**
			 * Constructs this type of byte buffer expander.
			 * <p>
			 * Precondition: {@code length} &le; {@code nE}.
			 * 
			 * @param insertPos The position within an FL data block where to
			 *        insert the array of zero bytes.
			 * @param length The length of the byte array to insert, assumed to be
			 *        greater than zero.
			 * @param buf The byte buffer housing the FL data blocks to be
			 *        expanded.
			 * @param nE The number of excess bytes, greater than zero and less
			 *        than or equal to 6.
			 */
			RespectGapLengthLeExcess(int insertPos, int length, ByteBuffer buf,
																							int nE) {
				this.insertPos = insertPos;
				this.length = length;
				this.buf = buf;
				this.bytes = buf.array();
				this.m = 8 - insertPos - nE;
			}
			
			@Override
			public final void expand() {
				// The ByteBuffer.put(byte[], ...)-methods are not well implemented.
				int nextBlock = 0;
				final int size = buf.limit();
				while (nextBlock < size) {
					if (bytes[nextBlock] >= 0) {
						final int off = nextBlock + insertPos;
						final int newBufPos = off + length;
						System.arraycopy(bytes, off, bytes, newBufPos, m);
						Arrays.fill(bytes, off, newBufPos, zero);
					}
					nextBlock += 8;
				}
			}
		}
		
		/**
		 * The WR store as given via the constructor
		 */
		private final WRStore store;
		/**
		 * The underlying file of the FL file space.
		 */
		private final FileIO flDataFile;
		/**
		 * The size of an FL data block, greater than or equal to 8.
		 */
		private final int n;
		/**
		 * The position within the {@code flDataFile} where the first FL data
		 * block starts.
		 */
		private final int start;
		/**
		 * The size of the {@code flDataFile}.
		 */
		private final long size;
		
		/**
		 * The constructor.
		 * 
		 * @param  store The store, not allowed to be {@code null}.
		 */
		SingleExcentric(WRStore store) {
			this.store = store;
			this.flDataFile = store.flDataFile;
			this.n = store.n;
			final FLFileSpace flFileSpace = store.flFileSpace;
			this.start = (int) flFileSpace.indexToPos(0);
			this.size = flFileSpace.size();
		}
		
		/**
		 * Expands the FL data blocks stored in the underlying FL data file of the
		 * FL file space.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB1}.
		 * 
		 * @param  insertPos The position within an FL data block where to insert
		 *         the array of zero bytes.
		 * @param  length The length of the byte array to insert, must be greater
		 *         than zero.
		 * @param  nE The number of excess bytes, greater than or equal to zero
		 *         and less than or equal to 6.
		 * 
		 * @throws ImplementationRestrictionException If the new size of the FL
		 *         data blocks exceeds {@code Integer.MAX_VALUE}.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void expand(int insertPos, int length, int nE) throws
									ImplementationRestrictionException, FileIOException {
			// Find out how much the size of the FL data block increases.
			final int m = length > nE ? length - nE : 0;
			final int newN = n + m;
			if (newN < 0) {
				throw new ImplementationRestrictionException("FL data blocks too " +
																	"large for being expanded.");
			}
			
			final long nofBlocks = store.flFileSpace.nofBlocks();
			
			// Find out number of FL data blocks that can be buffered and create
			// byte buffer.
			final Buffer buffer = store.gb1;
			final int maxCap = buffer.maxCap();
			int p;
			final ByteBuffer buf;
			if (maxCap < newN) {
				p = 1;
				buf = ByteBuffer.allocate(newN);
			}
			else {
				if (maxCap >= nofBlocks * newN)
					p = (int) nofBlocks;
				else {
					p = maxCap / newN;
					// p < nofBlocks
				}
				buf = buffer.buf(p * newN);
			}
			// 0 < p <= nofBlocks && buf.limit() > 0 && buf.limit() % newN == 0
												
			// Create the expander.
			final IByteBufferExpander bbc;
			if (insertPos >= 8)
				// Since insertPos >= 8 implies nE == 0 it follows
				// insertPos >= 8 && nE == 0
				bbc = new Simple(insertPos, length, buf, n);
			else if (nE == 0)
				// insertPos < 8 && nE == 0
				bbc = new RespectGap(insertPos, length, buf, n);
			else if (length > nE)
				// insertPos < 8 && nE > 0 && size of FL data blocks increases
				// From nE > 0 it follows n == 8
				bbc = new RespectGapLengthGtExcess(insertPos, length, buf, nE);
			else {
				// insertPos < 8 && nE > 0 && size of FL data blocks does not
				// increase
				// From nE > 0 it follows n == 8
				bbc = new RespectGapLengthLeExcess(insertPos, length, buf, nE);
			}
			
			// Create a temporary copy of the FL data file. Experiments with my
			// MS Windows computer equipped with an SSD revealed a better
			// performance than expanding the FL data file in situ.
			final Path tempPath = Paths.get(flDataFile.path.toString() + "_");
			try {
				flDataFile.copyFile(tempPath);
				try (FileIO tempFile = new FileIO(tempPath)) {
					tempFile.position(start);
					flDataFile.position(start);
					
					final int netBufSize = p * n;
					int bufStart = p * m;
					long remaining = nofBlocks * n;
					
					// 0 < netBufSize <= remaining
					do {
						// Load buf
						if (netBufSize > remaining) {
							p = (int) remaining / n;
							buf.limit(p * newN);
							bufStart = p * m;
						}
						buf.position(bufStart);
						tempFile.read(buf);
						remaining -= buf.limit() - bufStart;
						buf.position(bufStart);
						
						// Expand
						// buf.position() == bufStart &&
						// (buf.limit() - bufStart) % n == 0 &&
						// buf.limit() == p * newN.
						bbc.expand();
						
						// Write buf
						buf.position(0);
						flDataFile.write(buf);
					} while (tempFile.position() < size);
				}
			} finally {
				if (Files.exists(tempPath)) {
					try {
						Files.delete(tempPath);
					} catch (IOException e) {
						new FileIOException(tempPath, e);
					}
				}
			}
		}
		
		/**
		 * Accommodates the FL data file around the specified excentric
		 * contraction-only spot.
		 * <p>
		 * If there exists an excess and its length is greater than or equal to
		 * the value of the {@code spot.cLen} argument then the size of the FL
		 * data blocks remains unchanged.
		 * Otherwise, this method increases the size of the FL data blocks and
		 * therefore the size of the FL data file.
		 * <p>
		 * Note that the FL file space and hence the whole database won't work
		 * properly anymore after this method has been executed.
		 * The database should be closed as soon as possible.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB1}.
		 * 
		 * @param  spot The excentric contraction-only spot.
		 *         (The value of the {@code cLen} property of a excentric
		 *         contraction spot is greater than zero.)
		 *
		 * @throws ImplementationRestrictionException If the new size of the FL
		 *         data blocks exceeds {@code Integer.MAX_VALUE}.
		 * @throws FileIOException If an I/O error occurs.
		 */
		final void run(Spot spot) throws ImplementationRestrictionException,
																					FileIOException {
			// Precondition: size > start && spot.cLen > 0
			flDataFile.open();
			try {
				expand(spot.pos, spot.cLen, store.nE);
			} finally {
				flDataFile.close();
			}
		}
	}
	
	/**
	 * Provides the {@link #run} method which accommodates the FL data file
	 * around the specified spots, optionally presenting the current FL data
	 * block.
	 * <p>
	 * The accommodation implemented in this class is inferior to the ones
	 * implemented in the {@link SingleConcentric} and {@link SingleExcentric}
	 * classes but in contrast to the accommodations implemented in those classes
	 * the accommodation implemented in this class works for all cases, not only
	 * for a single special case.
	 *
	 * @author Beat Hoermann
	 */
	private static final class General {
		/**
		 * The WR store as given via the constructor
		 */
		private final WRStore store;
		/**
		 * The underlying file of the FL file space.
		 */
		private final FileIO flDataFile;
		/**
		 * The size of an FL data block, greater than or equal to 8.
		 */
		private final int n;
		/**
		 * The number of excess bytes, &ge; 0 and &le; 6.
		 */
		private final int nE;
		/**
		 * The position within the {@code flDataFile} where the first FL data
		 * block starts.
		 */
		private final int start;
		/**
		 * The size of the {@code flDataFile}.
		 */
		private final long size;
		
		/**
		 * The constructor.
		 * 
		 * @param  store The store, not allowed to be {@code null}.
		 */
		General(WRStore store) {
			this.store = store;
			this.flDataFile = store.flDataFile;
			this.n = store.n;
			this.nE = store.nE;
			final FLFileSpace flFileSpace = store.flFileSpace;
			this.start = (int) flFileSpace.indexToPos(0);
			this.size = flFileSpace.size();
		}
		
		/**
		 * Returns a buffer with a capacity of at least {@code minSize} bytes
		 * but no more than {@code maxSize} or {@code buffer.maxCap()} bytes,
		 * whichever is less.
		 * <p>
		 * The position of the returned buffer is equal to zero and the limit
		 * is equal to max({@code minSize}, min({@code m}, {@code maxSize})),
		 * where {@code m} denotes the value of {@code buffer.maxCap()} / {@code
		 * minSize} * {@code minSize}.
		 * Thus, the limit is divisible by {@code minSize} without remainder if
		 * {@code maxSize} is divisible by {@code minSize} without remainder.
		 * 
		 * @param  buffer The buffer to be reused if possible.
		 * @param  minSize The minimum number of bytes the buffer must be able
		 *         to store.
		 * @param  maxSize The maximum number of byte the buffer mus be able to
		 *         store.
		 * 
		 * @return The byte buffer, never {@code null}.
		 */
		private final ByteBuffer getBuf(Buffer buffer, int minSize, long maxSize){
			final ByteBuffer buf;
			
			final int maxCap = buffer.maxCap();
			if (maxCap < minSize)
				buf = ByteBuffer.allocate(minSize);
			else if (maxCap >= maxSize)
				buf = buffer.buf(maxSize);
			else {
				buf = buffer.buf(maxCap / minSize * minSize);
			}
			
			return buf;
		}
		
		/**
		 * Loads the buffer by reading data from the FL data file at the specified
		 * position.
		 * <p>
		 * Note that if the number of remaining bytes to read is less than
		 * the limit of the specified buffer than the buffer's limit is adjusted
		 * accordingly.
		 * <p>
		 * The buffer is not assumed to be rewinded and this method leaves the
		 * buffer unrewinded.
		 * 
		 * @param  buf The unrewinded buffer.
		 * @param  pos The position.
		 * 
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void loadBuf(ByteBuffer buf, long pos) throws
																					FileIOException {
			final long remaining = size - pos;
			if (buf.limit() > remaining) {
				buf.limit((int) remaining);
			}
			buf.rewind();
			flDataFile.read(buf, pos);
		}
		
		/**
		 * Accommodates the FL data blocks contained in {@code bytes}.
		 * <p>
		 * The byte array {@code bytes} is considered read-and-write.
		 * <p>
		 * This method assumes that {@code bytes} contains at least one FL
		 * data block and that all spots are update-only spots.
		 * 
		 * @param presenter The presenter, may be {@code null}.
		 * @param spots The update-only spots, not allowed to be {@code null} and
		 *        not allowed to be empty.
		 * @param bytes The byte array containing the FL data blocks, not allowed
		 *        to be {@code null}.
		 *        Must contain at least one FL data block.
		 * @param limit The sum of the sizes of the FL data blocks contained in
		 *        {@code bytes}.
		 *
		 * @throws FileIOException If an I/O error occurs while presenting the
		 *         current FL data block.
		 */
		private final void doUpdate(Presenter presenter, List<Spot> spots,
										byte[] bytes, int limit) throws FileIOException {
			// Preconditions: bytes must contain at least one FL data block.
			//                All spots are update-only spots.
			
			// Loop over the buffered FL data blocks.
			int blockStart = 0;
			do {
				// Process current FL data block.
				
				if (bytes[blockStart] >= 0) {
					// No gap!
					
					// Present
					if (presenter != null) {
						presenter.rowData(bytes, blockStart);
					}
					
					// Process update-only spots.
					for (Spot spot : spots) {
						spot.updater.newData(bytes, blockStart + spot.pos);
					}
				}
				// Set start of next FL data block.
				blockStart += n;
			} while (blockStart < limit);
		}
		
		/**
		 * Accommodates the FL data file around the specified update-only spots,
		 * optionally presenting the current FL data block.
		 * <p>
		 * This method assumes that the FL data file contains at least one FL
		 * data block and that all spots are update-only spots.
		 * <p>
		 * The {@code FileIO} instance {@code flDataFile} is considered
		 * read-and-write.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB1}.
		 * 
		 * @param  presenter The client's presenter, may be {@code null}.
		 * @param  spots The update-only spots, not allowed to be {@code null}
		 *         and not allowed to be empty.
		 *
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void doUpdate(Presenter presenter, List<Spot> spots) throws
																					FileIOException {
			// Get buffer.
			final long netSize = size - start;
			// netSize >= n && netSize % n == 0
			final ByteBuffer buf = getBuf(store.gb1, n, netSize);
			// buf0.position() == 0 && buf0.limit() % n == 0
			
			// Initialize byte array.
			final byte[] bytes = buf.array();
			
			// Set reading position of flDataFile.
			flDataFile.position(start);
			
			// Accomodate the FL data file by accommodating a buffered bunch of FL
			// data blocks at a time.
			long filePos = start;
			do {
				// Load a bunch of FL data blocks into the buffer.
				loadBuf(buf, filePos);
				// The value of buf.limit() may be smaller for the last bunch.
				final int limit = buf.limit();
				// Accomodate the buffered bunch of FL data blocks.
				doUpdate(presenter, spots, bytes, limit);
				// Save the buffered bunch of accommodated FL data blocks to the
				// FL data file.
				buf.rewind();
				flDataFile.write(buf, filePos);
				filePos += limit;
			} while (filePos < size);
		}
		
		/**
		 * Accommodates the FL data file around the specified <em>update-only</em>
		 * spots, optionally presenting the current FL data block.
		 * <p>
		 * Note that the format of the FL data file may be corrupted if this
		 * method throws an exception.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB1}.
		 * 
		 * @param  presenter The client's presenter, may be {@code null}.
		 * @param  spots The update-only spots, not allowed to be {@code null}
		 *         and not allowed to be empty.
		 * 
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void update(Presenter presenter, List<Spot> spots) throws
																					FileIOException {
			flDataFile.open();
			try {
				// Precondition: size > start && All spots are update-only spots.
				doUpdate(presenter, spots);
			} finally {
				flDataFile.close();
			}
		}
		
		/**
		 * Accommodates the FL data blocks contained in {@code bytes0} and saves
		 * the accommodated blocks to {@code bytes1}.
		 * <p>
		 * The byte arrays {@code bytes0} and {@code bytes1} are considered
		 * read-only and write-only, respectively.
		 * <p>
		 * This method assumes that {@code bytes0} contains at least one FL
		 * data block.
		 * 
		 * @param presenter The presenter, may be {@code null}.
		 * @param spots The spots, not allowed to be {@code null} and not allowed
		 *        to be empty.
		 * @param bytes0 The byte array containing the FL data blocks, not allowed
		 *        to be {@code null}.
		 *        Must contain at least one FL data block.
		 * @param bytes1 The byte array where the modified FL data blocks are
		 *        saved to, not allowed to be {@code null} and must be of the
		 *        "right" length.
		 * @param limit0 The sum of the sizes of the FL data blocks contained in
		 *        {@code byte0}.
		 * @param newN The new size of the FL data blocks.
		 *
		 * @throws FileIOException If an I/O error occurs while presenting the
		 *         current FL data block.
		 */
		private final void doContract(Presenter presenter, List<Spot> spots,
							byte[] bytes0, byte[] bytes1, int limit0, int newN) throws
																					FileIOException {
			// Precondition: bytes0 must contain at least one FL data block.
			
			// Define some positions within the bytes arrays.
			int blockStart0 = 0;
			int pos0 = blockStart0;
			int blockStart1 = 0;
			int pos1 = blockStart1;
			
			// Loop over the buffered FL data blocks.
			do {
				// Process current FL data block.
				
				// pos0 == blockStart0 && pos1 == blockStart1
				if (bytes0[blockStart0] >= 0) {
					// No gap!
					
					// Present
					if (presenter != null) {
						presenter.rowData(bytes0, blockStart0);
					}

					// Process spots.
					for (Spot spot : spots) {
						// Get properties of current spot.
						final int absSpotPos = blockStart0 + spot.pos;
						final int cLen = spot.cLen;
						final int uLen = spot.uLen;
						final Updater updater = spot.updater;

						// Copy bytes to position of current spot.
						final int d = absSpotPos - pos0;
						System.arraycopy(bytes0, pos0, bytes1, pos1, d);
						pos0 += d;
						// pos0 == absSpotPos
						pos1 += d;

						// Compute new value for pos0. Recall cLen < 0 and cLen > 0
						// for a concentric and excentric contraction, respectively.
						// Note that cLen may be zero.
						final int diff = cLen - uLen;
						if (diff < 0) {
							// cLen < uLen
							pos0 -= diff;
						}

						// Copy new data.
						if (updater != null) {
							updater.newData(bytes1, pos1);
							pos1 += uLen;
						}

						if (diff > 0) {
							// cLen > uLen. Since uLen >= 0 it follows cLen > 0, hence,
							// we have an excentric contraction. Fill with zeros.
							for (int i = 0; i < diff; i++) {
								bytes1[pos1++] = 0;
							}
						}
					}
				}
				// Set starts of next FL data block.
				blockStart0 += n;
				blockStart1 += newN;

				// Terminate current FL data block by copying remaining bytes of FL
				// data block. The following procedure implicitly respects nE > 0
				// and a potential gap.
				final int d0 = blockStart0 - pos0;
				final int d1 = blockStart1 - pos1;
				// If FL data block is a gap then d0 >= 8 and d1 >= 8.
				System.arraycopy(bytes0, pos0, bytes1, pos1, d1 < d0 ? d1 : d0);
				pos0 += d0;
				pos1 += d1;
				// pos0 == blockStart0 && pos1 == blockStart1
			} while (blockStart0 < limit0);
		}
		
		/**
		 * Accommodates the content of the FL data file around the specified
		 * spots, optionally presenting the current FL data block, and saves the
		 * result in the specified {@code FileIO} instance.
		 * <p>
		 * This method assumes that the FL data file contains at least one FL
		 * data block.
		 * <p>
		 * The {@code FileIO} instances {@code flDataFile} and {@code fileIO} are
		 * considered read-only and write-only, respectively.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB1}.
		 * 
		 * @param  presenter The client's presenter, may be {@code null}.
		 * @param  spots The spots, not allowed to be {@code null} and not allowed
		 *         to be empty.
		 * @param  m The net contraction length, hence, the sum of the values
		 *         of the {@code cLen} property of the specified spots.
		 * @param  fileIO The {@code FileIO} instance.
		 *
		 * @throws ImplementationRestrictionException If the new size of the FL
		 *         data blocks exceeds {@code Integer.MAX_VALUE}.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void doContract(Presenter presenter, List<Spot> spots,
																	int m, FileIO fileIO) throws
									ImplementationRestrictionException, FileIOException {
			// Find out the new length of the FL data blocks.
			final int d = n - nE + m;
			if (d < 0) {
				throw new ImplementationRestrictionException("FL data blocks too " +
																	"large for being expanded.");
			}
			final int newN = d > 8 ? d : 8;
			// newN >= 8
			
			// Get buffers.
			final long netSize = size - start;
			// netSize >= n && netSize % n == 0
			final ByteBuffer buf0 = getBuf(store.gb1, n, netSize);
			// buf0.position() == 0 && buf0.limit() % n == 0
			final int limit0 = buf0.limit();
			final ByteBuffer buf1 = ByteBuffer.allocate(limit0 / n * newN);
			// buf1.position() == 0 && buf1.limit() % newN == 0 &&
			// buf1.limit() / newN == limit0 / n
			
			// Initialize byte arrays.
			final byte[] bytes0 = buf0.array();
			final byte[] bytes1 = buf1.array();
			
			// Copy beginning of file.
			final ByteBuffer bufStart = ByteBuffer.allocate(start);
			flDataFile.read(bufStart);
			bufStart.rewind();
			fileIO.write(bufStart);
			
			// Accomodate the content of the FL data file by accommodating a
			// buffered bunch of FL data blocks at a time.
			long filePos = start;
			do {
				// Load a bunch of FL data blocks into the buffer.
				loadBuf(buf0, filePos);
				// The value of buf0.limit() may be smaller for the last bunch. The
				// number of buffered FL data blocks is equal to buf0.limit() / n.
				final int curLimit0 = buf0.limit();
				filePos += curLimit0;
				// Accomodate the buffered bunch of FL data blocks.
				doContract(presenter, spots, bytes0, bytes1, curLimit0, newN);
				// Save the buffered bunch of accommodated FL data blocks to fileIO.
				buf1.rewind();
				if (curLimit0 < limit0) {
					// Adjust limit of buf1 for last bunch.
					buf1.limit(curLimit0 / n * newN);
				}
				fileIO.write(buf1);
			} while (filePos < size);
		}
		
		/**
		 * Accommodates the FL data file around the specified spots, optionally
		 * presenting the current FL data block.
		 * <p>
		 * This method accommodates the FL data file in all kinds of situations.
		 * However, this method is typically invoked in more complex situations
		 * where there are no other more effective methods around, hence, in
		 * situations different from the following two situations:
		 * 
		 * <ul>
		 *    <li>All spots are update-only spots.</li>
		 *    <li>The list of spots contains only one spot and this spot is a
		 *        contraction-only spot and no presenter is specified.</li>
		 * </ul>
		 * <p>
		 * To run properly, the FL data file's {@code FileIO} instance {@code
		 * flDataFile} is not allowed to be already open at the time this method
		 * is invoked.
		 * <p>
		 * Note that the FL data file remains unchanged if this method throws an
		 * exception.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB1}.
		 * 
		 * @param  presenter The client's presenter, may be {@code null}.
		 * @param  spots The spots, not allowed to be {@code null} and not allowed
		 *         to be empty.
		 * @param  m The net contraction length, hence, the sum of the values
		 *         of the {@code cLen} property of the specified spots.
		 * 
		 * @throws ImplementationRestrictionException If the new size of the FL
		 *         data blocks exceeds {@code Integer.MAX_VALUE}.
		 * @throws FileIOException If an I/O error occurs.
		 */
		private final void contract(Presenter presenter, List<Spot> spots,
																						int m) throws
									ImplementationRestrictionException, FileIOException {
			// Create new file.
			FileIO fileIO = new FileIO(Paths.get(flDataFile.path.toString() +
																		"_"), WRITE, CREATE_NEW);
			try {
				flDataFile.open();
				try {
					// Precondition: size > start.
					doContract(presenter, spots, m, fileIO);
				} finally {
					flDataFile.close();
				}
				// The file channel of flDataFile is idle. Close the file channel,
				// in case the data base was opened with an opMode equal to -1 or
				// greater than zero. The file channel must be closed for it to be
				// successfully replaced.
				flDataFile.closeFileChannel();
				
				fileIO.close();
				fileIO.move(flDataFile, REPLACE_EXISTING);
				fileIO = null;
			} finally {
				if (fileIO != null) {
					fileIO.close();
					if (Files.exists(fileIO.path)) {
						fileIO.delete();
					}
				}
			}
		}
		
		/**
		 * Accommodates the FL data file around the specified spots, optionally
		 * presenting the current FL data block.
		 * <p>
		 * To run properly, the FL data file's {@code FileIO} instance {@code
		 * flDataFile} is not allowed to be already open at the time this method
		 * is invoked.
		 * <p>
		 * The FL data file must contain at least one FL data block.
		 * <p>
		 * Note that the format of the FL data file may be corrupted if this
		 * method throws an exception.
		 * <p>
		 * {@linkplain WRStore.GlobalBuffer GB1}.
		 * 
		 * @param  presenter The client's presenter, may be {@code null}.
		 * @param  spots The spots, not allowed to be {@code null} and not allowed
		 *         to be empty.
		 * 
		 * @throws ImplementationRestrictionException If the new size of the FL
		 *         data blocks exceeds {@code Integer.MAX_VALUE}.
		 * @throws FileIOException If an I/O error occurs.
		 */
		final void run(Presenter presenter, List<Spot> spots) throws
									ImplementationRestrictionException, FileIOException {
			// Precondition: size > start.
			
			// Find out if there is a contraction spot and if there exist
			// contraction spots then find out how much the size of the FL data
			// block changes.
			boolean cSpotFound = false;
			int m = 0;
			for (Spot spot : spots) {
				final int cLen = spot.cLen;
				if (cLen != 0) {
					cSpotFound = true;
					m += cLen;
				}
			}
			
			if (cSpotFound)
				// There is at least one contraction spot.
				contract(presenter, spots, m);
			else {
				// There exists at least one spot and all spots are update-only
				// spots.
				update(presenter, spots);
			}
		}
	}
	
	/**
	 * Accommodates the FL data file around the specified spots, optionally
	 * presenting the current FL data block.
	 * This method has no effect if the FL data file contains no FL data blocks.
	 * <p>
	 * To run properly, the FL data file's {@code FileIO} instance {@code
	 * flDataFile} is not allowed to be already open at the time this method is
	 * invoked.
	 * <p>
	 * Note that the FL file space and hence the whole database won't work
	 * properly anymore after this method has been executed.
	 * The database should be closed as soon as possible.
	 * <p>
	 * Note also that the format of the FL data file may be corrupted if this
	 * method throws an exception different from the listed {@code
	 * IllegalArgumentException} and {@code
	 * ImplementationRestrictionException}.
	 * (Different exceptions from the listed {@code FileIOException} may be
	 * thrown by client implementations of the {@link Presenter#rowData} or
	 * {@link Updater#newData} methods.)
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1}.
	 * 
	 * @param  store The WR store, not allowed to be {@code null}.
	 * @param  presenter The client's presenter, may be {@code null}.
	 * @param  spots The spots, not allowed to be {@code null} and not allowed
	 *         to be empty.
	 * 
	 * @throws IllegalArgumentException If {@code spots} is {@code null} or
	 *         empty.
	 * @throws ImplementationRestrictionException If the new size of the FL data
	 *         blocks exceeds {@code Integer.MAX_VALUE}.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void run(WRStore store, Presenter presenter,
																		List<Spot> spots) throws
						IllegalArgumentException, ImplementationRestrictionException,
																					FileIOException {
		if (spots == null || spots.size() == 0) {
			throw new IllegalArgumentException();
		}
		if (store.flFileSpace.nofBlocks() > 0) {
			// There is at least one FL data block.
			if (presenter == null && spots.size() == 1 &&
																spots.get(0).updater == null) {
				// The list of spots contains only one spot and this spot is a
				// contraction-only spot and no presenter is specified.
				final Spot spot = spots.get(0);
				final int cLen = spot.cLen;
				if (cLen < 0)
					new SingleConcentric(store).run(spot);
				else {
					new SingleExcentric(store).run(spot);
				}
			}
			else {
				new General(store).run(presenter, spots);
			}
		}
	}
}