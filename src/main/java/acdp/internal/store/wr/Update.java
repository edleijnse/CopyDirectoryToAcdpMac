/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import acdp.Column;
import acdp.ColVal;
import acdp.Ref;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.IllegalReferenceException;
import acdp.exceptions.MaximumException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.FileIOException;
import acdp.internal.Ref_;
import acdp.internal.store.Bag;
import acdp.internal.store.wr.FLDataHelper.Block;
import acdp.internal.store.wr.FLDataHelper.Blocks;
import acdp.internal.store.wr.FLDataHelper.Section;
import acdp.internal.store.wr.FLDataReader.ColOffset;
import acdp.internal.store.wr.FLDataReader.FLData;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.misc.Utils;

/**
 * For a given store the update operation updates some or all values of an
 * existing row.
 *
 * @author Beat Hoermann
 */
class Update extends GenericWriteOp {
	/**
	 * The constructor.
	 * 
	 * @param store The store housing the row that must be updated, not allowed
	 *        to be {@code null}.
	 */
	Update(WRStore store) {
		super(store);
	}
	
	/**
	 * Writes each of the specified blocks to the table's FL data file.
	 * 
	 * @param  pos The position of the row within the table's FL data file.
	 * @param  flData0 The stored FL data.
	 * @param  bytes The byte array containing the updated FL data.
	 * @param  blocks The blocks.
	 *
	 * @throws UnitBrokenException If recording before data fails.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void writeBlocks(long pos, FLData flData0, byte[] bytes,
						Blocks blocks) throws UnitBrokenException, FileIOException {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		for (Block b : blocks) {
			// Compute the position within the FL data file where the block
			// starts.
			final long absPos = pos + b.offset();
			// Get the first column of the block.
			final WRColInfo ci = b.ci();
			// Compute the index within flData0.bytes and bytes where the FL data
			// for this block starts.
			final int offset = ci != null ? flData0.colOffsets[flData0.colMap.get(
																				ci.col)].offset : 0;
			// Get the length of the block.
			final int length = b.length();
			// Record before data.
			if (unit != null) {
				unit.record(flDataFile, absPos, flData0.bytes, offset, length);
			}
			// Write block to FL data file. Set the limit of the buffer first and
			// then its position to avoid an IllegalArgumentException in
			// buf.position(offset).
			buf.limit(offset + length);
			buf.position(offset);
			flDataFile.write(buf, absPos);
		}
	}
	
	/**
	 * Writes the whole row to the table's FL data file.

	 * @param  pos The position of the row within the table's FL data file.
	 * @param  bytes0 The byte array containing the stored row.
	 * @param  bytes The byte array containing the updated row.
	 *
	 * @throws UnitBrokenException If recording before data fails.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void writeWhole(long pos, byte[] bytes0, byte[] bytes) throws
														UnitBrokenException, FileIOException {
		// Record before data.
		if (unit != null) {
			unit.record(flDataFile, pos, bytes0);
		}
		// Write row to FL data file.
		flDataFile.write(ByteBuffer.wrap(bytes), pos);
	}
	
	/**
	 * Writes the updated FL data to the table's FL data file.
	 * 
	 * @param  index The index of the FL memory block housing the row.
	 * @param  flData0 The stored FL data.
	 * @param  bytes The byte array containing the updated FL data.
	 * 
	 * @throws UnitBrokenException If recording before data fails.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final void persist(long index, FLData flData0, byte[] bytes) throws
														UnitBrokenException, FileIOException {
		final long pos = store.flFileSpace.indexToPos(index);
		final byte[] bytes0 = flData0.bytes;
		
		if (bytes.length < store.n) {
			// Row was read in blocks, write FL data back in blocks. Do not write
			// unchanged FL data.
			
			// Create the sections of the updated FL data.
			final List<Section> sections = new ArrayList<>(
																flData0.colOffsets.length + 1);
			if (!Utils.equals(bytes0, bytes, 0, store.nBM)) {
				sections.add(new Section(store));
			}
			for (ColOffset co : flData0.colOffsets) {
				final WRColInfo ci = co.ci;
				if (!Utils.equals(bytes0, bytes, co.offset, ci.len)) {
					sections.add(new Section(ci));
				}
			}
			
			if (!sections.isEmpty()) {
				writeBlocks(pos, flData0, bytes, new Blocks(sections));
			}
		}
		else {
			// Row was read as a whole, write changes back as a whole row. Even
			// unchanged FL data gets written, as well as FL data that was never
			// part of the update operation.
			writeWhole(pos, bytes0, bytes);
		}
	}
	
	/**
	 * Gets the columns from the specified array of column values.
	 * 
	 * @param  colVals The column values, not allowed to be {@code null}.
	 * 
	 * @return The columns, never {@code null}.
	 * 
	 * @throws NullPointerException if at least one column value is {@code null}.
	 */
	protected final Column<?>[] getCols(ColVal<?>[] colVals) throws
																			NullPointerException {
		final int n = colVals.length;
		final Column<?>[] cols = new Column<?>[n];
		for (int i = 0; i < n; i++) {
			cols[i] = colVals[i].column();
		}
		return cols;
	}


	/**
	 * Updates the values of the specified columns with the specified new values.
	 * <p>
	 * The new values must be {@linkplain acdp.types.Type#isCompatible
	 * compatible} with the type of their columns.
	 * This method does not explicitly check this precondition.
	 * In any case, if this precondition is not satisfied then this method
	 * throws an exception, however, this may be an exception of a type not
	 * listed below.
	 * 
	 * @param  index The index of the FL memory block housing the row.
	 * @param  flData0 The stored FL data.
	 * @param  colVals The array of column values.
	 * 
	 * @throws IllegalArgumentException If for at least one value the length of
	 *         the byte representation of the value (or one of the elements if
	 *         the value is an array value) exceeds the maximum length allowed
	 *         by this type.
	 *         Furthermore, this exception happens if for at least one value the
	 *         value is a reference and the reference points to a row that does
	 *         not exist within the referenced table or if the reference points
	 *         to a row gap or if the value is an array of references and this
	 *         condition is satisfied for at least one of the references
	 *         contained in the array.
	 * @throws MaximumException If a new memory block in the VL file space must
	 *         be allocated and its file position exceeds the maximum allowed
	 *         position or if the maximum value of the reference counter of a
	 *         referenced row is exceeded.
	 * @throws CryptoException If encryption fails.
	 *         This exception never happens if encryption is not applied.
	 * @throws UnitBrokenException If recording before data fails.
	 * @throws FileIOException If an I/O error occurs.
	 */
	protected final void update(long index, FLData flData0,
																	ColVal<?>[] colVals) throws
								IllegalArgumentException, MaximumException,
								CryptoException, UnitBrokenException, FileIOException {
		// Initialize bitmap, bag0 and bag.
		final byte[] bytes0 = flData0.bytes;
		long bitmap = Utils.unsFromBytes(bytes0, store.nBM);
		final Bag bag0 = new Bag(bytes0);
		final byte[] bytes = Arrays.copyOf(bytes0, bytes0.length);
		final Bag bag = new Bag(bytes);
		final ColOffset[] colOffsets = flData0.colOffsets;
		
		// Update each column value.
		for (int i = 0; i < colVals.length; i++) {
			final ColVal<?> colVal = colVals[i];
			
			final ColOffset co =  colOffsets[i];
			final int offset = co.offset;
			bag0.offset = offset;
			bag.offset = offset;
			
			bitmap = co.ci.o2b.convert(colVal.value(), bitmap, bag0, unit, bag);
		}
		
		// Put updated bitmap into bag.
		Utils.unsToBytes(bitmap, store.nBM, bag.bytes);
		
		// Persist updated FL data.
		persist(index, flData0, bytes);
	}

	/**
	 * Updates the values of the specified columns of the specified row with the
	 * specified new values.
	 * <p>
	 * The new values must be {@linkplain acdp.types.Type#isCompatible
	 * compatible} with the type of their columns.
	 * This method does not explicitly check this precondition.
	 * In any case, if this precondition is not satisfied then this method
	 * throws an exception, however, this may be an exception of a type not
	 * listed below.
	 * 
	 * @param  params The reference to the row as well as the array of column
	 *         values.
	 * 
	 * @return The {@code null} value.
	 * 
	 * @throws NullPointerException If {@code params} is {@code null} or if
	 *         either the reference to the row or the array of column values,
	 *         both casted from {@code params}, is equal to {@code null}.
	 *         Furthermore, this exception happens if the array of column values
	 *         contains an element equal to {@code null}.
	 * @throws IllegalArgumentException If the array of column values contains
	 *         at least one column that is not a column of the table.
	 *         This exception also happens if for at least one value the length
	 *         of the byte representation of the value (or one of the elements if
	 *         the value is an array value) exceeds the maximum length allowed
	 *         by this type.
	 *         Furthermore, this exception happens if for at least one value the
	 *         value is a reference and the reference points to a row that does
	 *         not exist within the referenced table or if the reference points
	 *         to a row gap or if the value is an array of references and this
	 *         condition is satisfied for at least one of the references
	 *         contained in the array.
	 * @throws IllegalReferenceException If the specified reference points to
	 *         a row that does not exist within the table or if the reference
	 *         points to a row gap.
	 *         Such a situation cannot occur if {@code ref} is a {@linkplain Ref
	 *         valid} reference.
	 * @throws MaximumException If a new memory block in the VL file space must
	 *         be allocated and its file position exceeds the maximum allowed
	 *         position or if the maximum value of the reference counter of a
	 *         referenced row is exceeded.
	 * @throws CryptoException If encryption fails.
	 *         This exception never happens if encryption is not applied.
	 * @throws UnitBrokenException If recording before data fails.
	 * @throws FileIOException If an I/O error occurs.
	 */
	@Override
	protected Object body(Object params) throws NullPointerException,
									IllegalArgumentException, IllegalReferenceException,
									MaximumException, CryptoException,
														UnitBrokenException, FileIOException {
		final Ref_ ref = (Ref_) ((Object[]) Objects.requireNonNull(params,
						ACDPException.prefix(table) + "Parameter array is null."))[0];
		final ColVal<?>[] colVals = (ColVal[]) ((Object[]) params)[1];

		if (ref == null)
			throw new NullPointerException(ACDPException.prefix(table) +
																	"Reference to row is null.");
		else if (colVals == null)
			throw new NullPointerException(ACDPException.prefix(table) +
															"Array of column values is null.");
		else if (colVals.length > 0) {
			// Convert the reference to the index of the FL memory block.
			final long index = store.refToRi(ref) - 1;
			// Read the bitmap and the FL column data, hence, the stored FL data.
			final FLData flData0 =  FLDataReader.createRandomFLDataReader(
												getCols(colVals), store).readFLData(index);
			if (flData0 == null) {
				// Row gap!
				throw new IllegalReferenceException(table, ref, true);
			}
			// Do the job.
			update(index, flData0, colVals);
		}
		
		return null;
	}
}