/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.util.Objects;

import acdp.ColVal;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.MaximumException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.FileIOException;
import acdp.internal.store.wr.FLDataReader.FLData;
import acdp.internal.store.wr.FLDataReader.IFLDataReader;

/**
 * For a given store this update operation updates all rows of a table.
 * <p>
 * Within a column, all rows are updated with the same value.
 * Create a {@code UpdateAllValueSupplier} or {@code UpdateAllValueChanger}
 * operation to update each row with a different value.
 *
 * @author Beat Hoermann
 */
final class UpdateAllColVals extends Update {
	/**
	 * The constructor.
	 * 
	 * @param store The store housing the row that must be updated, not allowed
	 *        to be {@code null}.
	 */
	UpdateAllColVals(WRStore store) {
		super(store);
	}

	/**
	 * Updates the values of the specified columns with the specified new values
	 * in all rows of the table.
	 * <p>
	 * The new values must be {@linkplain acdp.types.Type#isCompatible
	 * compatible} with the type of their columns.
	 * This method does not explicitly check this precondition.
	 * In any case, if this precondition is not satisfied then this method
	 * throws an exception, however, this may be an exception of a type not
	 * listed below.
	 * 
	 * @param  colValsObj The array of column values.
	 * 
	 * @return The {@code null} value.
	 * 
	 * @throws NullPointerException If the array of column values is equal to
	 *         {@code null} or contains an element equal to {@code null}.
	 * @throws IllegalArgumentException If the array of column values contains
	 *         at least one column that is not a column of the table.
	 *         This exception also happens if for at least one value the length
	 *         of the byte representation of the value (or one of the elements
	 *         if the value is an array value) exceeds the maximum length
	 *         allowed by this type.
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
	@Override
	protected final Object body(Object colValsObj) throws NullPointerException,
								IllegalArgumentException, MaximumException,
								CryptoException, UnitBrokenException, FileIOException {
		// params != null
		final ColVal<?>[] colVals = (ColVal[]) Objects.requireNonNull(
													colValsObj, ACDPException.prefix(table) +
															"Array of column values is null.");
		if (colVals.length > 0) {
			final long nofBlocks = store.flFileSpace.nofBlocks();
			IFLDataReader flDataReader = FLDataReader.createNextFLDataReader(
										getCols(colVals), store, 0, nofBlocks, store.gb1);
			for (long index = 0; index < nofBlocks; index++) {
				final FLData flData = flDataReader.readFLData(index);
				// flData == null <=> row gap
				if (flData != null) {
					update(index, flData, colVals);
				}
			}
		}
		
		return null;
	}
}