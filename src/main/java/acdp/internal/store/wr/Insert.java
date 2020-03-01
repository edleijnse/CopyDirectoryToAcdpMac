/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import acdp.exceptions.ACDPException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.MaximumException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.Buffer;
import acdp.internal.FileIOException;
import acdp.internal.Ref_;
import acdp.internal.Table_;
import acdp.internal.misc.Utils_;
import acdp.internal.store.Bag;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.misc.Utils;

/**
 * The insert operation inserts a new row into a given store.
 * Note that inserting a new row means to provide values for each column of
 * the row.
 * <p>
 * The {@code null} value is allowed except for ST or A[ST] parametrized such
 * that the simple type forbids the {@code null} value.
 * Trying to insert a {@code null} value into a column that forbids the
 * {@code null} value raises a {@code NullPointerException}.
 * 
 * @author Beat Hoermann
 */
final class Insert extends GenericWriteOp {
	/**
	 * The number of columns.
	 */
	private final int nofCols;
	/**
	 * The first global buffer.
	 */
	private final Buffer gb1;
	
	/**
	 * The constructor.
	 * 
	 * @param store The store into which the values must be inserted, not
	 *        allowed to be {@code null}.
	 */
	Insert(WRStore store) {
		super(store);
		nofCols = store.colInfoArr.length;
		gb1 = store.gb1;
	}
	
	/**
	 * Writes all outrow data of the specified values to the VL data file and
	 * puts the FL data into the specified byte array.
	 * <p>
	 * The values must be {@linkplain Table_#isCompatible(Object[]) compatible}
	 * with this table.
	 * This method does not explicitly check this precondition.
	 * In any case, if this precondition is not satisfied then this method
	 * throws an exception, however, this may be an exception of a type not
	 * listed below.
	 * 
	 * @param  values The values to insert into the row.
	 * @param  flData The FL data, not allowed to be {@code null}.
	 * 
	 * @return The bitmap of the row.
	 * 
	 * @throws NullPointerException If a value (a simple value or an element of
	 *         an array value) is equal to {@code null} but the column type
	 *         forbids the {@code null} value.
	 * @throws IllegalArgumentException If for at least one value the length of
	 *         the byte representation of the value (or one of the elements if
	 *         the value is an array value) exceeds the maximum length allowed
	 *         by the simple column type.
	 *         This exception also happens if the value is an array value and
	 *         the size of the array exceeds the maximum length allowed by the
	 *         array column type.
	 *         Last, this exception also happens if for at least one value the
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
	 *         This exception never happens if the WR database does not apply
	 *         encryption.
	 * @throws UnitBrokenException If recording before data fails.
	 * @throws FileIOException If an I/O error occurs.
	 */
	private final long insertValues(Object[] values, byte[] flData) throws
											NullPointerException, IllegalArgumentException,
											MaximumException, CryptoException,
														UnitBrokenException, FileIOException {
		// values != null && row != null
		long bitmap = 0L;
		final WRColInfo[] colInfoArr = store.colInfoArr;
		final Bag bag = new Bag(flData);
		for (int i = 0; i < colInfoArr.length; i++) {
			final WRColInfo ci = colInfoArr[i];
			bag.offset = ci.offset;
			bitmap = ci.o2b.convert(values[i], bitmap, null, unit, bag);
		}
		return bitmap;
	}

	/**
	 * Inserts the specified values into the table.
	 * <p>
	 * The specified array of values must be {@linkplain
	 * Table_#isCompatible(Object[]) compatible} with this table.
	 * This method does not explicitly check this precondition.
	 * In any case, if this precondition is not satisfied then this method
	 * throws an exception, however, this may be an exception of a type not
	 * listed below.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1},
	 * {@linkplain WRStore.GlobalBuffer GB2},
	 * {@linkplain WRStore.GlobalBuffer GB3}.
	 * 
	 * @param  values The values to insert into the row.
	 * 
	 * @return The reference to the inserted row.
	 * 
	 * @throws NullPointerException If {@code values} is {@code null} or if a
	 *         value (a simple value or an element of an array value) is equal
	 *         to {@code null} but the column type forbids the {@code null}
	 *         value.
	 * @throws IllegalArgumentException If the number of values is not equal to
	 *         the number of columns of the table.
	 *         This exception also happens if for at least one value the length
	 *         of the byte representation of the value (or one of the elements if
	 *         the value is an array value) exceeds the maximum length allowed
	 *         by the simple column type.
	 *         Furthermore, this exception happens if the value is an array value
	 *         and the size of the array exceeds the maximum length allowed by
	 *         the array column type.
	 *         Last, this exception happens if for at least one value the value
	 *         is a reference and the reference points to a row that does not
	 *         exist within the referenced table or if the reference points to a
	 *         row gap or if the value is an array of references and this
	 *         condition is satisfied for at least one of the references
	 *         contained in the array.
	 * @throws MaximumException If a new memory block in the VL file space must
	 *         be allocated and its file position exceeds the maximum allowed
	 *         position or if the maximum value of the reference counter of a
	 *         referenced row is exceeded or if the maximum number of rows for
	 *         this table is exceeded.
	 * @throws CryptoException If encryption fails.
	 *         This exception never happens if the WR database does not apply
	 *         encryption.
	 * @throws UnitBrokenException If recording before data fails.
	 * @throws FileIOException If an I/O error occurs.
	 */
	@Override
	protected final Object body(Object values) throws NullPointerException,
								IllegalArgumentException, MaximumException,
								CryptoException, UnitBrokenException, FileIOException {
		Objects.requireNonNull(values, ACDPException.prefix(table) +
																"The array of values is null.");
		if (nofCols != ((Object[]) values).length) {
			// Never happens if the values are compatible with this table.
			throw new IllegalArgumentException(ACDPException.prefix(table) +
													"The number of values to be inserted " +
													"into the table differs from the " +
													"number of columns of the table.");
		}
		
		// Create the buffer and fill it with zeros.
		final int n = store.n;
		final ByteBuffer flDataBuffer;
		if (n > gb1.maxCap())
			flDataBuffer = ByteBuffer.allocate(n);
		else {
			flDataBuffer = gb1.buf(n);
			Arrays.fill(flDataBuffer.array(), 0, n, (byte) 0);
		}
		final byte[] flData = flDataBuffer.array();
		// flDataBuffer.position() == 0 && flDataBuffer.limit() == n &&
		// flData == flDataBuffer.array() && flData filled with zeros.

		// Convert and put values into flData and get header bitmap.
		final long bitmap = insertValues((Object[]) values, flData);

		// Allocate file space for the row.
		final long pos = store.flFileSpace.allocate(unit);

		// Convert to row index and check if valid.
		final long rowIndex = store.posToRi(pos);
		if (rowIndex > Utils_.bnd8[store.nobsRowRef]) {
			String str = "Maximum of " + Utils_.bnd8[store.nobsRowRef] +
																				" rows exceeded.";
			if (store.nobsRowRef < 8) {
				str += " You may want to refactor the table with a higher value " +
																			"of \"nobsRowRef\".";
			}
			throw new MaximumException(table, str);
		}

		// Put bitmap into flData.
		Utils.unsToBytes(bitmap, store.nBM, flData);
		
		// Reference counter is already set to zero because flData is initialized
		// with zeros.
		
		// Write FL data. Before data already written when file space was
		// allocated.
		flDataFile.write(flDataBuffer, pos);
		
		return new Ref_(rowIndex);
	}
}