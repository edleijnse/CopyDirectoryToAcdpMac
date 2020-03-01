/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.nio.ByteBuffer;
import java.util.Objects;

import acdp.exceptions.ACDPException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.MaximumException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.Column_;
import acdp.internal.FileIOException;
import acdp.internal.IUnit;
import acdp.internal.store.Bag;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.misc.Utils;
import acdp.types.Type;

/**
 * Given a particular column of a particular table, a column updater updates the
 * values stored in that column with values returned by a given instance of
 * a class implementing the {@link IValueProvider} interface.
 *
 * @author Beat Hoermann
 */
final class ColUpdater extends ColProcessor {
	/**
	 * Defines a value provider.
	 * <p>
	 * The {@link #getValue} method of a value provider is invoked for each row
	 * of the table.
	 * 
	 * @author Beat Hoermann
	 */
	interface IValueProvider {
		/**
		 * Returns a value for the specifed column and for the next row of the
		 * table.
		 * <p>
		 * Depending on the concrete implementation, this method may throw an
		 * exception that is different from the listed {@code FileIOException}.
		 * 
		 * @param  ci The column info object.
		 * @param  bitmap The bitmap of the row.
		 * @param  bag The bag containing the stored FL column data.
		 * 
		 * @return The value.
		 * 
		 * @throws FileIOException If an I/O error occurs.
		 */
		Object getValue(WRColInfo ci, long bitmap,Bag bag) throws FileIOException;
	}
	
	private final IUnit unit;
	private final IValueProvider valueProvider;
	private final Bag bag0;
	private final byte[] colData0;
	private final ByteBuffer buf;
	private final byte[] colData;
	private final Bag bag;
	
	/**
	 * The constructor.
	 * 
	 * @param  store The store, not allowed to be {@code null}.
	 * @param  col The column to be updated, not allowed to be {@code null}.
	 *         The column must be a column of the table.
	 * @param  valueProvider The value provider, not allowed to be {@code null}.
	 * @param  unit The unit.
	 * 
	 * @throws NullPointerException If one of the first three arguments is equal
	 *         to {@code null}.
	 * @throws IllegalArgumentException If the column is not a column of the
	 *         table.
	 */
	ColUpdater(WRStore store, Column_<?> col, IValueProvider valueProvider,
				IUnit unit) throws NullPointerException, IllegalArgumentException {
		super(store, col);
		this.valueProvider = Objects.requireNonNull(valueProvider, ACDPException.
										prefix(store.table) + "Value provider is null.");
		this.unit = unit;
		colData0 = getBuf().array();
		bag0 = new Bag(colData0);
		buf = ByteBuffer.allocate(len);
		colData = buf.array();
		bag = new Bag(colData);
	}

	/**
	 * Receives a value for the given column from the given {@linkplain
	 * IValueProvider value provider}, converts it to its FL data and saves the
	 * FL data to the FL data file at the specified position.
	 * <p>
	 * If the value returned by the value provider is not {@linkplain
	 * Type#isCompatible compatible} with the type of the column then this
	 * method throws an exception, however, this may be an exception of a type
	 * not listed below.
	 * <p>
	 * Depending on the concrete implementation of the value provider, this
	 * method may throw an exception that is different from the listed
	 * exceptions.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB2},
	 * {@linkplain WRStore.GlobalBuffer GB3}.
	 * 
	 * @param  bitmap0 The stored bitmap of the row or 0 if the FL data consists
	 *         of the FL column data only.
	 * @param  rowPos The position within the FL data file where the row starts.
	 * @param  colPos The position within the FL data file where the column
	 *         starts.
	 * 
	 * @throws IllegalArgumentException If the length of the byte representation
	 *         of the new value (or one of the elements if the new value is an
	 *         array value) exceeds the maximum length allowed by the simple
	 *         column type.
	 *         This exception also happens if the new value is a reference and
	 *         the reference points to a row that does not exist within the
	 *         referenced table or if the reference points to a row gap or if
	 *         the new value is an array of references and this condition is
	 *         satisfied for at least one of the references contained in the
	 *         array.
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
	@Override
	protected final void process(long bitmap0, long rowPos, long colPos) throws
																					FileIOException {
		// Set colData to be equal to colData0.
		System.arraycopy(colData0, 0, colData, 0, len);
		
		// Get new value and convert back to bitmap and FL column data.
		final long bitmap = ci.o2b.convert(valueProvider.getValue(ci, bitmap0,
															bag0), bitmap0, bag0, unit, bag);
		// Write new bitmap.
		if (bitmap != bitmap0) {
			Utils.unsToBytes(bitmap, store.nBM, bmBufArr);
			bmBuf.rewind();
			flDataFile.write(bmBuf, rowPos);
		}
		
		// Write new FL column data;
		if (!Utils.equals(colData0, colData, 0, len)) {
			buf.rewind();
			flDataFile.write(buf, colPos);
		}
	}
}
