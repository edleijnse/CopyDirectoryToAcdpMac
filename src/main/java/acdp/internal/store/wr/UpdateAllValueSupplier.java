/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.util.Objects;

import acdp.Table.ValueSupplier;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.ImplementationRestrictionException;
import acdp.exceptions.MaximumException;
import acdp.exceptions.UnitBrokenException;
import acdp.internal.Column_;
import acdp.internal.FileIOException;
import acdp.internal.store.Bag;
import acdp.internal.store.wr.ColUpdater.IValueProvider;
import acdp.internal.store.wr.WRStore.WRColInfo;
import acdp.types.Type;

/**
 * Given a particular column of a particular table this update operation updates
 * the values stored in that column with values returned by an injected
 * {@linkplain ValueSupplier value supplier}.
 * <p>
 * If you need to know the stored values then use the {@link
 * UpdateAllValueChanger} class.
 *
 * @author Beat Hoermann
 */
final class UpdateAllValueSupplier extends GenericWriteOp {
	private final ColUpdater colUpdater;

	/**
	 * The constructor.
	 * 
	 * @param  store The store, not allowed to be {@code null}.
	 * @param  col The column to be updated, not allowed to be {@code null}.
	 *         The column must be a column of the table.
	 * @param  valueSupplier The value supplier, not allowed to be {@code null}.
	 * 
	 * @throws NullPointerException If one of the arguments is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If the column is not a column of the
	 *         table.
	 */
	UpdateAllValueSupplier(WRStore store, Column_<?> col,
													final ValueSupplier<?> valueSupplier) {
		super(store);
		Objects.requireNonNull(valueSupplier, ACDPException.prefix(table) +
																	"Value supplier is null.");
		this.colUpdater = new ColUpdater(store, col,
				new IValueProvider() {
					@Override
					public Object getValue(WRColInfo ci, long bitmap, Bag bag) throws
																					FileIOException {
						return valueSupplier.supplyValue();
					}
				}, unit);
	}
	
	/**
	 * Given a particular column of a particular table this method updates the
	 * values stored in this column with values returned by the value supplier
	 * passed via the constructor.
	 * <p>
	 * If a value returned by the value supplier is not {@linkplain
	 * Type#isCompatible compatible} with the type of the given column then
	 * this method throws an exception, however, this may be an exception of a
	 * type not listed below.
	 * <p>
	 * Depending on the concrete implementation of the value supplier, this
	 * method may throw an exception that is different from the listed
	 * exceptions.
	 * <p>
	 * {@linkplain WRStore.GlobalBuffer GB1},
	 * {@linkplain WRStore.GlobalBuffer GB2},
	 * {@linkplain WRStore.GlobalBuffer GB3}.
	 * 
	 * @param  params The parameters of this update operation, ignored.
	 * 
	 * @return The {@code null} value.
	 * 
	 * @throws IllegalArgumentException If the length of the byte representation
	 *         of a new value (or one of the elements if the new value is an
	 *         array value) exceeds the maximum length allowed by the simple
	 *         column type.
	 *         This exception also happens if the new value is a reference
	 *         and the reference points to a row that does not exist within the
	 *         referenced table or if the reference points to a row gap or if
	 *         the new value is an array of references and this condition is
	 *         satisfied for at least one of the references contained in the
	 *         array.
	 * @throws ImplementationRestrictionException If the number of row gaps is
	 *         greater than {@code Integer.MAX_VALUE}.
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
	protected final Object body(Object params) throws FileIOException {
		colUpdater.run();
		return null;
	}
}
