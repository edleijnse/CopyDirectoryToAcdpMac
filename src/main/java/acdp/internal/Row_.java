/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import java.util.Arrays;
import java.util.Iterator;

import acdp.Column;
import acdp.Ref;
import acdp.Row;
import acdp.Table;
import acdp.exceptions.ACDPException;

/**
 * Implements the {@link Row} interface.
 *
 * @author Beat Hoermann
 */
public final class Row_ implements Row {
	private final Table_ table;
	private final Column<?>[] cols;
	private final Object[] vals;
	private Ref_ ref;
	private short i;
	
	/**
	 * Constructs a row.
	 * 
	 * @param table The table this row belongs to.
	 * @param cols The columns of the row, not allowed to be {@code null} but
	 *        may be an empty array.
	 *        The columns must be columns of {@code table}.
	 * @param vals The array of values, not allowed to be {@code null} and not
	 *        allowed to be empty.
	 *        The order and length of the array must be in strict accordance
	 *        with the array of columns of the {@code cols} argument or with
	 *        the table definition if {@code cols} is empty.
	 */
	public Row_(Table_ table, Column<?>[] cols, Object[] vals) {
		this.table = table;
		this.cols = cols;
		this.vals = vals;
		this.ref = null;
		this.i = 0;
	}
	
	/**
	 * Sets the reference of the row within the table.
	 * 
	 * @param  ref The reference.
	 */
	public final void setRef(Ref_ ref) {
		this.ref = ref;
	}
	
	@Override
	public final Table getTable() {
		return table;
	}
	
	@Override
	public final Column<?>[] getColumns() {
		if (cols.length == 0)
			return table.getColumns();
		else {
			return Arrays.copyOf(cols, cols.length);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public final <T> T get(Column<T> col) throws IllegalArgumentException {
		try {
			if (cols.length == 0) {
				final Column_<?> col_ = (Column_<?>) col;
				if (col_.table() != table) {
					throw new Exception();
				}
				return (T) vals[col_.index()];
			}
			else {
				// Quick return if cols[i] == col.
				if (i < cols.length && cols[i] == col)
					return (T) vals[i++];
				else {
					i = 0;
					do {
						if (cols[i] == col) {
							return (T) vals[i++];
						}
						i++;
					} while (true);
				}
			}
		} catch (Exception e) {
			if (col == null) 
				throw new IllegalArgumentException(ACDPException.prefix(table) +
																				"Column is null");
			else {
				throw new IllegalArgumentException(ACDPException.prefix(table,
													(Column_<?>) col) + "The row has no " +
													"value for the specified column.");
			}
		}
	}
	
	@Override
	public final Object get(int index) throws IndexOutOfBoundsException {
		return vals[index];
	}
	
	@Override
	public final Ref getRef() {
		return ref;
	}

	@Override
	public final Iterator<Object> iterator() {
		return Arrays.asList(vals).iterator();
	}
}
