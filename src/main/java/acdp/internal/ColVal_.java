/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import acdp.ColVal;

/**
 * Implements a column value pair.
 *
 * @author Beat Hoermann
 */
public final class ColVal_<T> implements ColVal<T> {
	/**
	 * The column.
	 */
	private final Column_<T> col;
	/**
	 * The value.
	 */
	private final T val;
	
	/**
	 * The constructor.
	 * 
	 * @param col the column, not allowed to be {@code null}.
	 * @param val the value.
	 */
	public ColVal_(Column_<T> col, T val) {
		this.col = col;
		this.val = val;
	}
	
	@Override
	public Column_<T> column() {
		return col;
	}
	
	@Override
	public T value() {
		return val;
	}
}
