/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import java.util.Objects;

import acdp.Column;
import acdp.ColVal;
import acdp.Information.ColumnInfo;
import acdp.design.SimpleType;
import acdp.internal.types.Type_;
import acdp.types.Type;

/**
 * Implements a column of a table.
 *
 * @author Beat Hoermann
 */
public final class Column_<T> implements Column<T> {
	/**
	 * The name of the column, never {@code null} and never an empty string
	 * after the {@link #initialize} method has been called.
	 * <p>
	 * The value can be considered final after the {@link #initialize} method
	 * has been called.
	 */
	private String name;
	/**
	 * The type of the column, never {@code null}.
	 */
	private final Type_ type;
	/**
	 * The referenced table, never an empty string.
	 * After the {@link #initialize} method has been called, this value is
	 * {@code null} if and only if this column does not reference a table.
	 * <p>
	 * The value can be considered final after the {@link #initialize} method
	 * has been called.
	 */
	private String refdTable;
	/**
	 * The column's table, never {@code null}.
	 * <p>
	 * The value can be considered final after the {@link #initialize} method
	 * has been called.
	 */
	private Table_ table;
	/**
	 * The index of the column within the table's table definition.
	 * <p>
	 * The value can be considered final after the {@link #initialize} method
	 * has been called.
	 */
	private int index;
	
	/**
	 * Just implements the {@code WRColInfo} interface.
	 * 
	 * @author Beat Hoermann
	 */
	public static final class Info implements ColumnInfo {
		private final Column_<?> col;
		private final Type_ type;
		
		/**
		 * Creates the information object of the specified column.
		 * 
		 * @param  col The column, not allowed to be {@code null}.
		 * 
		 * @throws NullPointerException If {@code col} is {@code null}.
		 */
		public Info(Column_<?> col) throws NullPointerException {
			this.col = col;
			this.type = col.type();
		}
		
		@Override
		public final String name() {
			return col.name();
		}

		@Override
		public final Type type() {
			return type;
		}

		@Override
		public final String typeFactoryClassName() {
			return type instanceof SimpleType ?
								((SimpleType<?>) type).getTypeFactoryClassName() : null;
		}

		@Override
		public final String typeFactoryClasspath() {
			return type instanceof SimpleType ?
								((SimpleType<?>) type).getTypeFactoryClasspath() : null;
		}

		@Override
		public final String refdTable() {
			return col.refdTable();
		}
	}
	
	/**
	 * The constructor.
	 * To initialize the column invoke the {@link #initialize} method.
	 * 
	 * @param  type The type of the column, not allowed to be {@code null}.
	 * 
	 * @throws NullPointerException If {@code type} is {@code null}.
	 */
	public Column_(Type_ type) {
		name = null;
		this.type = Objects.requireNonNull(type);
		refdTable = null;
		table = null;
		index = 0;
	}
	
	/**
	 * Initializes the column.
	 * 
	 * @param name The name of the column, not allowed to be {@code null} and
	 *        not allowed to be an empty string.
	 * @param refdTable The referenced table of the column, not allowed to be
	 *        an empty string.
	 *        The value must be equal to {@code null} if and only if the column
	 *        does not reference a table.
	 * @param table The column's table, not allowed to be {@code null}.
	 * @param index The index of the column within the table's table definition.
	 */
	public final void initialize(String name, String refdTable, Table_ table,
																						int index) {
		this.name = name;
		this.refdTable = refdTable;
		this.table = table;
		this.index = index;
	}
	
	/**
	 * Returns the type of the column.
	 * 
	 * @return The type of the column, never {@code null}.
	 */
	public final Type_ type() {
		return type;
	}
	
	/**
	 * Returns the referenced table of the column.
	 * 
	 * @return The referenced table, never an empty string.
	 *         This value is {@code null} if and only if this column doesn't
	 *         reference a table.
	 */
	public final String refdTable() {
		return refdTable;
	}
	
	/**
	 * Returns the column's table.
	 * 
	 * @return The column's table, never {@code null}.
	 */
	public final Table_ table() {
		return table;
	}
	
	/**
	 * Returns the index of the column within the table definition.
	 * 
	 * @return The table defintion index.
	 */
	public final int index() {
		return index;
	}
	
	@Override
	public final String name() {
		return name;
	}
	
	@Override
	public final ColumnInfo info() {
		return new Info(this);
	}

	@Override
	public final ColVal<T> value(T val) {
		return new ColVal_<T>(this, val);
	}
	
	@Override
	public String toString() {
		return name;
	}
}
