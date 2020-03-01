/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import acdp.Column;
import acdp.Ref;
import acdp.Row;
import acdp.Table.TableIterator;
import acdp.exceptions.ACDPException;
import acdp.exceptions.CryptoException;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.IllegalReferenceException;
import acdp.exceptions.ShutdownException;
import acdp.internal.Database_;
import acdp.internal.Ref_;
import acdp.internal.Row_;
import acdp.internal.Table_;

/**
 * The read-only operation, created once for each table's store, provides the
 * {@code get}, {@code iterator} and {@code rows} methods, the three basic
 * read-only operations of an ACDP table.
 *
 * @author Beat Hoermann
 */
public abstract class ReadOp {
	/**
	 * The database.
	 */
	protected final Database_ db;
	/**
	 * The table this read-only operation belongs to, never {@code null}.
	 */
	protected final Table_ table;
	/**
	 * The store this read-only operation belongs to, never {@code null}.
	 */
	private final Store store;
	
	/**
	 * The constructor.
	 * 
	 * @param store The store this read-only operation belongs to, not allowed
	 *        to be {@code null}.
	 */
	protected ReadOp(Store store) {
		this.db = store.table.db();
		this.table = store.table;
		this.store = store;
	}
	
	/**
	 * Provides the {@link #load} method which reads the row data from a data
	 * source and converts it to a {@link Row_} object.
	 * In a WR database the data source are the table's FL/VL data files whereas
	 * in an RO database the data source is the database file.
	 * The database file may be cached in memory.
	 * <p>
	 * Note that a row loader has no {@code close} method.
	 * This allows to iterate the rows of a table using a for-each loop.
	 * (There is no way to tell the runtime environment to "close" the iterator
	 * that Java uses in a for-each loop.)
	 * 
	 * @author Beat Hoermann
	 */
	public interface IRowLoader {
		
		/**
		 * Reads the row data from a data source and converts it to a {@link Row_}
		 * object.
		 * Some implementations of this method rely on the {@code index} argument
		 * to be equal to the index of the <em>next</em> row.
		 * <p>
		 * Note that the {@code getRef()} method of the returned row returns
		 * {@code null}.
		 * <p>
		 * In case of table data corruption this method may throw an exception of
		 * a type not listed below.
		 * If the database is a writable database then temporary table data
		 * corruption may be due to concurrent writes.
		 * Execute this method inside a <em>read zone</em> or a <em>unit</em> to
		 * ensure that no concurrent writes are taken place in the database while
		 * this method is being executed.
		 * 
		 * @param  index The index of the row within the table, must be greater
		 *         than or equal to zero and less than the number of rows in the
		 *         table.
		 *         If the database is a WR database then an implementation of
		 *         this method may rely on the {@code index} argument to be equal
		 *         to the index of the <em>next</em> row.
		 * 
		 * @return The row or {@code null} if the "row" turns out to be a row gap.
		 *         
		 * @throws CryptoException If decryption fails.
		 *         This exception never happens if the database does not apply
		 *         encryption or if the database is an RO database.
		 * @throws ShutdownException If the file channel provider is shut down
		 *         due to a closed database.
		 *         This exception never happens if this read-only operation is
		 *         synchronized or if the database is an RO database and the
		 *         operating mode is "memory packed" or "memory unpacked".
		 * @throws IOFailureException If an I/O error occurs.
		 *         This exception never happens if the database is an RO database
		 *         and the operating mode is "memory unpacked".
		 */
		Row_ load(long index) throws CryptoException, ShutdownException,
																				IOFailureException;
	}
	
	/**
	 * Creates a row loader.
	 * 
	 * @param  cols The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of {@link #table}.
	 *         If the array of columns is empty then this method behaves as if
	 *         the array of columns is identical to the table definition.
	 * 
	 * @return The created row loader, never {@code null}.
	 * 
	 * @throws NullPointerException If the array of columns is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If the specified array of columns
	 *         contains at least one column that is not a column of {@code
	 *         table}.
	 */
	protected abstract IRowLoader createRowLoader(Column<?>[] cols) throws
											NullPointerException, IllegalArgumentException;
	
	/**
	 * Given a reference to a row, the get operation returns the referenced row.
	 * <p>
	 * This class does not implement any strategies to cope with the effects of
	 * concurrent writes in a writable database.
	 * Although it is impossible that any running method of this class can harm
	 * the integrity of the persisted data, strange results may happen from
	 * concurrent writes, including method termination by throwing an exception
	 * of a type not described in the method description.
	 * You may want to synchronize the get operation by invoking it within a
	 * <em>read zone</em> or a <em>unit</em>.
	 * This ensures that no concurrent writes are taken place in the database
	 * while the get operation is in use.
	 * See the description of the {@code acdp.internal.core.SyncManager} class to
	 * learn about "synchronized database operations".
	 *
	 * @author Beat Hoermann
	 */
	private final class Get {
		private final IRowLoader loader;
		
		/**
		 * Constructs the get operation.
		 *
		 * @param  cols The array of columns, not allowed to be {@code null}.
		 *         The columns must be columns of {@link #table}.
		 *         If the array of columns is empty then this method behaves as if
		 *         the array of columns is identical to the table definition.
		 *        
		 * @throws NullPointerException If the array of columns is equal to {@code
		 *         null}.
		 * @throws IllegalArgumentException If at least one column in the
		 *         specified array of columns is not a column of the table.
		 */
		Get(Column<?>[] cols) throws NullPointerException,
																		IllegalArgumentException {
			this.loader = createRowLoader(cols);
			// All cols are columns of the table.
		}
		
		/**
		 * Returns the referenced row.
	    * <p>
	    * In case of table data corruption this method may throw an exception of
		 * a type not listed below.
		 * If the database is a writable database then temporary table data
		 * corruption may be due to concurrent writes.
		 * Execute this method inside a <em>read zone</em> or a <em>unit</em> to
		 * ensure that no concurrent writes are taken place in the database while
		 * this method is being executed.
		 * 
		 * @param  ref The reference to the row, not allowed to be {@code null}.
		 * 
		 * @return The row, never {@code null}.
		 * 
		 * @throws NullPointerException If {@code ref} is {@code null}.
		 * @throws IllegalReferenceException If the specified reference points to
		 *         a row that does not exist within the table or if the reference
		 *         points to a row gap.
		 *         Such a situation cannot occur if {@code ref} is a {@linkplain
		 *         Ref valid} reference.
		 * @throws CryptoException If decryption fails.
		 *         This exception never happens if the database does not apply
		 *         encryption or if the database is an RO database.
		 * @throws ShutdownException If the file channel provider is shut down
		 *         due to a closed database.
		 *         This exception never happens if this get operation is
		 *         synchronized or if the database is an RO database and the
		 *         operating mode is "memory packed" or "memory unpacked".
		 * @throws IOFailureException If an I/O error occurs.
		 *         This exception never happens if the database is an RO database
		 *         and the operating mode is "memory unpacked".
		 */
		final Row_ execute(Ref_ ref) throws NullPointerException,
												IllegalReferenceException, CryptoException,
													ShutdownException, IOFailureException {
			Objects.requireNonNull(ref, ACDPException.prefix(table) +
											"Reference to row is not allowed to be null.");
			// cols != null && all cols are columns of the table && ref != null
			
			final Row_ row = loader.load(store.refToRi(ref) - 1);
			if (row == null)
				// The "row" is a row gap, can only occur if reference is invalid or
				// even corrupt.
				throw new IllegalReferenceException(table, ref, true);
			else {
				row.setRef(ref);
			}
			
			return row;
		}
	}
	
	/**
	 * Returns the referenced row.
    * <p>
    * In case of table data corruption this method may throw an exception of a
    * type not listed below.
    * If the database is a writable database then temporary table data
    * corruption may be due to concurrent writes.
    * Execute this method inside a <em>read zone</em> or a <em>unit</em> to
    * ensure that no concurrent writes are taken place in the database while
    * this method is being executed.
	 * 
	 * @param  ref The reference to the row, not allowed to be {@code null}.
	 * @param  cols The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of {@link #table}.
	 *         If the array of columns is empty then this method behaves as if
	 *         the array of columns is identical to the table definition.
	 *         
	 * @return The row, never {@code null}.
	 *
	 * @throws NullPointerException If one of the arguments is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If at least one column in the
	 *         specified array of columns is not a column of {@code table}.
	 * @throws IllegalReferenceException If the specified reference points to
	 *         a row that does not exist within the table or if the reference
	 *         points to a row gap.
	 *         Such a situation cannot occur if {@code ref} is a {@linkplain Ref
	 *         valid} reference.
	 * @throws CryptoException If decryption fails.
	 *         This exception never happens if the database does not apply
	 *         encryption or if the database is an RO database.
	 * @throws ShutdownException If the file channel provider is shut down due
	 *         to a closed database.
	 *         This exception never happens if this get operation is
	 *         synchronized or if the database is an RO database and the
	 *         operating mode is "memory packed" or "memory unpacked".
	 * @throws IOFailureException If an I/O error occurs.
	 *         This exception never happens if the database is an RO database
	 *         and the operating mode is "memory unpacked".
	 */
	final Row_ get(Ref_ ref, Column<?>[] cols) throws NullPointerException,
								IllegalArgumentException, IllegalReferenceException,
								CryptoException, ShutdownException, IOFailureException {
		return new Get(cols).execute(ref);
	}
	
	/**
	 * Provides the {@link #next} method which loads the next row.
	 * Loading a row means reading the row data from a data source and converting
	 * it to a {@link Row_} object.
	 * In a WR database the data source are the table's FL/VL data files whereas
	 * in an RO database the data source is the database file.
	 * The database file may be cached in memory.
	 * <p>
	 * This class does not implement any strategies to cope with the effects of
	 * concurrent writes in a writable database.
	 * Although it is impossible that any running method of this class can harm
	 * the integrity of the persisted data, strange results may happen from
	 * concurrent writes, including method termination by throwing an exception
	 * of a type not described in the method descriptions.
	 * You may want to synchronize the row advancer by calling its methods,
	 * including the constructor, within a <em>read zone</em> or a <em>unit</em>.
	 * <p>
	 * Note that a row advancer has no {@code close} method.
	 * This allows to iterate the rows of a table using a for-each loop.
	 * (There is no way to tell the runtime environment to "close" the iterator
	 * that Java uses in a for-each loop.)
	 * 
	 * @author Beat Hoermann
	 */
	protected static final class RowAdvancer {
		/**
		 * The row loader, never {@code null}.
		 */
		private final IRowLoader rowLoader;
		/**
		 * The current index.
		 */
		private long index;
		/**
		 * The end index.
		 */
		private final long end;
		
		/**
		 * The constructor.
		 * <p>
		 * Let's assume that {@code start} is less than {@code end} and that
		 * the given table has at least one row with an index greater than or
		 * equal to {@code start} and less than {@code end}.
		 * Under this assumption, the first row returned by the row advancer is
		 * the row having an index equal to {@code start}, unless the database is
		 * a WR database and the FL data block with that index marks a row gap in
		 * which case the first row returned by the row advancer is the first row
		 * with an index greater than {@code start}.
		 * Similarly, under the same assumption, the last row returned by the
		 * row advancer is the row having an index equal to {@code end - 1},
		 * unless the database is a WR database and the FL data block with that
		 * index marks a row gap.
		 * In such a case the last row returned by the row advancer is the last
		 * row with an index less than {@code end - 1}.
		 * <p>
		 * If {@code start} is greater than or equal to {@code end} or the given
		 * table has no row with an index greater than or equal to {@code start}
		 * and less than {@code end} then the {@code next} method returns {@code
		 * null}.
		 * 
		 * @param rowLoader The row loader, not allowed to be {@code null}.
		 * @param start The first index the row advancer tries to return the
		 *        row for, must be greater than or equal to zero.
		 *        For further details see the method description.
		 * @param end The second-last index the row advancer tries to return the
		 *        row for, must be less than or equal to the sum of the number of
		 *        rows and the number of row gaps.
		 *        For further details see the method description.
		 */
		public RowAdvancer(IRowLoader rowLoader, long start, long end) {
			this.rowLoader = rowLoader;
			this.index = start;
			this.end = end;
		}
		
		/**
		 * Returns the next row.
		 * The method returns {@code null} if there is no next row.
		 * 
		 * @return The next row or {@code null} if there is no next row.
		 * 
		 * @throws CryptoException If decryption fails.
		 *         This exception never happens if the database does not apply
		 *         encryption or if the database is an RO database.
		 * @throws ShutdownException If the file channel provider is shut down
		 *         due to a closed database.
		 *         This exception never happens if this read-only operation is
		 *         synchronized or if the database is an RO database.
		 * @throws IOFailureException	If an I/O error occurs.
		 *         This exception never happens if the database is an RO database
		 *         and the operating mode is "memory unpacked".
		 */
		final Row_ next() throws CryptoException, ShutdownException,
																				IOFailureException {
			Row_ row = null;
			while (index < end && row == null) {
				row = rowLoader.load(index++);
				// row == null <=> row gap
			}
			// index == end || row != null
			// row == null => no more rows in the table.
			if (row != null) {
				// Note that the reference to the first row of the table has an
				// index equal to 1!
				row.setRef(new Ref_(index));
			}
			return row;
		}
	}
	
	/**
	 * Creates a row advancer.
	 * 
	 * @param  start The first index the row advancer tries to return the row
	 *         for, must be greater than or equal to zero.
	 *         For further details read the description of the constructor of
	 *         the {@link RowAdvancer} class.
	 * @param  end The second-last index the row advancer tries to return the
	 *         row for, must be less than or equal to the sum of the number of
	 *         rows and the number of row gaps.
	 *         For further details read the description of the constructor of
	 *         the {@link RowAdvancer} class.
	 * @param  cols The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of {@link #table}.
	 *         If the array of columns is empty then the row advancer behaves as
	 *         if the value is identical to the table definition.
	 * 
	 * @return The created advancer, never {@code null}.
	 * 
	 * @throws NullPointerException If the array of columns is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If the specified array of columns
	 *         contains at least one column that is not a column of {@code
	 *         table}.
	 */
	protected abstract RowAdvancer createRowAdvancer(long start, long end,
										Column<?>[] cols) throws IllegalArgumentException;
	
	/**
	 * An iterator over the sequence of rows of a given table.
	 * <p>
	 * This class does not implement any strategies to cope with the effects of
	 * concurrent writes in a writable database.
	 * Although it is impossible that any running method of this class can harm
	 * the integrity of the persisted data, strange results may happen from
	 * concurrent writes, including method termination by throwing an exception
	 * of a type not described in the method descriptions.
	 * You may want to synchronize the iterator by calling its methods, including
	 * the constructor, within a <em>read zone</em> or a <em>unit</em>.
	 * This ensures that no concurrent writes are taken place in the database
	 * while the iterator is in use.
	 * See the description of the {@code acdp.internal.core.SyncManager} class to learn
	 * about "synchronized database operations".
	 *
	 * @author Beat Hoermann
	 */
	private final class TableIterator_ implements TableIterator {
		/**
		 * The row advancer, never {@code null}.
		 */
		private final RowAdvancer advancer;
		/**
		 * Indicates if the next row is already loaded due to a previous call
		 * to the {@code hasNext()} method.
		 */
		private boolean loaded;
		/**
		 * The next row.
		 * This is the row that will be returned by an incovation of the {@code
		 * next()}  method.
		 */
		private Row_ row;
		
		/**
		 * Constructs the table iterator.
		 * 
		 * @param  start The first index the iterator tries to return the row for,
		 *         must be greater than or equal to zero.
		 *         Note that if the database is a WR database it may be the case
		 *         that the FL data block with that index marks a row gap in which
		 *         case the first row returned by the iterator is the first row
		 *         with an index greater than {@code start}, provided that such a
		 *         row exists at all.
		 * @param  cols The array of columns, not allowed to be {@code null}.
		 *         The columns must be columns of {@link #table}.
		 *         If the array of columns is empty then the iterator behaves as
		 *         if the array of columns is identical to the table definition.
		 *         
		 * @throws NullPointerException If the array of columns is equal to {@code
		 *         null}.
		 * @throws IllegalArgumentException If {@code start} is less than zero or
		 *         if at least one column in the specified array of columns is
		 *         not a column of {@code table}.
		 */
		TableIterator_(long start, Column<?>[] cols) throws NullPointerException,
																		IllegalArgumentException {
			if (start < 0) {
				throw new IllegalArgumentException(ACDPException.prefix(table) +
													"Starting index is negative: " + start);
			}
			// index >= 0.
			this.advancer = createRowAdvancer(start, store.numberOfRows() +
																store.numberOfRowGaps(), cols);
			// All cols are columns of the table.
			loaded = false;
			row = null;
		}
		
		@Override
		public final boolean hasNext() throws  CryptoException,
													ShutdownException, IOFailureException {
			if (!loaded) {
				row = advancer.next();
				loaded = true;
			}
			return row != null;
			// loaded
		}
		
		@Override
		public final Row next() throws NoSuchElementException, CryptoException,
													ShutdownException, IOFailureException {
			if (!loaded)
				row = advancer.next();
			else {
				loaded = false;
			}
			if (row == null) {
				throw new NoSuchElementException(ACDPException.compose(db.name(),
																			table.name(), null));
			}
			return row;
			// !loaded
		}

		@Override
		public final void remove() throws UnsupportedOperationException {
			throw new UnsupportedOperationException(ACDPException.compose(
																db.name(), table.name(), null));
		}
	}
	/**
	 * Returns an iterator over the rows of the table.
	 * <p>
    * In case of table data corruption this method may throw an exception of a
    * type not listed below.
    * If the database is a writable database then temporary table data
    * corruption may be due to concurrent writes.
    * Execute this method or even the whole iteration inside a <em>read
    * zone</em> or a <em>unit</em> to ensure that no concurrent writes are
    * taken place in the database while the iteration is being executed.
	 * 
	 * @param  start The first index the iterator tries to return the row for,
	 *         must be greater than or equal to zero.
	 *         Note that if the database is a WR database it may be the case
	 *         that the FL data block with that index marks a row gap in which
	 *         case the first row returned by the iterator is the first row with
	 *         an index greater than {@code start}, provided that such a row
	 *         exists at all.
	 * @param  cols The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of {@link #table}.
	 *         If the array of columns is empty then the iterator behaves as if
	 *         the array of columns is identical to the table definition.
	 *         
	 * @return The iterator, never {@code null}.
	 * 
	 * @throws NullPointerException If the array of columns is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If {@code start} is less than zero or if
	 *         at least one column in the specified array of columns is not a
	 *         column of {@code table}.
	 */
	final TableIterator iterator(long start, Column<?>[] cols) throws
										NullPointerException, IllegalArgumentException {
		return new TableIterator_(start, cols);
	}
	
	/**
	 * An implementation of {@link Spliterator} returning the rows of a given
	 * table.
	 * <p>
	 * The table spliterator returns the {@link #IMMUTABLE} characteristic value.
	 * Indeed, the tables of an RO database or of a write protected WR database
	 * are immutable, but not the tables of a writable database.
	 * For a writable database that is operated in an environment that allows
	 * concurrent writes, the table spliterator must be executed inside a
	 * <em>read zone</em> or a <em>unit</em>.
	 * In this regard, it is worth noting that the table spliterator is
	 * <em>late-binding</em>.
	 * <p>
	 * The table spliterator is able to split efficiently and in a perfect
	 * balanced manner in the absence of row gaps.
	 * If there are row gaps, the top-level table spliterator still returns a
	 * {@link #SIZED} characteristic value but not a {@link #SUBSIZED} one.
	 * Consequently, any child table spliterator can neither be {@code SIZED} nor
	 * {@code #SUBSIZED} in the presence of row gaps.
	 * 
	 * @author Beat Hoermann
	 */
	private final class TableSpliterator implements Spliterator<Row> {
		/**
		 * The parent table spliterator or {@code null} if this table spliterator
		 * is a top-level table spliterator.
		 */
		private final TableSpliterator parent;
		/**
		 * The first index the table spliterator tries to return the row for.
		 */
		private final long start;
		/**
		 * The second-last index the row advancer tries to return the row for.
		 * <p>
		 * Due to one or more splits the initial value of {@code end} may be
		 * decreased.
		 */
		private long end;
		/**
		 * The array of columns.
		 * For details see the description of the {@code cols} argument of the
		 * constructor.
		 */
		private final Column<?>[] cols;
		/**
		 * The row advancer, initially set to {@code null}, created on demand.
		 */
		private RowAdvancer rowAdvancer;
		
		/**
		 * The constructor for creating a child table spliterator.
		 * 
		 * @param  parent The parent table spliterator, not allowed to be {@code
		 *         null}.
		 * @param  start The first index the table spliterator tries to return the
		 *         row for, must be greater than or equal to zero.
		 *         For further details read the description of the constructor of
		 *         the {@link RowAdvancer} class.
		 * @param  end The second-last index the row advancer tries to return the
		 *         row for, must be less than or equal to the sum of the number of
		 *         rows and the number of row gaps.
		 *         For further details read the description of the constructor of
		 *         the {@link RowAdvancer} class.
		 * @param  cols The array of columns, not allowed to be {@code null}.
		 *         The columns must be columns of {@link #table}.
		 *         If the array of columns is empty then the row advancer behaves
		 *         as if the value is identical to the table definition.
		 *         
		 * @throws NullPointerException If the array of columns is equal to {@code
		 *         null}.
		 * @throws IllegalArgumentException If the specified array of columns
		 *         contains at least one column that is not a column of {@code
		 *         table}.
		 */
		private TableSpliterator(TableSpliterator parent,
										long start, long end, Column<?>[] cols)  throws
										NullPointerException, IllegalArgumentException {
			this.parent = parent;
			this.start = start;
			this.end = end;
			this.cols = cols;
			rowAdvancer = null;
		}
		
		/**
		 * The constructor for creating a top-level table spliterator.
		 * 
		 * @param  cols The array of columns, not allowed to be {@code null}.
		 *         The columns must be columns of {@link #table}.
		 *         If the array of columns is empty then the row advancer behaves
		 *         as if the value is identical to the table definition.
		 *         
		 * @throws NullPointerException If the array of columns is equal to {@code
		 *         null}.
		 * @throws IllegalArgumentException If the specified array of columns
		 *         contains at least one column that is not a column of {@code
		 *         table}.
		 */
		TableSpliterator(Column<?>[] cols)  throws NullPointerException,
																		IllegalArgumentException {
			this(null, 0, store.numberOfRows() + store.numberOfRowGaps(), cols);
		}
		
		@Override
		public final void forEachRemaining(Consumer<? super Row> action) {
			if (rowAdvancer == null) {
				rowAdvancer = createRowAdvancer(start, end, cols);
			}
			Row row = rowAdvancer.next();
			while (row != null) {
				action.accept(row);
				row = rowAdvancer.next();
			}
		}

		@Override
		public final boolean tryAdvance(Consumer<? super Row> action) {
			if (rowAdvancer == null) {
				rowAdvancer = createRowAdvancer(start, end, cols);
			}
			final Row row = rowAdvancer.next();
			if (row != null) {
				action.accept(row);
				return true;
			}
			else {
				return false;
			}
		}

		@Override
		public final Spliterator<Row> trySplit() {
			if (rowAdvancer == null) {
				final long mid = (start + end) / 2;
				if (start == mid)
					return null;
				else {
					// start < mid
					final long oldEnd = end;
					end = mid;
					return new TableSpliterator(this, mid, oldEnd, cols);
				}
			}
			else {
				// Do not split if this table spliterator has already returned a
				// row.
				return null;
			}
		}

		@Override
		public final long estimateSize() {
			return parent == null ? store.numberOfRows() : end - start;
		}

		@Override
		public final int characteristics() {
			int c = ORDERED | NONNULL | IMMUTABLE;
			if (parent == null) {
				// top-level table spliterator
				c |= SIZED;
				if (store.numberOfRowGaps() == 0) {
					c |= SUBSIZED;
				}
			}
			else {
				// child table spliterator
				if (parent.hasCharacteristics(SUBSIZED)) {
					c |= SIZED | SUBSIZED;
				}
			}
			return c;
		}
	}
	
	/**
	 * Returns a {@link Stream} of the table's rows.
	 * <p>
	 * The returned stream is a sequential, ordered stream which may be turned
	 * into a parallel, unordered stream by invoking the stream's {@link
	 * Stream#parallel() parallel()} and/or {@link Stream#unordered()
	 * unordered()} methods, as explained in the description of the {@code
	 * Stream} class.
	 * <p>
	 * For a writable database that is operated in an environment that allows
	 * concurrent writes, at least the <em>terminal operation</em> of the
	 * <em>stream pipeline</em> must be executed inside a <em>read zone</em> or
	 * a <em>unit</em>.
	 * (The stream's underlying {@link Spliterator} is <em>late-binding</em>.)
	 * 
	 * @param  cols The array of columns, not allowed to be {@code null}.
	 *         The columns must be columns of {@link #table}.
	 *         If the array of columns is empty then the stream behaves as if
	 *         the array of columns is identical to the table definition.
	 *         
	 * @return The stream of the table's rows, never {@code null}.
	 *         All elements of the stream are non-null.
	 * 
	 * @throws NullPointerException If the array of columns is equal to {@code
	 *         null}.
	 * @throws IllegalArgumentException If the specified array of columns
	 *         contains at least one column that is not a column of {@code
	 *         table}.
	 */
	final Stream<Row> rows(Column<?>[] cols) throws NullPointerException,
																		IllegalArgumentException {
		return StreamSupport.stream(new TableSpliterator(cols), false);
	}
}