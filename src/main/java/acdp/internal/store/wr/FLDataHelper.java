/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.store.wr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import acdp.Column;
import acdp.exceptions.ACDPException;
import acdp.internal.Column_;
import acdp.internal.store.wr.WRStore.WRColInfo;

/**
 * This class introduces <em>sections</em> and <em>blocks</em> and provides a
 * special <em>heuristic</em> which makes it easier to decide how best to
 * access the FL column data of an FL data block for a given set of columns.
 * <p>
 * A <em>section</em> is either associated with a column or with the bitmap
 * of a row.
 * A section keeps an offset, hence, the index where the FL column data for a
 * particular column or the bitmap starts within the byte array containing the
 * FL data block.
 * In addition, a section keeps the length of that FL column data or the length
 * of the bitmap.
 * <p>
 * On the other hand, a <em>block</em> is a list of sections such that any
 * two sections {@code s}<sub>{@code i}</sub> and {@code s}<sub>{@code
 * i+1}</sub> in the list of sections are such that the offset of
 * {@code s}<sub>{@code i+1}</sub> is equal to the offset plus the length of
 * {@code s}<sub>{@code i}</sub>.
 * The sections in a block are said to be <em>connected</em>.
 * (Likewise, the columns associated with the sections of a block as well as
 * the FL column data of these columns are said to be connected.)
 * A block makes it possible to access the FL column data of several connected
 * columns (and possibly the bitmap of an unreferenced table) with a single
 * read or write operation.
 * <p>
 * The forementioned heuristic takes as input a list of blocks created on the
 * basis of a given set of columns.
 * Let {@code p} &ge; 1 be the number of blocks and let {@code m} be the sum of
 * the lengths of the FL column data that correspond to the given columns and
 * let {@code n} be the length of the FL data block.
 * (Of course, {@code m} &le; {@code n}.)
 * If
 * 
 * <pre>
 * m &lt; n / p</pre>
 * 
 * then the heuristic suggests that the blocks are best accessed with {@code p}
 * individual read or write operations.
 * Otherwise, the heuristic suggests that the blocks are accessed by accessing
 * the whole FL data block of length {@code n} in a single read or write
 * operation.
 * (To simplify matters, we just ignored the fact that it may be necessary
 * to consider the bitmap of the row.
 * Doing so does not change the way how the heuristic works.)
 * <p>
 * For instance, assume that the FL data block has a length of 1000 bytes,
 * hence {@code n} = 1000.
 * Furthermore assume that we just want to focus on the FL column data of two
 * connected columns forming just a single block ({@code p} = 1) and having a
 * total length of 600 bytes ({@code m} = 600).
 * In this case the heuristic suggests accessing the one and only one block
 * from the table's FL data file.
 * However, assume the same situation but the two columns not connected
 * ({@code p} = 2).
 * Then it is easy to see that the heuristic, instead of accessing both blocks
 * individually, suggests that the whole FL data block being accessed in a
 * single read or write operation, thus accessing 400 bytes for nothing but
 * hopefully being faster due to reduced latency.
 * 
 * @author Beat Hoermann
 */
final class FLDataHelper {
	/**
	 * Represents a <em>section</em> on the FL data block.
	 * A section has an <em>offset</em> and a <em>length</em> and it houses a
	 * {@code WRColInfo} object which may be {@code null}.
	 * <p>
	 * The offset of a section is the position within the FL data block where the
	 * section starts and the length is the length of the section.
	 * <p>
	 * If the {@code WRColInfo} object {@code ci} of a section is not
	 * {@code null} then the offset and the length of the section are equal to
	 * the values of {@code ci.offset} and {@code ci.len}, respectively.
	 * Otherwise, the offset and the length of the section are equal to 0 and
	 * {@code store.nBM}, respectively, and the section represents the bitmap of
	 * the row.
	 * 
	 * @author Beat Hoermann
	 */
	static final class Section implements Comparable<Section> {
		/**
		 * The {@code WRColInfo} object associated with this section.
		 * If this value is {@code null} then the section represents the bitmap
		 * of the row.
		 */
		final WRColInfo ci;
		/**
		 * The offset of the section, hence the position within the FL data block
		 * where this section starts.
		 */
		final int offset;
		/**
		 * The length of this section, greater than or equal to zero.
		 */
		final int len;
		
		/**
		 * Constructs a section for the specified {@code WRColInfo} object.
		 * 
		 * @param ci The {@code WRColInfo} object, not allowed be {@code null}.
		 */
		Section(WRColInfo ci) {
			this.ci = ci;
			this.offset = ci.offset;
			this.len = ci.len;
		}
		
		/**
		 * Constructs a section representing the bitmap of the row.
		 * 
		 * @param store The store of the table, not allowed to be {@code null}.
		 */
		Section(WRStore store) {
			this.ci = null;
			this.offset = 0;
			this.len = store.nBM;
		}

		@Override
		public final int compareTo(Section s) {
			return Integer.compare(offset, s.offset);
		}
	}
	
	/**
	 * A <em>block</em> is a list of <em>connected</em> {@linkplain Section
	 * sections}.
	 * <p>
	 * In complete analogy to a section a block has an <em>offset</em> and
	 * a <em>length</em>.
	 * <p>
	 * Note that the iterator returns the sections of this block representing a
	 * column in the order defined by the table definition <em>and not</em> in
	 * the order defined by the array of columns passed via the constructor of
	 * the {@code FLDataHelper} class.
	 *  
	 * @author Beat Hoermann
	 */
	static final class Block implements Iterable<Section> {
		/**
		 * The list of connected sections.
		 * For any offset {@code i} with 0 &le; {@code i} &lt; {@code l.size()-1}
		 * the following holds:
		 * {@code l.get(i+1).offset == l.get(i).offset + l.get(i).len}.
		 */
		private final List<Section> l;
		
		/**
		 * The constructor.
		 * Creates an empty block.
		 */
		private Block() {
			l = new ArrayList<>();
		}
		
		/**
		 * Adds a new section to this block.
		 * If this method was invoked before then the specified section
		 * {@code s} must be connected with the last added section {@code
		 * s0} or to be precise: {@code s.offset == s0.offset + s0.len}.
		 * 
		 * @param s The section to be added to this block.
		 */
		private final void add(Section s) {
			l.add(s);
		}
		
		/**
		 * The {@code WRColInfo} object of the first section of this block.
		 * 
		 * @return The {@code WRColInfo} object of the first section of this
		 *         block.
		 *         This value is {@code null} if this is the first block and its
		 *         first section represents the bitmap of the row.
		 */
		final WRColInfo ci() {
			return l.get(0).ci;
		}
		
		/**
		 * Returns the offset of this block.
		 * It is assumed that this block has at least one section.
		 * 
		 * @return The offset of this block.
		 */
		final int offset() {
			return l.get(0).offset;
		}
		
		/**
		 * Returns the length of this block which is equal to the sum of the
		 * lengths of all its sections.
		 * It is assumed that this block has at least one section.
		 * 
		 * @return The length of this block, greater than or equal to zero.
		 */
		final int length() {
			final Section last = l.get(l.size() - 1);
			return last.offset + last.len - offset();
		}

		/**
		 * Returns an iterator.
		 * <p>
		 * Note that the iterator returns the sections of this block representing
		 * a column in the order defined by the table definition <em>and not</em>
		 * in the order defined by the array of columns passed via the
		 * constructor of the {@code FLDataHelper} class.
		 * 
		 * @return The iterator.
		 */
		@Override
		public final Iterator<Section> iterator() {
			return l.iterator();
		}
	}
	
	/**
	 * Encapsulates a list of {@linkplain Block blocks}.
	 * 
	 * @author Beat Hoermann
	 */
	static final class Blocks implements Iterable<Block> {
		/**
		 * The list of blocks.
		 */
		private final List<Block> l;
		
		/**
		 * The constructor.
		 * 
		 * @param sections The sections used to construct this list of blocks,
		 *        not allowed to be {@code null} and not allowed to be empty.
		 */
		Blocks(List<Section> sections) {
			l = new ArrayList<>();
			
			// Sort sections in ascending order with respect to the offset of
			// the sections.
			Collections.sort(sections);
			
			// Create the list of blocks.
			int offset = -1;
			Block block = null;
			for (Section s : sections) {
				if (offset != s.offset) {
					// The previous section and this section are not connected.
					block = new Block();
					l.add(block);
				}
				block.add(s);
				offset = s.offset + s.len;
			}
		}
		
		/**
		 * Returns the size of this list of blocks.
		 * The value returned by this method is equal to the number of connected
		 * sections passed via the constructor.
		 * 
		 * @return The size of this list of blocks, always greater than zero.
		 */
		final int size() {
			return l.size();
		}

		@Override
		public final Iterator<Block> iterator() {
			return l.iterator();
		}
	}
	
	/**
	 * The columns passed via the constructor.
	 * The columns may or may not be sorted according to the order defined by
	 * the table definition.
	 * It is guaranteed that all columns are actually columns of {@code
	 * store.table}, where {@code store} denotes the store passed via the
	 * constructor.
	 */
	final Column<?>[] cols;
	
	/**
	 * The list of sections created on the basis of the columns passed via the
	 * constructor.
	 * The first section represents the bitmap of the row.
	 * <p>
	 * Note that the sections representing columns are sorted according to the
	 * order defined by the table definition, hence, they do not appear in the
	 * original order of the column array passed via the constructor.
	 */
	final List<Section> sections;
	
	/**
	 * The blocks made of the {@link #sections}.
	 */
	final Blocks blocks;
	
	/**
	 * The result of the heuristic as explained in the class description.
	 */
	final boolean doProcessBlocks;
	
	/**
	 * The sum of the length of all sections.
	 */
	final int length;
	
	/**
	 * Computes the heuristic mentioned in the class description.
	 * Note that the following condition is satisfied:
	 * {@code len <= n && blocks.size() >= 1}.
	 * 
	 * @param  len The sum of the length of the FL column data of all columns
	 *         (and possibly the bitmap of the row) that need to be accessed.
	 * @param  n The length of the FL data block.
	 * @param  blocks The blocks, not allowed to be {@code null} and not allowed
	 *         to be empty.
	 *         
	 * @return The boolean value {@code true} if the criterion described in the
	 *         class description is satisfied, {@code false} otherwise.
	 */
	private final boolean heuristic(int len, int n, Blocks blocks) {
		return len < n / blocks.size();
	}
	
	/**
	 * The constructor.
	 * 
	 * @param  cols The columns, not allowed to be {@code null} and not allowed
	 *         to be empty.
	 *         The columns may or may not be sorted according to the order
	 *         defined by the table definition.
	 *         The columns must be columns of {@code store.table}.
	 * @param  store The store of the table.
	 * 
	 * @throws IllegalArgumentException If the specified array of columns
	 *         contains at least one column that is not a column of the store's
	 *         table.
	 */
	FLDataHelper(Column<?>[] cols, WRStore store) throws
																		IllegalArgumentException {
		this.cols = cols;
		// Compute the sum of the lengths of each specified column, including the
		// bitmap of the row and collect the sections.
		sections = new ArrayList<>(cols.length + 1);
		// Add section representing the bitmap of the row.
		sections.add(new Section(store));
		final Map<Column_<?>, WRColInfo> colInfoMap = store.colInfoMap;
		int len = store.nBM;
		for (Column<?> col : cols) {
			final WRColInfo ci = colInfoMap.get(col);
			if (ci == null) {
				throw new IllegalArgumentException(ACDPException.prefix(
														store.table) + "Column \"" + col +
														"\" is not a column of this table.");
			}
			sections.add(new Section(ci));
			len += ci.len;
		}
		
		// Create blocks. As a side effect the sections are sorted.
		blocks = new Blocks(sections);
		
		// Compute heuristic.
		doProcessBlocks = heuristic(len, store.n, blocks);
		
		// Set length.
		length = len;
	}
}