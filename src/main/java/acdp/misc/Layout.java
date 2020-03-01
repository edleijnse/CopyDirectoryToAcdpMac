/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.misc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;

import acdp.exceptions.DanglingCommentException;
import acdp.exceptions.IndentationException;
import acdp.exceptions.MissingEntryException;

/**
 * Defines a layout, that is, a collection of well-structured values.
 * A layout can be easier read by a human being than an XML or JSON formatted
 * text but it is not that powerful, though more powerful than just a flat set
 * of properties, e.g., name = john, age = 38.
 * <p>
 * The syntax of a layout in EBNF (Extended Backus–Naur Form) is recursively
 * defined as follows:
 * 
 * <pre>
 * layout      = [ entryList, [EOL] ]
 * entryList   = indEntry, { EOL, indEntry }
 * indEntry    = [ comment ], indent, entry
 * indent      = <i>level</i> * INDENT
 * entry       = KEY, WS, "=", WS, VALUE |
 *               KEY, "[]", seqVal |
 *               KEY, layoutVal
 * layoutVal   = [ EOL, entryList ]
 * seqVal      = { EOL, [ comment ], indent, element }
 * element     = TEXT |
 *               "[]", seqVal |
 *               ".", EOL, layoutVal
 * comment     = cLine, { cLine }
 * cLine       = indent, "#", CTEXT, EOL
 * 
 * EOL         = ? any <em>predefined</em> non-empty sequence of characters
 *                 identifying the end of a line. ?
 * INDENT      = ? any <em>predefined</em> non-empty sequence of characters with
 *                 codes less than or equal to SPACE U+0020 and not
 *                 containing EOL. ?
 * WS          = ? any empty or non-empty sequence of characters with
 *                 codes less than or equal to SPACE U+0020 and not
 *                 containing EOL. ?
 * KEY         = ? any non-empty sequence of characters that starts
 *                 and ends with a character having a code greater than
 *                 SPACE U+0020 and that starts with a character different from
 *                 the NUMBER SIGN ('{@code #}') U+0023 and that neither
 *                 contains the EQUALS SIGN ('{@code =}') U+003D nor EOL. ?
 * VALUE       = ? any empty or non-empty sequence of characters that
 *                 starts and ends with a character having a code greater
 *                 than SPACE U+0020 and that does not contain EOL. ?
 * TEXT        = ? any non-empty sequence of characters that starts
 *                 and ends with a character having a code greater than
 *                 SPACE U+0020 and that starts with a character different from
 *                 the NUMBER SIGN ('{@code #}') U+0023 and that does not
 *                 contain EOL. ?
 * CTEXT       = ? any empty or non-empty sequence of characters that does not
 *                 contain EOL. ?</pre>
 * 
 * The terminal symbol {@code EOL} denotes a {@linkplain Files#readAllLines
 * line terminator} and the symbol <i>{@code level}</i> denotes an integer
 * variable called the <em>level of indentation</em>.
 * Initially this variable is set to 0 and it is incremented by one each time a
 * {@code layoutVal} or {@code seqVal} non-terminal symbol is started to be
 * applied and it is decremented by one each time that {@code layoutVal} or that
 * {@code seqVal} non-terminal symbol is finished to be applied.
 * <p>
 * Thus, a layout can roughly be seen as a collection of key-value pairs where
 * the key is some text and the value is either a string, a sequence or again a
 * layout.
 * A key-value pair with a value being a string is also known as a
 * <em>property</em>, e.g., "{@code age = 38}".
 * An element of a sequence can be a string, again a sequence or a layout.
 * <p>
 * Applying three space characters for indentation, a database with a name
 * equal to "TriasDB" containing a single table "Configuration" may be laid out
 * like this:
 * 
 * <pre>
 * name = TriasDB
 * version = 1.5
 * consistencyNumber = 0
 * # cipherFactoryClassName = com.example.CipherFactory
 * # cipherChallenge = 29jxv5pqc56i69ys
 * forceWriteCommit = off
 * recFile = rec
 * tables
 *    Configuration
 *       columns[]
 *          .
 *             name = ID
 *             typeDesc = s!i58v
 *          .
 *             name = Locales
 *             typeDesc = s!i40v
 *          .
 *             name = Timestamp
 *             typeDesc = l!i8
 *       store
 *          nobsRowRef = 1
 *          flDataFile = Configuration_fld</pre>
 *       
 * Due to its recursive definition the entries under {@code tables},
 * {@code Configuration}, {@code store}, and the dots ({@code .}) are themselves
 * layouts.
 * Note that empty layouts, empty sequences and even empty string property
 * values are allowed.
 * <p>
 * Indentation is the only way to give a layout a more complex structure than
 * just a flat set of key-value pairs.
 * Changing the indentations manually within a file containing a layout must be
 * done with care.
 * It is an error if a line of text starts with a sequence of white spaces that
 * can't be matched to an indentation.
 * (A white space is a character with a code less than or equal to SPACE
 * U+0020.)
 * It is also an error if any line of text forming an entry in a layout or
 * forming an element in a sequence has an indentation level that is too large,
 * like the last line of text in the following example:
 * 
 * <pre>
 * store
 *    nobsRowRef = 1
 *       flDataFile = Configuration_fld</pre>
 * <p>
 * Note, however, that
 * 
 * <pre>
 * store
 *    nobsRowRef = 1
 * flDataFile = Configuration_fld</pre>
 * 
 * is perfectly correct, though, this might not be what the user wanted
 * because the last entry is an entry of the "root" layout and not an entry of
 * the nested {@code store} layout.
 * <p>
 * Note that comments must be properly indented.
 * Therefore
 * 
 * <pre>
 * #
 * # This is a store
 * #
 * store
 *    nobsRowRef = 1
 *    # The backing file of the store
 *    flDataFile = Configuration_fld</pre>
 * <p>
 * is perfectly correct, whereas
 * 
 * <pre>
 * store
 *    nobsRowRef = 1
 *   # The backing file of the store
 *    flDataFile = Configuration_fld</pre>
 * <p>
 * and 
 * <pre>
 * store
 *    nobsRowRef = 1
 * # Now comes the table class name
 *    flDataFile = Configuration_fld</pre>
 * 
 * raise an exception because the comment is not properly indented.
 * <p>
 * This class provides the following operations:
 * 
 * <ul>
 * 	<li>Two factory methods for creating a layout by reading it from a UTF-8
 *        encoded text file and input stream, respectively.</li>
 *    <li>A constructor for creating an empty layout.</li>
 *    <li>A copy constructor for creating a memory independent copy of an
 *        already existing layout.</li>
 * 	<li>Methods for creating, returning and manipulating the entries of a
 *        layout.</li>
 * 	<li>A method for saving the changes made to a layout.</li>
 * 	<li>Two methods for writing a layout to a UTF-8 encoded text file and
 *        output stream, respectively.</li>
 * </ul>
 *
 * @author Beat Hoermann
 */
public final class Layout {
	private static final Charset utf8 = Charset.forName("UTF-8");
	
	/**
	 * An item either wraps the value of an entry in a layout or an element in
	 * a sequence.
	 * An item may have a comment.
	 * 
	 * @author Beat Hoermann
	 */
	private static class Item {
		/**
		 * The comment, never an empty list but may be {@code null}.
		 */
		final List<String> comment;
		/**
		 * The actual value, never {@code null}.
		 * The type of the value is one of {@code String}, {@code Seq} or {@code
		 * Layout}.
		 */
		final Object value;
		
		/**
		 * Creates an item with a comment equal to {@code null}.
		 * 
		 * @param  value The actual value, not allowed to be {@code null}.
		 *         The type of the value must be one of {@code String},
		 *         {@code Seq} or {@code Layout}.
		 *         
		 * @throws NullPointerException If the specified value is {@code null}.
		 */
		Item(Object value) throws NullPointerException {
			this(null, value);
		}
		
		/**
		 * The constructor.
		 * 
		 * @param  comment The comment, never an empty list but may be {@code
		 *         null}.
		 * @param  value The actual value, not allowed to be {@code null}.
		 *         The type of the value must be one of {@code String},
		 *         {@code Seq} or {@code Layout}.
		 *         
		 * @throws NullPointerException If the specified value is {@code null}.
		 */
		Item(List<String> comment, Object value) throws NullPointerException {
			this.comment = comment;
			this.value = Objects.requireNonNull(value);
		}
	}
	
	/**
	 * The value class used by the internal backing {@link Layout#map map}.
	 * Wraps a {@linkplain #value} of an entry of a layout.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class Value extends Item {
		/**
		 * The ordinal number of the value.
		 */
		final int ordinal;
		
		/**
		 * Creates a value with a comment equal to {@code null}.
		 * 
		 * @param  value The value, not allowed to be {@code null}.
		 *         The type of the value must be one of {@code String},
		 *         {@code Seq} or {@code Layout}.
		 * @param  ordinal The ordinal number which may be used for sorting the
		 *         values.
		 *         
		 * @throws NullPointerException If the specified value is {@code null}.
		 */
		Value(Object value, int ordinal) throws NullPointerException {
			this(null, value, ordinal);
		}
		
		/**
		 * The constructor.
		 * 
		 * @param  comment The comment, never an empty list but may be {@code
		 *         null}.
		 * @param  value The value, not allowed to be {@code null}.
		 *         The type of the value must be one of {@code String},
		 *         {@code Seq} or {@code Layout}.
		 * @param  ordinal The ordinal number which may be used for sorting the
		 *         values.
		 *         
		 * @throws NullPointerException If the specified value is {@code null}.
		 */
		Value(List<String> comment, Object value, int ordinal) throws
																			NullPointerException {
			super(comment, value);
			this.ordinal = ordinal;
		}
	}
	
	/**
	 * A comparator that compares two entries of the backing {@link #map}.
	 */
	private static final Comparator<Map.Entry<String, Value>> comparator =
									new Comparator<Map.Entry<String, Value>>() {
										@Override
										public int compare(Map.Entry<String, Value> o1,
																Map.Entry<String, Value> o2) {
											return Integer.compare(o1.getValue().ordinal,
																			o2.getValue().ordinal);
										}
									};
	
	/**
	 * The file passed via the {@link #fromFile} factory method.
	 * If this layout was constructed with a different constructor or factory
	 * method then this value is {@code null}.
	 */
	private Path file;
	
	/**
	 * The indentation, never an empty string.
	 * This value is different from {@code null} if and only if this layout
	 * was constructed with the {@link #fromFile} or the {@link #fromInputStream}
	 * factory method <em>and</em> the parsed layout did contain one or more
	 * nested elements.
	 */
	private String indent;
	
	/**
	 * The backing map of this layout.
	 */
	private final Map<String, Value> map;
	
	/**
	 * A counter initially set to zero and incremented by the {@code add} and
	 * the {@code parseEntry} methods each time a new entry is created.
	 * The entries of the backing {@link #map} tagged with the value of this
	 * counter form a natural ordering.
	 */
	private int n;
	
	/**
	 * Constructs a new empty layout.
	 */
	public Layout() {
		file = null;
		indent = null;
		map = new HashMap<>();
		n = 0;
	}
	
	/**
	 * Copy constructor.
	 * Creates a new layout and deeply copies the elements of the specified
	 * layout into it.
	 * The created layout is <em>memory independent</em> of the specified
	 * layout.
	 * 
	 * @param  layout The layout to copy, not allowed to be {@code null}.
	 * 
	 * @throws NullPointerException If {@code layout} is {@code null}.
	 */
	public Layout(Layout layout) throws NullPointerException {
		file = layout.file;
		indent = layout.indent;
		map = new HashMap<>(layout.map);
		for (java.util.Map.Entry<String, Value> entry : map.entrySet()) {
			final Value value = entry.getValue();
			final Object valueObj = value.value;
			
			final Object copy;
			if (valueObj instanceof Layout)
				copy = new Layout((Layout) valueObj);
			else if (valueObj instanceof Seq)
				copy = new Seq((Seq) valueObj);
			else {
				// valueObj instanceof String
				copy = null;
			}
			
			if (copy != null) {
				map.put(entry.getKey(), new Value(value.comment, copy,
																					value.ordinal));
			}
		}
		n = layout.n;
	}
	
	/**
	 * A pair where the first element is a string and the second element is an
	 * instance of {@link Value}.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class Entry {
		String key;
		Value value;
	}
	
	/**
	 * An iterator that iterates the lines of a text.
	 * This iterator is able to find out the level of indentation of the next
	 * line of text and to return the next line of <em>raw</em> text.
	 * A raw text is a non-empty string which starts with a character that is not
	 * a white space character and which does not end with EOL.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class LineIterator {
		private final String indent;
		private final List<String> lines;
		
		private int index;
		
		/**
		 * The constructor.
		 * 
		 * @param indent The string that specifies the {@linkplain Layout#indent
		 *        indentation}.
		 * @param lines The lines of text.
		 *        The elements are non-empty strings not ending with EOL.
		 */
		LineIterator(String indent, List<String> lines) {
			this.indent = indent;
			this.lines = lines;
			index = 0;
		}
		
		/**
		 * Computes the indentation level of the specified line of text.
		 * <p>
		 * If this line iterator was constructed with an {@code indent} argument
		 * equal to {@code null} then this method returns 0.
		 * 
		 * @param  line The line of text, not allowed to contain white spaces
		 *         only.
		 * 
		 * @return The indentation level of the specified line of text.
		 *         
		 * @throws IndentationException If the line of text starts with a
		 *         sequence of white spaces that can't be matched to a sequence
		 *         of indentations.
		 */
		private final int level(String line) throws IndentationException {
			if (indent == null)
				return 0;
			else {
				int indentLevel = 0;
				while (line.startsWith(indent)) {
					indentLevel++;
					line = line.substring(indent.length());
				}
				if (line.charAt(0) <= ' ') {
					throw new IndentationException(line.trim());
				}
				return indentLevel;
			}
		}
		
		/**
		 * Returns {@code true} if the iteration has more lines of text with the
		 * specified level of indentation.
		 * 
		 * @param  level The indentation level.
		 * 
		 * @return The boolean value {@code true} if the iteration has more lines
		 *         of text with the specified level of indentation, {@code false}
		 *         otherwise.
		 *         
		 * @throws IndentationException If the next line of text starts with a
		 *         sequence of white spaces that can't be matched to a sequence
		 *         of indentations or if the indentation level of the next
		 *         line of text is greater than the specified indentation level.
		 */
		final boolean hasNextWithLevel(int level) throws IndentationException {
			if (index < lines.size()) {
				final String nextLine = lines.get(index);
				int nextLineLevel = level(nextLine);
				if (nextLineLevel > level)
					throw new IndentationException(nextLine.trim());
				else {
					return nextLineLevel == level;
				}
			}
			else {
				return false;
			}
		}
		
		/**
		 * Finds out if the next line of text is a comment line and if so ensures
		 * that {@code hasNextWithLevel(level)} returns {@code true} for the
		 * line of text following the comment line.
		 * <p>
		 * This method assumes that {@code hasNextWithLevel(level)} returns
		 * {@code true}.
		 * 
		 * @param  level The indentation level.
		 * 
		 * @return The boolean value {@code true} if and only if the next line
		 *         of text is a comment.
		 * 
		 * @throws IndentationException If the next line of text is a comment
		 *         line and if the line of text following the comment line starts
		 *         with a sequence of white spaces that can't be matched to a
		 *         sequence of indentations or if the indentation level is
		 *         greater than the specified indentation level.
		 * @throws DanglingCommentException If the next line of text is the last
		 *         line of a comment that is not properly followed or not followed
		 *         at all by an entry or an element of a sequence.
		 */
		final boolean isNextComment(int level) throws IndentationException,
																		DanglingCommentException {
			// Precondition: hasNextWithLevel(level)
			final String nextLine = next(level);
			final boolean found = nextLine.charAt(0) == '#';
			if (found && !hasNextWithLevel(level)) {
				throw new DanglingCommentException(nextLine);
			}
			index--;
			return found;
		}
		
		/**
		 * Returns the next line of <em>raw</em> text.
		 * A raw text is a non-empty string that starts with a character that is
		 * not a white space character and that does not end with EOL.
		 * <p>
		 * This method assumes that {@code hasNextWithLevel(level)} returns
		 * {@code true}.
		 * 
		 * @param  level The indentation level.
		 * 
		 * @return The next line of raw text.
		 */
		final String next(int level) {
			// Precondition: hasNextWithLevel(level)
			final String line = lines.get(index++);
			// indent may be null if level == 0
			return level == 0 ? line : line.substring(level * indent.length());
		}
	}
	
	/**
	 * Returns a comment, if there exists a comment at the current line or
	 * {@code null} if there exists no comment at the current line.
	 * <p>
	 * Note that invoking {@code lineIterator.hasNextWithLevel(level)} will
	 * return {@code true} immediately after this method has terminated.
	 *
	 * @param  level The indentation level.
	 * @param  lineIterator The line iterator.
	 * 
	 * @return The comment or {@code null} if there is no comment starting at
	 *         the current line.
	 *
	 * @throws IndentationException If a line of text is not properly indented.
	 * @throws DanglingCommentException If there exists a comment that is not
	 *         properly followed or not followed at all by an entry or an
	 *         element of a sequence.
	 */
	private static final List<String> parseComment(int level,
								LineIterator lineIterator) throws IndentationException,
																		DanglingCommentException {
		// Precondition: lineIterator.hasNextWithLevel(level)
		final List<String> comment;
		if (lineIterator.isNextComment(level)) {
			comment = new ArrayList<>();
			do {
				comment.add(lineIterator.next(level));
			} while (lineIterator.isNextComment(level));
		}
		else {
			comment = null;
		}
		return comment;
	}
	
	/**
	 * Trims the specified string from the end.
	 * 
	 * @param  str The string to trim from the end, not allowed to be an empty
	 *         string and first character not allowed to be a white space.
	 *         
	 * @return The trimmed string or {@code str} if {@code str} does not end
	 *         with a white space character.
	 */
	private static final String trimFromEnd(String str) {
		// Precondition: !str.isEmtpy() && str.charAt(0) > ' '
		if (str.charAt(str.length() - 1) > ' ')
			return str;
		else {
			// str.length() >= 2
			int n = str.length();
			do {
				n--;
			} while (str.charAt(n - 1) <= ' ');
			return str.substring(0, n);
		}
	}
	
	/**
	 * Returns an element of a sequence.
	 * If the element is a text then this text is extracted from the next line
	 * of text.
	 * Otherwise, the element is a sequence or a layout and the sequence or the
	 * layout is extracted from the lines of texts following the next line of 
	 * ext.
	 * 
	 * @param  level The indentation level.
	 * @param  lineIterator The line iterator.
	 * 
	 * @return The newly created element.
	 * 
	 * @throws IndentationException If a line of text is not properly indented.
	 * @throws DanglingCommentException If there exists a comment that is not
	 *         properly followed or not followed at an element.
	 */
	private static final Item parseElement(int level,
										LineIterator lineIterator) throws
										IndentationException, DanglingCommentException {
		// Precondition: lineIterator.hasNextWithLevel(level)
		final List<String> comment = parseComment(level, lineIterator);
		// lineIterator.hasNextWithLevel(level)
		
		Item element;
		String line = lineIterator.next(level);
		if (line.startsWith("[]"))
			// Sequence
			element = new Item(comment, parseSeq(level + 1, lineIterator));
		else if (line.charAt(0) == '.')
			// Layout
			element = new Item(comment, parseLayout(level + 1, lineIterator));
		else {
			// Property
			element = new Item(comment, trimFromEnd(line));
		}
		
		return element;
	}
	
	/**
	 * Returns a new {@link Seq} where the elements are extracted from the lines
	 * of text forming a sequence.
	 * 
	 * @param  level The indentation level.
	 * @param  lineIterator The line iterator.
	 * 
	 * @return The newly created sequence.
	 * 
	 * @throws IndentationException If a line of text is not properly indented.
	 * @throws DanglingCommentException If there exists a comment that is not
	 *         properly followed or not followed at all by an element.
	 * 
	 */
	private static final Seq parseSeq(int level,
															LineIterator lineIterator) throws
										IndentationException, DanglingCommentException {
		final Seq seq = new Seq();

		while (lineIterator.hasNextWithLevel(level)) {
			seq.list.add(parseElement(level, lineIterator));
		}

		return seq;
	}
	
	/**
	 * Returns an {@link Entry} where the key is extracted from the next line of
	 * text forming an entry.
	 * If the entry is a property then the value is extracted from the same line
	 * of text.
	 * Otherwise, the entry is a sequence or a layout and the value is extracted
	 * from the following lines of text.
	 * 
	 * @param  level The indentation level.
	 * @param  lineIterator The line iterator.
	 * @param  ordinal The ordinal number of the entry's value.
	 * 
	 * @return The newly created entry.
	 * 
	 * @throws IndentationException If a line of text is not properly indented.
	 * @throws DanglingCommentException If there exists a comment that is not
	 *         properly followed or not followed at all by an entry.
	 */
	private static final Entry parseEntry(int level,
										LineIterator lineIterator, int ordinal) throws
										IndentationException, DanglingCommentException {
		// Precondition: lineIterator.hasNextWithLevel(level)
		final List<String> comment = parseComment(level, lineIterator);
		// lineIterator.hasNextWithLevel(level)
		
		final Entry entry = new Entry();
		final String line = trimFromEnd(lineIterator.next(level));
		int index = line.indexOf("=");
		if (index > 0) {
			// Property
			entry.key = trimFromEnd(line.substring(0, index));
			entry.value = new Value(comment, line.substring(index + 1).trim(),
																							ordinal);
		}
		else if (line.endsWith("[]")) {
			// Sequence
			entry.key = line.substring(0, line.length() - 2);
			entry.value = new Value(comment, parseSeq(level + 1, lineIterator),
																							ordinal);
		}
		else {
			// Layout
			entry.key = line;
			entry.value = new Value(comment, parseLayout(level + 1, lineIterator),
																							ordinal);
		}
		
		return entry;
	}
	
	/**
	 * Constructs a layout.
	 * 
	 * @param map The map containing the layout, not allowed to be {@code null}.
	 * @param n The {@linkplain #n counter}.
	 */
	private Layout(Map<String, Value> map, int n) {
		file = null;
		indent = null;
		this.map = map;
		this.n = n;
	}
	
	/**
	 * Returns a new layout where the entries are extracted from the lines of
	 * text forming a layout.
	 * 
	 * @param  level The indentation level.
	 * @param  lineIterator The line iterator.
	 * 
	 * @return The newly created layout.
	 * 
	 * @throws IndentationException If a line of text is not properly indented.
	 * @throws DanglingCommentException If there exists a comment that is not
	 *         properly followed or not followed at all by an entry or an
	 *         element of a sequence.
	 */
	private static final Layout parseLayout(int level,
															LineIterator lineIterator) throws
										IndentationException, DanglingCommentException {
		Map<String, Value> map = new HashMap<>();
		
		int n = 0;
		while (lineIterator.hasNextWithLevel(level)) {
			Entry entry = parseEntry(level, lineIterator, n);
			map.put(entry.key, entry.value);
			n++;
		}
		
		return new Layout(map, n);
	}
	
	/**
	 * Creates the layout from the specified lines of text.
	 * 
	 * @param  lines The lines of text, not allowed to be {@code null}.
	 * 
	 * @return The layout.
	 * 
	 * @throws NullPointerException If {@code lines} is {@code null}.
	 * @throws IndentationException If a line of text is not properly indented.
	 * @throws DanglingCommentException If there exists a comment that is not
	 *         properly followed or not followed at all by an entry or an
	 *         element of a sequence.
	 */
	private static final Layout fromLines(List<String> lines) throws
												NullPointerException, IndentationException,
																		DanglingCommentException {
		// Remove blank lines and find out indentation character(s).
		// A raw line in this context is a non-empty string that does not end
		// with EOL.
		List<String> rawLines = new ArrayList<>();
		String indent = null;
		for (String line : lines) {
			final int n = line.length();
			// Trim from start.
			int start = 0;
			while (start < n && line.charAt(start) <= ' ') {
				start++;
			}
			if (start < n) {
				// The line is not a blank line.
				if (indent == null && line.charAt(0) <= ' ') {
					// First non-blank line that starts with a white space character.
					// Find out indent!
					int index = 1;
					while (line.charAt(index) <= ' ') {
						index++;
					}
					indent = line.substring(0, index);
					// indent != null & !indent.isEmpty()
				}
				rawLines.add(line);
			}
		}
		// Parse layout.
		final Layout layout = parseLayout(0, new LineIterator(indent, rawLines));
		
		// Save computed indentation.
		layout.indent = indent;
		
		return layout;
	}

	/**
	 * Constructs a layout by reading it from the specified file.
	 * By convention, the first line of text that does not contain white spaces
	 * only and that starts with a (series of) white space(s) defines the
	 * indentation.
	 * (A white space is a character with a code less than or equal to
	 * '&#92;u0020', the code of SPACE U+0020.)
	 * <p>
	 * Blank lines, hence, lines of text containing white spaces only or even no
	 * characters at all are tolerated as well as lines of text ending with
	 * white spaces immediately before the {@link Files#readAllLines line
	 * terminator}.
	 * Note, however, that such extra sugar, unlike comments, is not retained
	 * when the layout is written back to the same file or to another file or
	 * output stream calling the {@link #toFile}, {@link #save} or {@link
	 * #toOutputStream} method.
	 * <p>
	 * This method assumes the characters in the file to be coded in UTF-8.
	 * 
	 * @param  file The file containing the layout, not allowed to be
	 *         {@code null}.
	 *         
	 * @return The layout.
	 *         
	 * @throws NullPointerException If {@code file} is {@code null}.
	 * @throws IndentationException If a line of text is not properly indented.
	 *         (See the class description to learn about a line of text that is
	 *         not properly indented.)
	 * @throws DanglingCommentException If there exists a comment that is not
	 *         properly followed or not followed at all by an entry or an
	 *         element of a sequence.
	 * @throws IOException If the specified file does not exist or if another
	 *         I/O error occurs.
	 */
	public static final Layout fromFile(Path file) throws
												NullPointerException, IndentationException,
												DanglingCommentException, IOException {
		// Read all lines from layout file.
		List<String> lines = Files.readAllLines(file, utf8);
		
		// Create layout from lines.
		final Layout layout = fromLines(lines);
		
		// Save file.
		layout.file = file;
		
		return layout;
	}
	
	/**
	 * Constructs a layout by reading it from the specified input stream.
	 * Apart from reading the layout from a stream rather than from a file
	 * invoking this factory method has exactly the same effect as invoking the
	 * {@link #fromFile} factory method.
	 * <p>
	 * The specified input stream is not closed.
	 * 
	 * @param  inputStream The input stream from where to read the layout, not
	 *         allowed to be {@code null}.
	 *         
	 * @return The layout, never {@code null} but may be an empty layout.
	 *         
	 * @throws NullPointerException If {@code inputStream} is {@code null}.
	 * @throws IndentationException If a line of text is not properly indented.
	 *         (See the class description to learn about a line of text that is
	 *         not properly indented.)
	 * @throws DanglingCommentException If there exists a comment that is not
	 *         properly followed or not followed at all by an entry or an
	 *         element of a sequence.
	 * @throws IOException If an I/O error occurs.
	 */
	public static final Layout fromInputStream(InputStream inputStream) throws
												NullPointerException, IndentationException,
												DanglingCommentException, IOException {
		List<String> lines = new ArrayList<>();
		final BufferedReader r = new BufferedReader(new InputStreamReader(
																			inputStream, utf8));
		String line = r.readLine();
		while (line != null) {
			lines.add(line);
			line = r.readLine();
		}
		
		return fromLines(lines);
	}
	
	/**
	 * Defines the sequence value type of a layout.
	 * A sequence is one of the three value types of an entry in a layout or of
	 * an element in a sequence.
	 * 
	 * @author Beat Hoermann
	 */
	public static final class Seq {
		/**
		 * The backing list of this sequence.
		 */
		private final List<Item> list;
		
		/**
		 * Constructs a new sequence with a comment equal to {@code null}.
		 */
		private Seq() {
			this.list = new ArrayList<>();
		}
		
		/**
		 * Copy constructor.
		 * Creates a new sequence and copies the elements of the specified
		 * sequence into it.
		 * The created sequence will be <em>memory independent</em> of the
		 * specified sequence.
		 * 
		 * @param seq The sequence to copy, not allowed to be {@code null}.
		 */
		private Seq(Seq seq) {
			list = new ArrayList<>(seq.list);
			
			for (ListIterator<Item> it = list.listIterator(); it.hasNext(); ) {
				final Item e = it.next();
				final Object obj = e.value;
				
				final Object copy;
				if (obj instanceof Layout)
					copy = new Layout((Layout) obj);
				else if (obj instanceof Seq)
					copy = new Seq((Seq) obj);
				else {
					copy = null;
				}
				
				if (copy != null) {
					// A comment is considered to be immutable.
					it.set(new Item(e.comment, copy));
				}
			}
		}
		
		/**
		 * Returns the number of elements in this sequence.
		 *
		 * @return The number of elements in this sequence.
		 */
		public final int size() {
			return list.size();
		}
		
		/**
		 * Returns this sequence as an {@link Collections#unmodifiableList
		 * unmodifiable} list.
		 * The elements of the returned list are the elements of this sequence
		 * in the correct order.
		 * They are of type {@code String}, {@code Seq} or {@code Layout}.
		 * 
		 * @return The sequence as an unmodifiable list.
		 */
		public final List<Object> asList() {
			final List<Object> list = new ArrayList<>(this.list.size());
			for (Item item : this.list) {
				list.add(item.value);
			}
			return Collections.unmodifiableList(list);
		}
		
		/**
		 * Returns the element at the specified position in this sequence.
		 *
		 * @param  index The index of the element to return.
		 * 
		 * @return The element at the specified position in this sequence.
		 * 
		 * @throws IndexOutOfBoundsException If the index is out of range
		 *         ({@code index} &lt; 0 || {@code index} &ge; {@code size()}).
		 */
		private final Item get(int index) throws IndexOutOfBoundsException {
			return list.get(index);
		}
		
		/**
		 * Returns the string value at the specified position in this sequence.
		 *
		 * @param  index The index of the element to return.
		 * 
		 * @return The string value at the specified position in this sequence.
		 * 
		 * @throws IndexOutOfBoundsException If the index is out of range
		 *         ({@code index} &lt; 0 || {@code index} &ge; {@code size()}).
		 * @throws ClassCastException If the stored element is not a string.
		 */
		public final String getString(int index) throws IndexOutOfBoundsException,
																				ClassCastException {
			return (String) get(index).value;
		}
		
		/**
		 * Returns the sequence at the specified position in this sequence.
		 *
		 * @param  index The index of the element to return.
		 * 
		 * @return The sequence at the specified position in this sequence.
		 * 
		 * @throws IndexOutOfBoundsException If the index is out of range
		 *         ({@code index} &lt; 0 || {@code index} &ge; {@code size()}).
		 * @throws ClassCastException If the stored element is not a sequence.
		 */
		public final Seq getSeq(int index) throws IndexOutOfBoundsException,
																				ClassCastException {
			return (Seq) get(index).value;
		}
		
		/**
		 * Returns the layout at the specified position in this sequence.
		 *
		 * @param  index The index of the element to return.
		 * 
		 * @return The layout at the specified position in this sequence.
		 * 
		 * @throws IndexOutOfBoundsException If the index is out of range
		 *         ({@code index} &lt; 0 || {@code index} &ge; {@code size()}).
		 * @throws ClassCastException If the stored element is not a layout.
		 */
		public final Layout getLayout(int index) throws IndexOutOfBoundsException,
																				ClassCastException {
			return (Layout) get(index).value;
		}
		
		/**
		 * Inserts the specified value at the specified position in this sequence.
		 * 
		 * @param  index The index at which the specified value is to be inserted.
		 * @param  value The value to be inserted.
		 * 
		 * @return This sequence.
		 * 
		 * @throws IndexOutOfBoundsException If the index is out of range
		 *         ({@code index} &lt; 0 || {@code index} &gt; {@code size()}).
		 * @throws NullPointerException If the specified value is {@code null}.
		 */
		private final Seq insertObj(int index, Object value) throws
										IndexOutOfBoundsException, NullPointerException {
			list.add(index, new Item(value));
			return this;
		}
		
		/**
		 * Inserts the specified string value at the specified position in this
		 * sequence.
		 * 
		 * @param  index The index at which the specified string value is to be
		 *         inserted.
		 * @param  value The string value to be inserted.
		 * 
		 * @return This sequence.
		 * 
		 * @throws IndexOutOfBoundsException If the index is out of range
		 *         ({@code index} &lt; 0 || {@code index} &gt; {@code size()}).
		 * @throws NullPointerException If the specified string value is
		 *         {@code null}.
		 */
		public final Seq insert(int index, String value) throws
										IndexOutOfBoundsException, NullPointerException {
			return insertObj(index, value);
		}
		
		/**
		 * Creates a new sequence, inserts it at the specified position in this
		 * sequence and returns the new sequence.
		 * 
		 * @param  index The index at which the created sequence is to be
		 *         inserted.
		 * 
		 * @return seq The newly created empty sequence, never {@code null}
		 * 
		 * @throws IndexOutOfBoundsException If the index is out of range
		 *         ({@code index} &lt; 0 || {@code index} &gt; {@code size()}).
		 */
		public final Seq insertSeq(int index) throws IndexOutOfBoundsException {
			Seq seq = new Seq();
			insert(index, seq);
			return seq;
		}
		
		/**
		 * Inserts the specified sequence at the specified position in this
		 * sequence.
		 * 
		 * @param  index The index at which the specified sequence is to be
		 *         inserted.
		 * @param  seq The sequence to be inserted.
		 * 
		 * @return This sequence.
		 * 
		 * @throws IndexOutOfBoundsException If the index is out of range
		 *         ({@code index} &lt; 0 || {@code index} &gt; {@code size()}).
		 * @throws NullPointerException If the specified sequence is {@code null}.
		 */
		public final Seq insert(int index, Seq seq) throws
										IndexOutOfBoundsException, NullPointerException {
			return insertObj(index, seq);
		}
		
		/**
		 * Creates a new layout, inserts it at the specified position in this
		 * sequence and returns the new layout.
		 * 
		 * @param  index The index at which the created layout is to be inserted.
		 * 
		 * @return The newly created empty layout, never {@code null}
		 * 
		 * @throws IndexOutOfBoundsException If the index is out of range
		 *         ({@code index} &lt; 0 || {@code index} &gt; {@code size()}).
		 */
		public final Layout insertLayout(int index) throws
																	IndexOutOfBoundsException {
			Layout layout = new Layout();
			insert(index, layout);
			return layout;
		}
		
		/**
		 * Inserts the specified layout at the specified position in this
		 * sequence.
		 * 
		 * @param  index The index at which the specified layout is to be
		 *         inserted.
		 * @param  layout The layout to be inserted.
		 * 
		 * @return This sequence.
		 * 
		 * @throws IndexOutOfBoundsException If the index is out of range
		 *         ({@code index} &lt; 0 || {@code index} &gt; {@code size()}).
		 * @throws NullPointerException If the specified layout is {@code null}.
		 */
		public final Seq insert(int index, Layout layout) throws
										IndexOutOfBoundsException, NullPointerException {
			return insertObj(index, layout);
		}
		
		/**
		 * Appends the specified value to the end of this sequence.
		 * 
		 * @param  value The value to be appended to this sequence, not allowed
		 *         to be {@code null}.
		 * 
		 * @return This sequence.
		 *         
		 * @throws NullPointerException If the specified value is {@code null}.
		 */
		private final Seq addObj(Object value) throws NullPointerException {
			list.add(new Item(value));
			return this;
		}
		
		/**
		 * Appends the specified string value to the end of this sequence.
		 * 
		 * @param  value The string value to be appended to this sequence, not
		 *         allowed to be {@code null}.
		 * 
		 * @return This sequence.
		 *         
		 * @throws NullPointerException If the specified string value is
		 *         {@code null}.
		 */
		public final Seq add(String value) throws NullPointerException {
			return addObj(value);
		}
		
		/**
		 * Creates a new sequence, appends it to the end of this sequence and
		 * returns the new sequence.
		 * 
		 * @return The newly created empty sequence, never {@code null}.
		 */
		public final Seq addSeq() {
			Seq seq = new Seq();
			add(seq);
			return seq;
		}
		
		/**
		 * Appends the specified sequence to the end of this sequence.
		 * 
		 * @param  seq The sequence to be appended to this sequence, not allowed
		 *         to be {@code null}.
		 * 
		 * @return This sequence.
		 *         
		 * @throws NullPointerException If the specified sequence is {@code null}.
		 */
		public final Seq add(Seq seq) throws NullPointerException {
			return addObj(seq);
		}
		
		/**
		 * Creates a new layout, appends it to the end of this sequence and
		 * returns the new layout.
		 * 
		 * @return The newly created empty layout, never {@code null}
		 */
		public final Layout addLayout() throws NullPointerException {
			Layout layout = new Layout();
			add(layout);
			return layout;
		}
		
		/**
		 * Appends the specified layout to the end of this sequence.
		 * 
		 * @param  layout The layout to be appended to this sequence, not allowed
		 *         to be {@code null}.
		 * 
		 * @return This sequence.
		 *         
		 * @throws NullPointerException If the specified layout is {@code null}.
		 */
		public final Seq add(Layout layout) throws NullPointerException {
			return addObj(layout);
		}
		
		/**
		 * Replaces the value at the specified position in this sequence with
		 * the specified value.
		 * 
		 * @param  index The index of the value to replace.
		 * @param  value The value to be inserted.
		 * 
		 * @return This sequence.
		 * 
		 * @throws IndexOutOfBoundsException If the index is out of range
		 *         ({@code index} &lt; 0 || {@code index} &ge; {@code size()}).
		 * @throws NullPointerException If the specified value is {@code null}.
		 * @throws ClassCastException If the stored value at the specified
		 *         position is not of the same type as the specified value.
		 */
		private final Seq replaceObj(int index, Object value) throws
										IndexOutOfBoundsException, NullPointerException,
																				ClassCastException {
			Item storedValue = get(index);
			// Create new value here to get a "nice" NullPointerException if value
			// is null. After this line of code value can not be null.
			Item newValue = new Item(value);
			if(!storedValue.value.getClass().equals(value.getClass())) {
				throw new ClassCastException();
			}
			list.set(index, newValue);
			return this;
		}
		
		/**
		 * Replaces the string value at the specified position in this sequence
		 * with the specified string value.
		 * 
		 * @param  index The index of the string value to replace.
		 * @param  value The value to be inserted.
		 * 
		 * @return This sequence.
		 * 
		 * @throws IndexOutOfBoundsException If the index is out of range
		 *         ({@code index} &lt; 0 || {@code index} &gt; {@code size()}).
		 * @throws NullPointerException If the specified value is {@code null}.
		 * @throws ClassCastException If the stored value at the specified
		 *         position is not a string.
		 */
		public final Seq replace(int index, String value) throws
										IndexOutOfBoundsException, NullPointerException,
																				ClassCastException {
			return replaceObj(index, value);
		}
		
		/**
		 * Replaces the sequence at the specified position in this sequence with
		 * the specified sequence.
		 * 
		 * @param  index The index of the sequence to replace.
		 * @param  seq The sequence to be inserted.
		 * 
		 * @return This sequence.
		 * 
		 * @throws IndexOutOfBoundsException If the index is out of range
		 *         ({@code index} &lt; 0 || {@code index} &ge; {@code size()}).
		 * @throws NullPointerException If the specified sequence is {@code null}.
		 * @throws ClassCastException If the stored value at the specified
		 *         position is not a sequence.
		 */
		public final Seq replace(int index, Seq seq) throws
										IndexOutOfBoundsException, NullPointerException,
																				ClassCastException {
			return replaceObj(index, seq);
		}
		
		/**
		 * Replaces the layout at the specified position in this sequence with the
		 * specified layout.
		 * 
		 * @param  index The index of the layout to replace.
		 * @param  layout The layout to be inserted.
		 * 
		 * @return This sequence.
		 * 
		 * @throws IndexOutOfBoundsException If the index is out of range
		 *         ({@code index} &lt; 0 || {@code index} &ge; {@code size()}).
		 * @throws NullPointerException If the specified layout is {@code null}.
		 * @throws ClassCastException If the stored value at the specified
		 *         position is not a layout.
		 */
		public final Seq replace(int index, Layout layout) throws
										IndexOutOfBoundsException, NullPointerException,
																				ClassCastException {
			return replaceObj(index, layout);
		}
		
		/**
		 * Removes the element at the specified position in this sequence.
		 * 
		 * @param  index The index of the element to be removed.
		 * 
		 * @return This sequence.
		 * 
		 * @throws IndexOutOfBoundsException If the index is out of range
		 *         ({@code index} &lt; 0 || {@code index} &ge; {@code size()}).
		 */
		public final Seq remove(int index) throws IndexOutOfBoundsException {
			list.remove(index);
			return this;
		}
	}
	
	/**
	 * Converts this layout to an {@link Collections#unmodifiableMap
	 * unmodifiable} map.
	 * The keys and the values of the returned map are the keys and the values
	 * of this layout.
	 * The values are of type {@code String}, {@code Seq} or {@code Layout}.
	 * 
	 * @return The layout as an unmodifiable map.
	 */
	public final Map<String, Object> toMap() {
		Map<String, Object> map = new HashMap<>((int) (this.map.size() * 4L / 3) +
																									1);
		for (Map.Entry<String, Value> entry : this.map.entrySet()) {
			map.put(entry.getKey(), entry.getValue().value);
		}
		return Collections.unmodifiableMap(map);
	}
	
	/**
	 * A layout entry.
	 * 
	 * @author Beat Hoermann
	 */
	public static final class LtEntry {
		/**
		 * The key, never {@code null}.
		 */
		public final String key;
		/**
		 * The value, never {@code null}.
		 * The type of the value is one of {@code String}, {@code Seq} or {@code
		 * Layout}.
		 */
		public final Object value;
		
		/**
		 * The constructor.
		 * 
		 * @param key The key, not allowed to be {@code null}.
		 * @param value The value, not allowed to be {@code null}.
		 *        The type of the value must be one of {@code String}, {@code Seq}
		 *        or {@code Layout}.
		 */
		private LtEntry(String key, Object value) {
			this.key = key;
			this.value = value;
		}
	}
	
	/**
	 * Returns the entries of this layout.
	 * The entries are sorted in the order of their creation.
	 * For example, if this layout was obtained from a file or an input stream
	 * then the entries appear in the same order as they were originally read
	 * from the input stream or the file.
	 * 
	 * @return The sorted array of entries of this layout, never {@code null}.
	 */
	public final LtEntry[] entries() {
		List<Map.Entry<String, Value>> entries = new ArrayList<>(map.entrySet());
		Collections.sort(entries, comparator);
		
		LtEntry[] tlEntries = new LtEntry[entries.size()];
		for (int i = 0; i < tlEntries.length; i++) {
			Map.Entry<String, Value> entry = entries.get(i);
			tlEntries[i] = new LtEntry(entry.getKey(), entry.getValue().value);
		}
		return tlEntries;
	}
	
	/**
	 * Returns the number of entries in this layout.
	 * 
	 * @return The number of entries in this layout.
	 */
	public final int size() {
		return map.size();
	}
	
	/**
	 * Returns {@code true} if this layout contains an entry with the specified
	 * key.
	 * 
	 * @param  key The key whose presence in the layout is to be tested.
	 * 
	 * @return The boolean value {@code true} if this layout contains an entry
	 *         with the specified key.
	 *         
	 * @throws NullPointerException If the specified key is {@code null}.
	 */
	public final boolean contains(String key) throws NullPointerException {
		return map.containsKey(Objects.requireNonNull(key));
	}
	
	/**
	 * Returns the value with the specified key.
	 * 
	 * @param  key The key whose associated value is to be returned.
	 * 
	 * @return The value with the specified key, never {@code null}.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified key.
	 * @throws NullPointerException If the specified key is {@code null}.
	 */
	private final Value get(String key) throws MissingEntryException,
																			NullPointerException {
		if (!contains(key)) {
			throw new MissingEntryException("No entry for \"" + key + "\".");
		}
		return map.get(key);
	}
	
	/**
	 * Returns the string with the specified key.
	 * 
	 * @param  key The key whose associated string is to be returned.
	 * 
	 * @return The string with the specified key, never {@code null}.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified key.
	 * @throws NullPointerException If the specified key is {@code null}.
	 * @throws ClassCastException If the stored value with the specified key is
	 *         not a string.
	 */
	public final String getString(String key) throws MissingEntryException,
												NullPointerException, ClassCastException {
		return (String) get(key).value;
	}
	
	/**
	 * Returns the sequence with the specified key.
	 * 
	 * @param  key The key whose associated sequence is to be returned.
	 * 
	 * @return The sequence with the specified key, never {@code null}.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified key.
	 * @throws NullPointerException If the specified key is {@code null}.
	 * @throws ClassCastException If the stored value with the specified key is
	 *         not a sequence.
	 */
	public final Seq getSeq(String key) throws MissingEntryException,
												NullPointerException, ClassCastException {
		return (Seq) get(key).value;
	}
	
	/**
	 * Returns the layout with the specified key.
	 * 
	 * @param  key The key whose associated layout is to be returned.
	 * 
	 * @return The layout with the specified key, never {@code null}.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified key.
	 * @throws NullPointerException If the specified key is {@code null}.
	 * @throws ClassCastException If the stored value with the specified key is
	 *         not a layout.
	 */
	public final Layout getLayout(String key) throws MissingEntryException,
												NullPointerException, ClassCastException {
		return (Layout) get(key).value;
	}
	
	/**
	 * Returns the value with the specified <em>qualified key</em>.
	 * <p>
	 * A qualified key denotes a sequence of normal keys where the keys are
	 * separated from each other by the specified separator character, as, for
	 * instance, in {@code profession.words.tableClassName} with a separator
	 * character equal to the dot ('{@code .}').
	 * All but the last key must map to a layout and are therefore called the
	 * <em>layout keys</em>.
	 * (In the example above, {@code profession} and {@code words} are layout
	 * keys.)
	 * <p>
	 * A qualified key not containing the separator character has no layout keys
	 * and thus reduces to a normal key.
	 * 
	 * @param  separator The separator character.
	 * @param  qualKey The qualified key whose associated value is to be
	 *         returned.
	 * 
	 * @return The value with the qualified key, never {@code null}.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified qualified key.
	 * @throws NullPointerException If the specified qualified key is
	 *         {@code null}.
	 * @throws ClassCastException If the stored value of one of the layout keys
	 *         of the specified qualified key is not a layout.
	 */
	private final Value getByQualKey(String qualKey, char separator) throws
					MissingEntryException, NullPointerException, ClassCastException {
		Layout cur = this;
		int separatorIndex = qualKey.indexOf(separator);
		while (separatorIndex >= 0) {
			cur = cur.getLayout(qualKey.substring(0, separatorIndex));
			qualKey = qualKey.substring(separatorIndex + 1);
			separatorIndex = qualKey.indexOf(separatorIndex);	
		}
		return cur.get(qualKey);
	}
	
	/**
	 * Returns the string with the specified <em>qualified key</em>.
	 * <p>
	 * The result and behaviour of invoking this method is identical to the
	 * result and behaviour of invoking the {@link #getStringByQualKey(String,
	 * char)} method with a separator character equal to the dot ('{@code .}')
	 * character.
	 * 
	 * @param  qualKey The qualified key whose associated string is to be
	 *         returned.
	 * 
	 * @return The string with the specified qualified key, never {@code null}.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified qualified key.
	 * @throws NullPointerException If the specified qualified key is
	 *         {@code null}.
	 * @throws ClassCastException If the stored value of one of the layout keys
	 *         of the specified qualified key is not a layout or if the stored
	 *         value with the last key is not a string.
	 */
	public final String getStringByQualKey(String qualKey) throws
					MissingEntryException, NullPointerException, ClassCastException {
		return (String) getByQualKey(qualKey, '.').value;
	}
	
	/**
	 * Returns the string with the specified <em>qualified key</em>, the keys
	 * separated by the specified separator character.
	 * <p>
	 * A qualified key denotes a sequence of normal keys where the keys are
	 * separated from each other by the specified separator character, as, for
	 * instance, in {@code profession.words.tableClassName} with a separator
	 * character equal to the dot ('{@code .}').
	 * All but the last key must map to a layout and are therefore called the
	 * <em>layout keys</em>.
	 * (In the example above, {@code profession} and {@code words} are layout
	 * keys.)
	 * <p>
	 * A qualified key not containing the separator character has no layout keys
	 * and thus reduces to a normal key.
	 * 
	 * @param  separator The separator character.
	 * @param  qualKey The qualified key whose associated string is to be
	 *         returned.
	 * 
	 * @return The string with the specified qualified key, never {@code null}.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified qualified key.
	 * @throws NullPointerException If the specified qualified key is
	 *         {@code null}.
	 * @throws ClassCastException If the stored value of one of the layout keys
	 *         of the specified qualified key is not a layout or if the stored
	 *         value with the last key is not a string.
	 */
	public final String getStringByQualKey(String qualKey, char separator) throws
					MissingEntryException, NullPointerException, ClassCastException {
		return (String) getByQualKey(qualKey, separator).value;
	}
	
	/**
	 * Returns the sequence with the specified <em>qualified key</em>.
	 * <p>
	 * The result and behaviour of invoking this method is identical to the
	 * result and behaviour of invoking the {@link #getSeqByQualKey(String,
	 * char)} method with a separator character equal to the dot ('{@code .}')
	 * character.
	 * 
	 * @param  qualKey The qualified key whose associated sequence is to be
	 *         returned.
	 * 
	 * @return The sequence with the specified qualified key, never {@code null}.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified qualified key.
	 * @throws NullPointerException If the specified qualified key is
	 *         {@code null}.
	 * @throws ClassCastException If the stored value of one of the layout keys
	 *         of the specified qualified key is not a layout or if the stored
	 *         value with the last key is not a sequence.
	 */
	public final Seq getSeqByQualKey(String qualKey) throws
					MissingEntryException, NullPointerException, ClassCastException {
		return (Seq) getByQualKey(qualKey, '.').value;
	}
	
	/**
	 * Returns the sequence with the specified <em>qualified key</em>, the keys
	 * separated by the specified separator character.
	 * <p>
	 * A qualified key denotes a sequence of normal keys where the keys are
	 * separated from each other by the specified separator character, as, for
	 * instance, in {@code profession.words.tableClassName} with a separator
	 * character equal to the dot ('{@code .}').
	 * All but the last key must map to a layout and are therefore called the
	 * <em>layout keys</em>.
	 * (In the example above, {@code profession} and {@code words} are layout
	 * keys.)
	 * <p>
	 * A qualified key not containing the separator character has no layout keys
	 * and thus reduces to a normal key.
	 * 
	 * @param  qualKey The qualified key whose associated sequence is to be
	 *         returned.
	 * @param  separator The separator character.
	 * 
	 * @return The sequence with the specified qualified key, never {@code null}.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified qualified key.
	 * @throws NullPointerException If the specified qualified key is
	 *         {@code null}.
	 * @throws ClassCastException If the stored value of one of the layout keys
	 *         of the specified qualified key is not a layout or if the stored
	 *         value with the last key is not a sequence.
	 */
	public final Seq getSeqByQualKey(String qualKey, char separator) throws
					MissingEntryException, NullPointerException, ClassCastException {
		return (Seq) getByQualKey(qualKey, separator).value;
	}
	
	/**
	 * Returns the layout with the specified <em>qualified key</em>.
	 * <p>
	 * The result and behaviour of invoking this method is identical to the
	 * result and behaviour of invoking the {@link #getLayoutByQualKey(String,
	 * char)} method with a separator character equal to the dot ('{@code .}')
	 * character.
	 * 
	 * @param  qualKey The qualified key whose associated layout is to be
	 *         returned.
	 * 
	 * @return The layout with the specified qualified key, never {@code null}.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified qualified key.
	 * @throws NullPointerException If the specified qualified key is
	 *         {@code null}.
	 * @throws ClassCastException If the stored value of one of the keys of the
	 *         specified qualified key is not a layout.
	 */
	public final Layout getLayoutByQualKey(String qualKey) throws
					MissingEntryException, NullPointerException, ClassCastException {
		return (Layout) getByQualKey(qualKey, '.').value;
	}
	
	/**
	 * Returns the layout with the specified <em>qualified key</em>, the keys
	 * separated by the specified separator character.
	 * <p>
	 * A qualified key denotes a sequence of normal keys where the keys are
	 * separated from each other by the specified separator character, as, for
	 * instance, in {@code profession.words.tableClassName} with a separator
	 * character equal to the dot ('{@code .}').
	 * All but the last key must map to a layout and are therefore called the
	 * <em>layout keys</em>.
	 * (In the example above, {@code profession} and {@code words} are layout
	 * keys.)
	 * <p>
	 * A qualified key not containing the separator character has no layout keys
	 * and thus reduces to a normal key.
	 * 
	 * @param  qualKey The qualified key whose associated layout is to be
	 *         returned.
	 * @param  separator The separator character.
	 * 
	 * @return The layout with the specified qualified key, never {@code null}.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified qualified key.
	 * @throws NullPointerException If the specified qualified key is
	 *         {@code null}.
	 * @throws ClassCastException If the stored value of one of the keys of the
	 *         specified qualified key is not a layout.
	 */
	public final Layout getLayoutByQualKey(String qualKey, char separator) throws
					MissingEntryException, NullPointerException, ClassCastException {
		return (Layout) getByQualKey(qualKey, separator).value;
	}
	
	/**
	 * Adds the specified value with the specified key.
	 * 
	 * @param  key The key with which the specified value is to be associated.
	 * @param  value The value to be associated with the specified key.
	 * 
	 * @return This layout.
	 * 
	 * @throws IllegalArgumentException If the layout already contains an entry
	 *         with the specified key.
	 * @throws NullPointerException If the specified key or value is
	 *         {@code null}.
	 */
	private final Layout addObj(String key, Object value) throws
										IllegalArgumentException, NullPointerException {
		if (contains(key)) {
			throw new IllegalArgumentException("\"" + key +
																		"\" already contained.");
		}
		map.put(key, new Value(value, n++));
		return this;
	}
	
	/**
	 * Adds the specified string value with the specified key.
	 * 
	 * @param  key The key with which the specified string value is to be
	 *         associated.
	 * @param  value The string value to be associated with the specified key.
	 * 
	 * @return This layout.
	 * 
	 * @throws IllegalArgumentException If the layout already contains an entry
	 *         with the specified key.
	 * @throws NullPointerException If the specified key or string value is
	 *         {@code null}.
	 */
	public final Layout add(String key, String value) throws
										IllegalArgumentException, NullPointerException {
		return addObj(key, value);
	}
	
	/**
	 * Creates a new sequence, adds it to this layout with the specified key and
	 * returns the new sequence.
	 * 
	 * @param  key The key with which the new sequence is to be associated.
	 *         
	 * @return The newly created empty sequence, never {@code null}.
	 * 
	 * @throws IllegalArgumentException If the layout already contains an entry
	 *         with the specified key.
	 * @throws NullPointerException If the specified key is {@code null}.
	 */
	public final Seq addSeq(String key) throws
										IllegalArgumentException, NullPointerException {
		Seq seq = new Seq();
		add(key, seq);
		return seq;
	}
	
	/**
	 * Adds the specified sequence with the specified key.
	 * 
	 * @param  key The key with which the specified sequence is to be associated.
	 * @param  seq The sequence to be associated with the specified key.
	 * 
	 * @return This layout.
	 * 
	 * @throws IllegalArgumentException If the layout already contains an entry
	 *         with the specified key.
	 * @throws NullPointerException If the specified key or sequence is
	 *         {@code null}.
	 */
	public final Layout add(String key, Seq seq) throws
										IllegalArgumentException, NullPointerException {
		return addObj(key, seq);
	}
	
	/**
	 * Creates a new layout, adds it to this layout with the specified key and
	 * returns the new layout.
	 * 
	 * @param  key The key with which the new layout is to be associated.
	 *         
	 * @return The newly created empty layout, never {@code null}.
	 * 
	 * @throws IllegalArgumentException If the layout already contains an entry
	 *         with the specified key.
	 * @throws NullPointerException If the specified key is {@code null}.
	 */
	public final Layout addLayout(String key) throws IllegalArgumentException,
																			NullPointerException {
		Layout layout = new Layout();
		add(key, layout);
		return layout;
	}
	
	/**
	 * Adds the specified layout with the specified key.
	 * 
	 * @param  key The key with which the specified layout is to be associated.
	 * @param  layout The layout to be associated with the specified key.
	 * 
	 * @return This layout.
	 * 
	 * @throws IllegalArgumentException If the layout already contains an entry
	 *         with the specified key.
	 * @throws NullPointerException If the specified key or layout is
	 *         {@code null}.
	 */
	public final Layout add(String key, Layout layout) throws
										IllegalArgumentException, NullPointerException {
		return addObj(key, layout);
	}
	
	/**
	 * Replaces the stored value having the specified key with the specified
	 * value.
	 * 
	 * @param  key The key whose value is to be replaced.
	 * @param  value The value, not allowed to be {@code null}.
	 * 
	 * @return This layout.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified key.
	 * @throws NullPointerException If the specified key or value is
	 *         {@code null}.
	 * @throws ClassCastException If the stored value with the specified key is
	 *         not of the same type as the specified value.
	 */
	private final Layout replaceObj(String key, Object value) throws
					MissingEntryException, NullPointerException, ClassCastException {
		Value storedValue = get(key);
		// Create new value here to get a "nice" NullPointerException if value is
		// null. After this line of code value cannot be null.
		Value newValue = new Value(value, storedValue.ordinal);
		if(!storedValue.value.getClass().equals(value.getClass())) {
			throw new ClassCastException();
		}
		map.put(key, newValue);
		return this;
	}
	
	/**
	 * Replaces the stored string value having the specified key with the
	 * specified string value.
	 * 
	 * @param  key The key whose value is to be replaced.
	 * @param  value The string value, not allowed to be {@code null}.
	 * 
	 * @return This layout.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified key.
	 * @throws NullPointerException If the specified key or string value is 
	 *         {@code null}.
	 * @throws ClassCastException If the stored value with the specified key is
	 *         not a string.
	 */
	public final Layout replace(String key, String value) throws
					MissingEntryException, NullPointerException, ClassCastException {
		return replaceObj(key, value);
	}
	
	/**
	 * Replaces the stored sequence having the specified key with the specified
	 * sequence.
	 * 
	 * @param  key The key whose value is to be replaced.
	 * @param  seq The sequence, not allowed to be {@code null}.
	 * 
	 * @return This layout.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified key.
	 * @throws NullPointerException If the specified key or sequence is
	 *         {@code null}.
	 * @throws ClassCastException If the stored value with the specified key is
	 *         not a sequence.
	 */
	public final Layout replace(String key, Seq seq) throws
					MissingEntryException, NullPointerException, ClassCastException {
		return replaceObj(key, seq);
	}
	
	/**
	 * Replaces the stored layout having the specified key with the specified
	 * layout.
	 * 
	 * @param  key The key whose value is to be replaced.
	 * @param  layout The layout, not allowed to be {@code null}.
	 * 
	 * @return This layout.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified key.
	 * @throws NullPointerException If the specified key or layout is 
	 *         {@code null}.
	 * @throws ClassCastException If the stored value with the specified key is
	 *         not a layout.
	 */
	public final Layout replace(String key, Layout layout) throws
					MissingEntryException, NullPointerException, ClassCastException {
		return replaceObj(key, layout);
	}
	
	/**
	 * Replaces the specified current key with the specified new key.
	 * <p>
	 * This method has no effect if the current key is equal to the new key.
	 * 
	 * @param  key The current key.
	 * @param  newKey The new key.
	 * 
	 * @return This layout.
	 * 
	 * @throws MissingEntryException If the layout contains no entry with the
	 *         specified current key.
	 * @throws IllegalArgumentException If the layout already contains an entry
	 *         with the specified new key.
	 * @throws NullPointerException If one of the arguments is {@code null}.
	 */
	public final Layout replaceKey(String key, String newKey) throws
										MissingEntryException, IllegalArgumentException,
																			NullPointerException {
		if (!key.equals(newKey)) {
			Value value = get(key);
			if (contains(newKey)) {
				throw new IllegalArgumentException("\"" + newKey +
																		"\" already contained.");
			}
			map.remove(key);
			map.put(newKey, value);
		}
		return this;
	}
	
	/**
	 * Removes the entry with the specified key.
	 * 
	 * @param  key The key whose entry is to be removed.
	 * 
	 * @return This layout.
	 * 
	 * @throws MissingEntryException If the layout contains no entry for the
	 *         specified key.
	 * @throws NullPointerException If the specified key is {@code null}.
	 */
	public final Layout remove(String key) throws MissingEntryException,
																			NullPointerException {
		if (!contains(key)) {
			throw new IllegalArgumentException("No entry for \"" + key + "\".");
		}
		map.remove(key);
		return this;
	}
	
	/**
	 * Returns the indentation discovered in the file or input stream when this
	 * layout was constructed with the {@link #fromFile} or
	 * {@link #fromInputStream} factory method, respectively.
	 * 
	 * @return The indentation, never an empty string.
	 *         The value is {@code null} if the layout was not constructed with
	 *         one of the factory methods mentioned above or if the layout has
	 *         no indented elements.
	 */
	public final String indent() {
		return indent;
	}
	
	/**
	 * The line collector collects lines of text and prepends to each line of
	 * text its proper indentation.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class LineCollector {
		private final String indent;
		private final List<String> lines;
		
		/**
		 * The constructor.
		 * 
		 * @param indent The string that specifies the indentation.
		 */
		LineCollector(String indent) {
			this.indent = indent;
			this.lines = new ArrayList<>();
		}
		
		/**
 		 * Computes the indentation depending on the specified indentation level.
		 * 
		 * @param  level The indentation level.
		 * 
		 * @return The indentation.
		 */
		private final String indentation(int level) {
			StringBuilder sb = new StringBuilder(level * indent.length());
			while (level > 0) {
				sb.append(indent);
				level--;
			}
			return sb.toString();
		}
		
		/**
		 * Adds a single line of text.
		 * The specified line of text is prepended by the proper indentation
		 * prior to adding it.
		 * 
		 * @param level The indentation level.
		 * @param line The line of text.
		 */
		final void addLine(int level, String line) {
			lines.add(indentation(level) + line);
		}
		
		/**
		 * Returns the collected lines of text.
		 * 
		 * @return The collected lines of text, never {@code null}.
		 */
		final List<String> lines() {
			return lines;
		}
	}
	
	/**
	 * If the specified entry or element has a comment then this methods sends
	 * the individual comment lines to the line collector.
	 *
	 * @param level The indentation level.
	 * @param item The entry or element with the optional comment.
	 * @param lc The line collector.
	 */
	private final void convertComment(int level, Item item, LineCollector lc) {
		if (item.comment != null) {
			for (String cLine : item.comment) {
				lc.addLine(level, cLine);
			}
		}
	}
	
	/**
	 * Converts the specified element.
	 * 
	 * @param level The indentation level.
	 * @param element The element of a sequence.
	 * @param lc The line collector.
	 */
	private final void convertElement(int level, Item element,
																				LineCollector lc) {
		convertComment(level, element, lc);
		Object elObj = element.value;
		if (elObj instanceof String)
			lc.addLine(level, (String) elObj);
		else if (elObj instanceof Seq) {
			lc.addLine(level, "[]");
			convertSeq(level + 1, (Seq) elObj, lc);
		}
		else { // elObj instanceof Layout
			lc.addLine(level, ".");
			convertLayout(level + 1, (Layout) elObj, lc);
		}
	}
	
	/**
	 * Converts the elements of the specified sequence by calling the {@link
	 * #convertElement} method for each element.
	 * 
	 * @param level The indentation level.
	 * @param seq The sequence.
	 * @param lc The line collector.
	 */
	private final void convertSeq(int level, Seq seq, LineCollector lc) {
		for (int i = 0; i < seq.size(); i++) {
			convertElement(level, seq.get(i), lc);
		}
	}
	
	/**
	 * Converts the specified entry.
	 * For better readability the equal character ('{@code =}') in an entry
	 * representing a property is surrounded with a space character.
	 * 
	 * @param level The indentation level.
	 * @param entry The entry of a layout.
	 * @param lc The line collector.
	 */
	private final void convertEntry(int level, Map.Entry<String, Value> entry, 
																				LineCollector lc) {
		final Value value = entry.getValue();
		
		convertComment(level, value, lc);
		final StringBuilder sb = new StringBuilder(entry.getKey());
		final Object valueObj = value.value;
		if (valueObj instanceof String)
			lc.addLine(level, sb.append(" = " + (String) valueObj).toString());
		else if (valueObj instanceof Seq) {
			lc.addLine(level, sb.append("[]").toString());
			convertSeq(level + 1, (Seq) valueObj, lc);
		}
		else { // value instanceof Layout
			lc.addLine(level, sb.toString());
			convertLayout(level + 1, (Layout) valueObj, lc);
		}
	}
	
	/**
	 * Converts the entries of the specified map by calling the {@link
	 * #convertEntry} method for each entry.
	 * Prior to converting the entries, this method sorts the entries according
	 * to the ordinals stored in the value part of the entries.
	 * 
	 * @param level The indentation level.
	 * @param layout The layout.
	 * @param lc The line collector.
	 */
	private final void convertLayout(int level, Layout layout, LineCollector lc){
		List<Map.Entry<String, Value>> entries = new ArrayList<>(
																			layout.map.entrySet());
		Collections.sort(entries, comparator);
		for (Map.Entry<String, Value> entry : entries) {
			convertEntry(level, entry, lc);
		}
	}
	
	/**
	 * Tests if all characters of the specified string are white spaces.
	 * 
	 * @param  str The string to test.
	 * 
	 * @return The boolean value {@code true} if all characters of the string
	 *         are white spaces, {@code false} otherwise.
	 */
	private final boolean wsOnly(String str) {
		boolean wsOnly = true;
		int i = 0;
		while (i < str.length() && wsOnly) {
			if (str.charAt(i++) > ' ') {
				wsOnly = false;
			}
		}
		return wsOnly;
	}
	
	/**
	 * Creates and returns a {@code LineCollector} filled with the content
	 * of this layout.
	 * 
	 * @param  indent The indentation to use for nested layouts or {@code null}
	 *         in which case an indentation equal to CHARACTER TABULATION U+0009
	 *         is used.
	 *         
	 * @return The line collector, never {@code null}.
	 * 
	 * @throws IllegalArgumentException If the specified indentation contains at
	 *         least one character that is not a white space.
	 */
	private final LineCollector toLineCollector(String indent) throws
																		IllegalArgumentException {
		if (indent != null && !wsOnly(indent))
			throw new IllegalArgumentException("At least one character of the " +
									"indent \"" + indent + "\" is not a white space.");
		else if (indent == null || indent.isEmpty()) {
			indent = "\t";
		}
		
		// Create the line writer and write the layout to it.
		LineCollector lc = new LineCollector(indent);
		convertLayout(0, this, lc);
		
		return lc;
	}
	
	/**
	 * Writes this layout to the specified file.
	 * Nested layouts and sequences are indented using the specified indentation
	 * or the tab character (CHARACTER TABULATION (U+0009)) if the specified
	 * indentation is {@code null} or an empty string.
	 * The written layout strictly conforms to the syntax rules given in the
	 * class description.
	 * <p>
	 * The entries of a layout appear in the order of their creation.
	 * This guarantees that a layout that was originally read from a file or an
	 * input stream (see {@link #fromFile} or {@link #fromInputStream}) looks
	 * "similar" to the layout later written back to the same file or to another
	 * file.
	 * Comments are retained.
	 * However, blank lines in the original file and the character (or sequence
	 * of characters) used for indendation, as well as extra white spaces
	 * immediately before the end of a line of text are not retained.
	 * Furthermore, the equal character ('{@code =}') in a property is
	 * surrounded by a single space character, ignoring any other surrounding
	 * sequences of white spaces in the original file or input stream.
	 * <p>
	 * Uses UTF-8 for character encoding.
	 * <p>
	 * This method fails if the file is locked.
	 * 
	 * @param  file The file to which to write this layout, not allowed to be
	 *         {@code null}.
	 * @param  indent The indentation to use for nested layouts or {@code null}
	 *         in which case an indentation equal to CHARACTER TABULATION U+0009
	 *         is used.
	 * @param  options The options specifying how the file is opened, see the
	 *         {@link Files#write(Path, Iterable, Charset, OpenOption...)
	 *         Files.write} method description for any details.
	 *         
	 * @throws IllegalArgumentException If the specified indentation contains at
	 *         least one character that is not a white space.
	 * @throws NullPointerException If the specified file is {@code null}.
	 * @throws UnsupportedOperationException If an unsupported option is
	 *         specified.
	 * @throws IOException If an I/O error occurs.
	 */
	public final void toFile(Path file, String indent,
								OpenOption... options) throws IllegalArgumentException,
								NullPointerException, UnsupportedOperationException,
																						IOException {
		final LineCollector lc = toLineCollector(indent);
		
		// Write all lines to the layout file.
		Files.write(file, lc.lines(), utf8, options);
	}
	
	/**
	 * Invokes the {@link #toFile} method with the file identical to the {@code
	 * file} argument of the {@link #fromFile} factory method and with no open
	 * options.
	 * 
	 * @throws UnsupportedOperationException If this layout was constructed with
	 *         a constructor different from the {@code fromFile} factory method.
	 * @throws IOException If an I/O error occurs.
	 */
	public final void save() throws UnsupportedOperationException, IOException {
		if (file == null) {
			throw new UnsupportedOperationException("This operation is " +
									"supported only for layouts that were constructed " +
									"with the \"fromFile\" factory method.");
		}
		toFile(file, indent);
	}
	
	/**
	 * Writes this layout to the specified output stream.
	 * Apart from writing the layout to a stream rather than to a file invoking
	 * this method has exactly the same effect as invoking the {@link #toFile}
	 * method.
	 * <p>
	 * The specified output stream is not closed.
	 * 
	 * @param  stream The output stream to which to write this layout, not
	 *         allowed to be {@code null}.
	 * @param  indent The indentation to use for nested layouts or {@code null}
	 *         in which case an indentation equal to CHARACTER TABULATION U+0009
	 *         is used.
	 *         
	 * @throws IllegalArgumentException If the specified indentation contains at
	 *         least one character that is not a white space.
	 * @throws NullPointerException If the specified stream is {@code null}.
	 * @throws IOException If an I/O error occurs.
	 */
	public final void toOutputStream(OutputStream stream, String indent) throws
						IllegalArgumentException, NullPointerException, IOException {
		final LineCollector lc = toLineCollector(indent);
		
		// Write all lines to the stream.
		final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
																					stream, utf8));
		for (CharSequence line: lc.lines()) {
			writer.append(line);
			writer.newLine();
		}
		// Since closing "writer" would close "stream" we do not close "writer".
		// BufferedWriter and OutputStreamWriter should not hold any system
		// resources. If we don't close "writer" then we must flush it, because
		// otherwise we have experienced that data beyound 8192 bytes gets lost.
		writer.flush();
	}
}