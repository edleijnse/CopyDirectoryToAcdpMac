/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp;

import java.io.Serializable;

/**
 * Defines a reference to a row in a {@linkplain Table table}.
 * Given a reference, ACDP can access a row without the need for a search or
 * a lookup in an index.
 * <p>
 * Since ACDP never relocates a row, a reference never gets invalid by ACDP's
 * file space management.
 * Moreover, even changing the structure of a table does not invalidate any
 * existing references.
 * Overall, references are <em>safe for a "disciplined" user even across
 * session boundaries</em>.
 * <p>
 * For reasons of efficiency and due to the author's believe that it is easy
 * for the disciplined user to ensure that a reference is used with no other
 * table than the table actually housing the referenced row, a value of type
 * {@code Ref} consists of a pointer to the referenced row only.
 * The information to which table the referenced row belongs is not part of the
 * value of the reference.
 * As described below, applying a reference with a table that does not house
 * the referenced row raises an exception only if the reference turns out to be
 * <em>illegal</em>.
 * <p>
 * Furthermore, a disciplined user is expected to be aware of the fact that
 * if some client deletes a row then any references to this row become
 * <em>invalid</em>.
 * Even an update of a referenced row may semantically invalidate the reference
 * from the point of view of the reference holder.
 * In a setting, where several users simultaneously delete and update rows,
 * even a disciplined user may loose control over the validity of his or her
 * references.
 * At least, the user can be assured that references received within a
 * {@linkplain Unit unit} or a {@linkplain ReadZone read zone} and exclusively
 * used within that unit or read zone are not vulnerable in this regard.
 * <p>
 * Another reason for a reference getting invalid is the relocation
 * of rows on the storage device caused by an externally invoked process which
 * <em>{@linkplain Table#compactFL compacts}</em> the FL file space of the WR
 * store associated with the dedicated table.
 * (See section "Compacting the WR Store" in the description of the {@code
 * Table} interface to learn about the FL file space of a WR store.)
 * However, there is almost never a need for compacting the FL file space:
 * One and perhaps the only one reason for compacting the FL file space might
 * be to free the space occupied by many <em>row gaps</em> in a situation where
 * there is no perspective that these row gaps may ever be reused by ACDP,
 * simply because there is no need for inserting any new rows.
 * (A table may contain <em>row gaps</em>, that are rows formerly inserted and
 * later deleted.
 * The associated file space of a row gap is at the disposition of the file
 * space manager waiting for being reused.)
 * To avoid references becoming invalid in the middle of a session due to
 * compaction, simply do not compact the FL file space during a session.
 * <p>
 * Trivially, references are always valid, even across session boundaries, if
 * the database is read-only.
 * In a setting, where only a single user can delete and update a row within a
 * session (all other sessions are blocked) the validity of references across
 * session boundaries can easily be established by means of "organisational
 * measures".
 * For instance, references may be persisted between two sessions along with
 * the database version.
 * If each "changeset" (including a changeset as a result of compacting a table)
 * gets its own database version then a change in the database version can be
 * considered as a warning that some or all persisted references may be
 * invalid.
 * Of course, more sophisticated procedures are conceivable.
 * <p>
 * The use of an <em>illegal</em> reference lets ACDP throw an {@link
 * acdp.exceptions.IllegalReferenceException IllegalReferenceException}.
 * A reference is illegal if and only if one of the following two conditions
 * is satisfied:
 * 
 * <ul>
 * 	<li>The reference points to a row that does not exist within the
 *        table.</li>
 * 	<li>The reference points to a row gap.</li>
 * </ul>
 * <p>
 * Of course, an illegal reference is invalid but an invalid reference is not
 * necessarily illegal.
 * Therefore, ACDP may not complain if it has to deal with an invalid reference.
 * For example, consider the case where a reference gets invalid because another
 * client deletes the referenced row.
 * At that time the reference is not only invalid but illegal because the
 * reference points to a row gap.
 * Let's assume that later on ACDP reuses the file space of the row gap because
 * the same or another client inserts some new rows.
 * Since the row gap has disappeared the reference is no longer illegal but it
 * remains invalid.
 * <p>
 * Let {@code r1} and {@code r2} be two references pointing both to rows of a
 * particular table {@code T}.
 * Both references reference the same row of {@code T} if and only if {@code
 * r1.equals(r2)} returns {@code true}.
 * <p>
 * References can be put into hash based collections: If {@code r1.equals(r2)}
 * returns {@code true} then {@code r1.hashCode() == r2.hashCode()}.
 * <p>
 * Without a doubt, using references is the most efficient way to access
 * persistent data.
 * You are therefore encouraged to use references whenever possible.
 * Moreover, it may be worthwhile to think about changing the environment (for
 * example reducing the degree of concurrency when modifying tables) in order
 * to increase the lifetime of valid references.
 * <p>
 * There is no need for clients to implement this interface and to create
 * references on their own.
 * Deserializing a serialized reference is the only legal way to create a
 * reference outside of ACDP.
 *
 * @author Beat Hoermann
 */
public interface Ref extends Serializable {
	/**
	 * Returns a string representation of this reference.
	 * <p>
	 * A reference internally keeps an integer value greater than zero which is
	 * equal to the index of the referenced row within the array of rows of the
	 * referenced table.
	 * This method returns this index as a string.
	 * 
	 * @return The string representation of this reference, never {@code null}.
	 */
	@Override
	String toString();
	
	/**
	 * Compares the specified object with this reference for equality.
	 * 
	 * @param o Object to be compared for equality with this reference.
	 * 
	 * @return The boolean value {@code true} if and only if the specified
	 *         object is equal to this reference.
	 */
	@Override
	boolean equals(Object o);
	
	/**
	 * Returns the hash code value for this reference.
	 * 
	 * @return The hash code value for this reference.
	 */
	@Override
	int hashCode();
}
