/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.misc.array;

/**
 * A domino iterator is an ordinary iterator over the elements of a collection
 * with a <em>destructive behaviour</em>:
 * While returning the elements of the collection one after the other, the
 * domino iterator internally exposes in pieces the backing construction of the
 * collection to the garbage collector.
 * (With "exposing an object to the garbage collector" we mean setting any
 * references to this object to {@code null}.)
 * Thus, the memory of a potentially huge collection can be recycled before
 * the whole collection is exposed to the garbage collector.
 * However, not much is gained if references to the elements are kept outside
 * of the array since the amount of occupied memory space in the construction
 * is typically negligible compared to the amount of occupied memory space
 * required by the elements.
 * 
 * <p>
 * Due to the destructive behaviour of the domino iterator, any calls to any
 * method of the collection is discouraged as soon as the collection has "size"-method returning the number of elements in the collection.
 * A call to the "size"-method is discouraged only after the domino iterator
 * has returned the first element of the collection.
 * <p>
 * A domino iterator may reference parts of the backing construction even after
 * having returned the last element of the collection.
 * Therefore, it is good practice to set all references to a domino iterator
 * equal to {@code null} after having received the last element of the
 * collection.
 * <p>
 * The use of a domino iterator is recommended in cases where memory space is
 * an issue.
 * <p>
 * The name "domino iterator" is inspired by the domino effect: The row of
 * dominos is like a sequence of elements.
 * Similar to the falling of the dominos in a chain reaction from one end of
 * the row to the other, the sequence of elements gets destroyed from one end
 * to the other while the domino iterator returns one element after the other.
 *
 * @author Beat Hoermann
 */
public interface DominoIterator {
}
