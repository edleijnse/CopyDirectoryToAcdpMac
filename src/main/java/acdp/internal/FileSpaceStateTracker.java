/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal;

import java.util.HashSet;
import java.util.Set;

/**
 * The file space state tracker saves the pristine internal state of all file
 * spaces whose internal states are subject to changes during the execution
 * of one or several write operations.
 * (The pristine internal state of a file space is the internal state of that
 * file space at the time its internal state was last persisted.)
 * The file space state tracker helps to avoid that the current internal states
 * of the various file spaces have to be persisted each time the internal states
 * change.
 * Instead, the current internal states are persisted only at the time a commit
 * is executed in a top-level unit or at the end of a Kamikaze write.
 * Furthermore, if a roll back is executed in a unit the file space state
 * tracker is used to restore the pristine internal states without the need for
 * reading them from storage.
 * <p>
 * Consult the method descriptions and the usage of the file space state tracker
 * in the {@link Unit_} class and in all classes that implement the {@link
 * IFileSpace} interface to get a deeper insight into the details of how the
 * file space state tracker helps reducing the number of write operations.
 *
 * @author Beat Hoermann
 */
public final class FileSpaceStateTracker {
	
	/**
	 * The internal state of a file space.
	 * A concrete implementation of a file space has its own notion of "internal
	 * state".
	 * 
	 * @author Beat Hoermann
	 */
	public interface IFileSpaceState {
	}
	
	/**
	 * A concrete file space class implements this interface so that its
	 * internal state can be managed by the file space state tracker.
	 * 
	 * @author Beat Hoermann
	 */
	public interface IFileSpace {
		/**
		 * Writes the internal current state of the file space to the storage
		 * device.
		 * 
		 * @throws FileIOException If an I/O error occurs.
		 */
		void writeState() throws FileIOException;
		
		/**
		 * The file space replaces its internal state with the specified state.
		 * 
		 * @param state The new file space state.
		 */
		void adoptState(IFileSpaceState state);
	}
	
	/**
	 * Defines an element of a set of pristine internal file space states.
	 * Such an element consists of the pristine internal file space state and
	 * the file space that state belongs to.
	 * <p>
	 * Two {@code FSSElement} objects {@code o1} and {@code o2} are considered
	 * equal if and only if {@code o1 == o2}.
	 * Thus, adding an {@code FSSElement} object {@code o} to a set of
	 * {@code FSSElement} objects has no effect, if the set already contains
	 * an {@code FSSElement} object {@code e} such that {@code o == e}.
	 * 
	 * @author Beat Hoermann
	 */
	private static final class FSSElement {
		/**
		 * The file space.
		 */
		final IFileSpace fs;
		/**
		 * The pristine internal state of the file state.
		 */
		final IFileSpaceState state;
		
		/**
		 * The constructor.
		 * 
		 * @param fs The file space.
		 * @param state The pristine internal state of the file space.
		 */
		private FSSElement(IFileSpace fs, IFileSpaceState state) {
			this.fs = fs;
			this.state = state;
		}

		@Override
		public int hashCode() {
			return fs.hashCode();
		}
		
		@Override
		public final boolean equals(Object obj) {
			return obj instanceof FSSElement && ((FSSElement) obj).fs == fs;
		}
	}
	
	/**
	 * The internal state of the file space state tracker.
	 * 
	 * @author Beat Hoermann
	 */
	static final class FSSTrackerState {
		/**
		 * The pristine states of all file spaces that changed their internal
		 * state.
		 */
		private final Set<FSSElement> s;
		/**
		 * The pristine states of all file spaces that changed their internal
		 * state in a nested unit.
		 */
		private final Set<FSSElement> stash;
		
		/**
		 * The constructor.
		 * 
		 * @param s	The pristine states of all file spaces that changed their
		 *        internal state.
		 * @param stash The pristine states of all file spaces that changed their
		 *        internal state in a nested unit.
		 */
		private FSSTrackerState(Set<FSSElement> s, Set<FSSElement> stash) {
			this.s = s;
			this.stash = stash;
		}
	}
	
	/**
	 * The pristine states of all file spaces that changed their internal state.
	 */
	private Set<FSSElement> s = new HashSet<>();
	
	/**
	 * The pristine states of all file spaces that changed their internal state
	 * in a nested unit.
	 * (A commit executed in a nested unit has the effect of moving any pristine
	 * states collected so far to the stash.)
	 */
	private Set<FSSElement> stash = new HashSet<>();
	
	/**
	 * A file space is asked to report its old state to the file space state
	 * tracker by invoking this method whenever its state changes.
	 * 
	 * @param fs The file space.
	 * @param oldState The state of the file space before it was changed.
	 */
	public final void reportOldState(IFileSpace fs, IFileSpaceState oldState) {
		s.add(new FSSElement(fs, oldState));
	}
	
	/**
	 * Invokes the {@link IFileSpace#writeState} method for each file space
	 * reported via the {@link #reportOldState} method since the file space
	 * state tracker was created or since the {@link #clearPristineStates}
	 * method was most recently invoked.
	 * 
	 * @throws FileIOException If an I/O error occurs.
	 */
	public final void writeStates() throws FileIOException {
		for (FSSElement e : s) {
			e.fs.writeState();
		}
	}
	
	/**
	 * Invokes the {@link IFileSpace#adoptState adoptState} method for each
	 * file space reported via the {@link #reportOldState} method since the file
	 * space state tracker was created or since the {@link #clearPristineStates}
	 * method was most recently invoked.
	 * <p>
	 * This method is invoked if a rollback is executed within a top-level or
	 * nested unit in order to restore the pristine states of all file spaces
	 * that changed their state.
	 */
	final void adoptPristineStates() {
		for (FSSElement e : s) {
			e.fs.adoptState(e.state);
		}
	}
	
	/**
	 * Clears the set of pristine states of all file spaces that changed their
	 * state.
	 */
	final void clearPristineStates() {
		s.clear();
	}
	
	/**
	 * Resets the file space state tracker.
	 * This includes clearing the set of pristine states of all file spaces that
	 * changed their state.
	 * <p>
	 * This method is invoked when a nested unit is opened.
	 */
	final void reset() {
		s.clear();
		stash.clear();
	}
	
	/**
	 * Adds the elements of the set of pristine states of all file spaces that
	 * changed their state to the stash.
	 * The reverse operation is done as part of executing the {@link
	 * #digestState} method.
	 * <p>
	 * This method is invoked when a commit is executed in a nested unit.
	 * Any pristine states collected so far are moved to the stash.
	 */
	final void stashPristineStates() {
		stash.addAll(s);
	}
	
	/**
	 * Returns a <em>copy</em> of the internal state of this file space state
	 * tracker.
	 * <p>
	 * This method is invoked when a nested unit is opened.
	 * A copy of the current state of this file space tracker is pushed onto a
	 * stack.
	 * 
	 * @return A copy of the internal state of this file space state tracker.
	 */
	final FSSTrackerState getCopyOfState(){
		return new FSSTrackerState(new HashSet<>(s), new HashSet<>(stash));
	}
	
	/**
	 * This file space state tracker replaces its internal state with the
	 * specified state and adds all stashed states to the set of pristine states
	 * of all file spaces that changed their state.
	 * <p>
	 * This method is invoked when a nested unit is closed.
	 * The specified state is the saved state of the file space tracker at the
	 * time the nested unit was opened.
	 * (The state is popped from a stack.)
	 * Any pristine states collected in the nested unit, hence, any pristine
	 * states in the stash, are moved to the regular set of pristine states.
	 * 
	 * @param state The new state of this file space tracker.
	 */
	final void digestState(FSSTrackerState state) {
		s = state.s;
		s.addAll(stash);
		stash = state.stash;
	}
}
