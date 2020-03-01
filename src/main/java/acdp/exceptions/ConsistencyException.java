/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.exceptions;

import java.nio.file.Path;

import acdp.design.CustomTable;
import acdp.internal.Database_;

/**
 * Thrown if the value of the consistency number given at the time a strongly
 * typed database is {@linkplain acdp.design.CustomDatabase#open(Path, int,
 * boolean, int, CustomTable...) opened} differs from the value of the
 * consistency number stored in the database layout.
 * Signals that the logic of a strongly typed database is inconsistent with the
 * data stored in that database.
 *
 * @author Beat Hoermann
 */
public final class ConsistencyException extends ACDPException {
	private static final long serialVersionUID = 2563395674421102623L;

	/**
    * The constructor.
    * 
    * @param db The database.
    * @param logicCN The consistency number of the logic.
    * @param dataCN The consistency number of the data.
    */
	public ConsistencyException(Database_ db, int logicCN, int dataCN) {
		super(db, "The logic of the database is inconsistent with its data. " +
										"Consistency number of logic: " + logicCN +
										", consistency number of data: " + dataCN + ".");
	}
}
