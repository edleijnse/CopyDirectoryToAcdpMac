/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package example;

import acdp.Column;
import acdp.Ref;
import acdp.Row;
import acdp.design.CL;
import acdp.design.CustomTable;
import acdp.tools.Setup.Setup_Column;
import acdp.tools.Setup.Setup_Table;
import acdp.types.Type.Scheme;

import java.math.BigInteger;
import java.util.stream.Stream;

/**
 * The Image Table
 *
 * @author Ed Leijnse
 */
@Setup_Table({ "Directory", "File", "ID", "IptcKeywords" })
public final class ImageTable extends CustomTable {
	// Must be public if Setup is used.
	@Setup_Column("Directory")
	public final Column<String> DIRECTORY = CL.typeString();
	@Setup_Column("File")
	public final Column<String> FILE = CL.typeString();
	@Setup_Column("ID")
	public final Column<BigInteger> ID = CL.create(new BigIntegerType(false, 20));
	@Setup_Column("IptcKeywords")
	public final Column<String> IPTCKEYWORDS = CL.typeString();

	/**
	 * Do not create instances of this class!
	 * If you create an instance of this class then calling any inherited method
	 * will most likely fail.
	 * Creating instances of this class is exclusively reserved to the
	 * {@link PersonDB} class.
	 */
	public ImageTable() {
		initialize(DIRECTORY, FILE, ID, IPTCKEYWORDS);
	}
	@Override

	public final Stream<Row> rows(Column<?>... columns) {

		return super.rows(columns);

	}
}
