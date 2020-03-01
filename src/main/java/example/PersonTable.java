/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package example;

import java.math.BigInteger;

import acdp.Column;
import acdp.Ref;
import acdp.design.CL;
import acdp.design.CustomTable;
import acdp.tools.Setup.Setup_Column;
import acdp.tools.Setup.Setup_Table;
import acdp.types.Type.Scheme;

/**
 * The person table.
 *
 * @author Beat Hoermann
 */
@Setup_Table({ "Name", "Vorname", "ID", "Hat_Kinder", "Anzahl_Kinder", "Kinder",
					"Bemerkung" })
public final class PersonTable extends CustomTable {
	// Must be public if Setup is used.
	@Setup_Column("Name")
	public final Column<String> NAME = CL.typeString();
	@Setup_Column("Vorname")
	public final Column<String> VORNAME = CL.typeString();
	@Setup_Column("ID")
	public final Column<BigInteger> ID = CL.create(new BigIntegerType(
																						false, 20));
	@Setup_Column("Hat_Kinder")
	public final Column<Boolean> HAT_KINDER = CL.typeBoolean(false);
	@Setup_Column("Anzahl_Kinder")
	public final Column<Byte> ANZAHL_KINDER = CL.typeByte(true);
	@Setup_Column(value = "Kinder", refdTable = "Person")
	public final Column<Ref[]> KINDER = CL.typeArrayOfRef(Scheme.OUTROW, 20);
	@Setup_Column("Bemerkung")
	public final Column<String> BEMERKUNG = CL.typeString(true, 2);
	
	/**
	 * Do not create instances of this class!
	 * If you create an instance of this class then calling any inherited method
	 * will most likely fail.
	 * Creating instances of this class is exclusively reserved to the
	 * {@link PersonDB} class.
	 */
	public PersonTable() {
		initialize(NAME, VORNAME, ID, HAT_KINDER, ANZAHL_KINDER, KINDER,
																							BEMERKUNG);
	}
}
