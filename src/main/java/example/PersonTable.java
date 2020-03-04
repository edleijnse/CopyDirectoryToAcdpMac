package example;

import java.math.BigInteger;

import static acdp.design.ST.Nulls.*;
import static acdp.design.ST.OutrowStringLength.*;
import static acdp.types.Type.Scheme.*;

import acdp.Column;
import acdp.Ref;
import acdp.design.CL;
import acdp.design.CustomTable;
import acdp.tools.Setup.Setup_Column;
import acdp.tools.Setup.Setup_Table;


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
	public final Column<String> NAME = CL.ofString();
	@Setup_Column("Vorname")
	public final Column<String> VORNAME = CL.ofString();
	@Setup_Column("ID")
	public final Column<BigInteger> ID = CL.create(new BigIntegerType(20));
	@Setup_Column("Hat_Kinder")
	public final Column<Boolean> HAT_KINDER = CL.ofBoolean(NO_NULL);
	@Setup_Column("Anzahl_Kinder")
	public final Column<Byte> ANZAHL_KINDER = CL.ofByte(NULLABLE);
	@Setup_Column(value = "Kinder", refdTable = "Person")
	public final Column<Ref[]> KINDER = CL.ofArrayOfRef(OUTROW, 20);
	@Setup_Column("Bemerkung")
	public final Column<String> BEMERKUNG = CL.ofString(NULLABLE, MEDIUM);
	
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
