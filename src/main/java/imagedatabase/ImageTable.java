package imagedatabase;

import acdp.Column;
import acdp.Ref;
import acdp.Row;
import acdp.design.CL;
import acdp.design.CustomTable;
import acdp.design.ST;
import acdp.exceptions.*;
import acdp.tools.Setup.Setup_Column;
import acdp.tools.Setup.Setup_Table;

import java.util.stream.Stream;

import static acdp.design.ST.Nulls.NO_NULL;
import static acdp.design.ST.OutrowStringLength.MEDIUM;
import static acdp.design.ST.OutrowStringLength.SMALL;

/**
 * The image Table
 *
 * @author Ed Leijnse
 */
@Setup_Table({ "Directory", "File", "IptcKeywords", "Image" })
public final class ImageTable extends CustomTable {
	@Setup_Column("Directory")
	public final Column<String> DIRECTORY = CL.ofString(NO_NULL, MEDIUM);
	@Setup_Column("File")
	public final Column<String> FILE = CL.ofString(NO_NULL, SMALL);
	@Setup_Column("IptcKeywords")
	public final Column<String[]> IPTCKEYWORDS = CL.ofArray(100, ST.beString(
																					NO_NULL, SMALL));
	@Setup_Column("Image")
	public final Column<byte[]> IMAGE = CL.create(new ImageType());

	public ImageTable() {
		initialize(DIRECTORY, FILE, IPTCKEYWORDS, IMAGE);
	}

	@Override
	public final void compactVL() throws UnsupportedOperationException,
            ACDPException, ShutdownException, IOFailureException {
		super.compactVL();
	}

	@Override
	public final Ref insert(Object... values) throws
						UnsupportedOperationException, NullPointerException,
						IllegalArgumentException, MaximumException, CryptoException,
            ShutdownException, ACDPException, UnitBrokenException,
            IOFailureException {
		return super.insert(values);
	}
	
	@Override
	public final void delete(Ref ref) throws UnsupportedOperationException,
						NullPointerException, DeleteConstraintException,
            IllegalReferenceException, ShutdownException, ACDPException,
            UnitBrokenException, IOFailureException {
		super.delete(ref);
	}

	@Override
	public final Stream<Row> rows(Column<?>... columns) {
		return super.rows(columns);

	}
}
