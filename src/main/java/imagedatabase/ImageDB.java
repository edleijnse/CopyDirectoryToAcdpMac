package imagedatabase;

import acdp.design.CustomDatabase;
import acdp.tools.Setup.Setup_Database;
import acdp.tools.Setup.Setup_TableDeclaration;

import java.nio.file.Path;

/**
 * The image database.
 *
 * @author Ed Leijnse
 */
@Setup_Database(
	name = "ImageDB",
	version = "1.0",
	tables = { "Image" }
)
public final class ImageDB extends CustomDatabase {
	@Setup_TableDeclaration("Image")
	public final ImageTable imageTable = new ImageTable();

	public ImageDB(Path mainFile, int opMode, boolean writeProtect) {
		open(mainFile, opMode, writeProtect, 0, imageTable);
	}
}
