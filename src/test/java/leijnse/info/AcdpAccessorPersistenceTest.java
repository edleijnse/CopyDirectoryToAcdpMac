package leijnse.info;

import org.junit.Before;
import org.junit.Test;

import javax.imageio.ImageReader;
import java.math.BigInteger;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class AcdpAccessorPersistenceTest {

    @Before
    public void setUp() throws Exception {
        AcdpAccessor testee = new AcdpAccessor();
        testee.copyLayout("src/data/acdpRunPersistence", "src/data/acdpRun");
    }

    @Test
    public void readImageTableColums() {
        AcdpAccessor testee = new AcdpAccessor();
        int anzColumns = testee.readImageTableColums("src/data/acdpRun/layout", "");
        assertEquals(4, anzColumns);
    }

    @Test
    public void readAllRowsFromImageTable() {
        AcdpAccessor testee = new AcdpAccessor();
        int anzRows = testee.readAllRowsFromImageTable("src/data/acdpRun/layout");
        assertEquals(5,anzRows);
    }
    @Test
    public void selectFromImageTableSomeKeywords() {
        AcdpAccessor testee = new AcdpAccessor();
        List<ImageRow>  rowsWithAllKeywords= testee.selectFromImageTable(false, "src/data/acdpRun/layout", "directory2","file1", BigInteger.valueOf(4),"building");
        assertEquals(4,rowsWithAllKeywords.size());
    }
}