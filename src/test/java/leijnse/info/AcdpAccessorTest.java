package leijnse.info;

import org.junit.Before;
import org.junit.Test;


import java.math.BigInteger;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class AcdpAccessorTest {

    @Before
    public void setUp() throws Exception {
        AcdpAccessor testee = new AcdpAccessor();
        testee.copyLayout("src/data/acdpImage", "src/data/acdpRun");
    }

    @Test
    public void readImageTableColums() {
        AcdpAccessor testee = new AcdpAccessor();
        int anzColumns = testee.readImageTableColums("src/data/acdpRun/layout", "");
        assertEquals(4, anzColumns);
    }

    @Test
    public void writeRowsToImageTableAndreadAllRowsFromImageTable() {
        AcdpAccessor testee = new AcdpAccessor();
        testee.writeRowToImageTable("src/data/acdpRun/layout", "directory1","file1", BigInteger.valueOf(1),"");
        testee.writeRowToImageTable("src/data/acdpRun/layout", "directory2","file2", BigInteger.valueOf(2), "blue.building");
        testee.writeRowToImageTable("src/data/acdpRun/layout", "directory3","file3", BigInteger.valueOf(3),"green.apple");
        testee.writeRowToImageTable("src/data/acdpRun/layout", "directory1","file1", BigInteger.valueOf(4), "white.building.red.apple");
        testee.writeRowToImageTable("src/data/acdpRun/layout", "directory2","file2", BigInteger.valueOf(5),"");
        testee.writeRowToImageTable("src/data/acdpRun/layout", "directory3","file3", BigInteger.valueOf(6),"");

        int anzRows = testee.readAllRowsFromImageTable("src/data/acdpRun/layout");
        assertEquals(6,anzRows);
    }
    @Test
    public void writeRowsToImageTableAndreadSomeRowsFromImageTable() {
        AcdpAccessor testee = new AcdpAccessor();
        testee.writeRowToImageTable("src/data/acdpRun/layout", "directory1","file1", BigInteger.valueOf(1),"");
        testee.writeRowToImageTable("src/data/acdpRun/layout", "directory2","file2", BigInteger.valueOf(2),"blue.building");
        testee.writeRowToImageTable("src/data/acdpRun/layout", "directory3","file1", BigInteger.valueOf(3),"green.apple");
        testee.writeRowToImageTable("src/data/acdpRun/layout", "directory3","file2", BigInteger.valueOf(4),"greem.leaves");
        testee.writeRowToImageTable("src/data/acdpRun/layout", "directory4","file4", BigInteger.valueOf(5),"");

        List<ImageRow>  rowsWithAllKeywords = testee.selectFromImageTable(true,"src/data/acdpRun/layout", "directory2","file1", BigInteger.valueOf(4),"building");
        assertEquals(4,rowsWithAllKeywords.size());
    }
}