package leijnse.info;

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.Assert.*;

public class CopyDirectoryTest {
    CopyDirectory testee = new CopyDirectory();

    @Before
    public void setUp() throws Exception {
        AcdpAccessor acdpPrepare = new AcdpAccessor();
        acdpPrepare.copyLayout("src/data/acdpImage", "src/data/acdpRun");
    }

    @Test
    public void copyFilesToACDPTest() {
        Instant start = Instant.now();
        // testee.copyFilesToACDP("/media/psf/MyDrive01/BilderImport/Annalis/Bilder nachbearbeitet", "/media/psf/MyDrive01/BilderImport/Annalis/BilderExportBearbeitet3");
        testee.copyFilesToACDP("src/data/copyDirectoryTest", "src/data/acdpRun/layout");
        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println("Duration (millisec): " + timeElapsed );

    }
}