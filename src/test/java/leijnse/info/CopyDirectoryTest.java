package leijnse.info;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

import static org.junit.Assert.*;

public class CopyDirectoryTest {
    String DIR_EMPTEE = "src/data/ImageDB";
    String DIR_RUN = "src/data/ImageDBRun";
    String LAYOUT_EMPTEE = "src/data/ImageDB/layout";
    String LAYOUT_RUN = "src/data/ImageDBRun/layout";

    CopyDirectory testee = new CopyDirectory();

    @Before
    public void setUp() throws Exception {
        AcdpAccessor acdpPrepare = new AcdpAccessor();
        acdpPrepare.copyLayout(DIR_EMPTEE, DIR_RUN);
    }

    @Test
    public void copyFilesToACDPTest() {
        Instant start = Instant.now();
        // testee.copyFilesToACDP("/media/psf/MyDrive01/BilderImport/Annalis/Bilder nachbearbeitet", "/media/psf/MyDrive01/BilderImport/Annalis/BilderExportBearbeitet3");
        testee.copyFilesToACDP("src/data/copyDirectoryTest", LAYOUT_RUN);
        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println("Duration (millisec): " + timeElapsed );

    }

    @Test
    public void addVisionTagsToFilesTest() throws IOException {
        Instant start = Instant.now();

        // String mySubscriptionKey = new String(Files.readAllBytes(Paths.get("/Users/edleijnse/OneDrive/Finanzen/Lizensen/Microsoft/keys/subscriptionKey1")));

        // testee.setSubscriptionKey(mySubscriptionKey);
        // testee.addVisionTagsToFiles("src/data/copyDirectoryTest/acdpTest/AesthetikDesZerfalls");
        // testee.addVisionTagsToFiles("/Volumes/MyDrive01/acdp/experiment", "/Volumes/MyDrive01/temp" + "");

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println("Duration (millisec): " + timeElapsed );

    }
    @Test
    public void escapeSpacesTest() {
        String myOutString = testee.escapeSpaces("/Volumes/MyDrive01/acdp/allAlbums/PICZ/Photowalk Escher-Wyssplatz/DU5A4635.jpg");
        System.out.println(myOutString);
    }
}