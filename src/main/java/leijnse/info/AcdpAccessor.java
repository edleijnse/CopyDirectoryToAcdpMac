package leijnse.info;

import imagedatabase.ImageDB;
import imagedatabase.ImageTable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;


public class AcdpAccessor {
    public int readImageTableColums(String myLayout, String copyDirectory) {
        Path myPath = Paths.get(myLayout);
        AtomicInteger anzRows = new AtomicInteger();
        anzRows.set(Integer.valueOf(0));

        try (ImageDB db = new ImageDB(Paths.get(myLayout), -1, true)) {
            ImageTable myTable = db.imageTable;
            // System.out.println("Number of columns: " + myTable.getColumns().length);
            myTable.rows().findFirst().ifPresent(row -> {
                anzRows.set(row.getColumns().length);
            });
            System.out.println("Number of rows: " + myTable.rows().count());
            //do something with myTable
        }
        return anzRows.get();
    }


    public void writeRowToImageTable(String myLayout, String myDirectory, String myFile, String[] myIPTCKeywords, byte[] myImage) {
        Path myPath = Paths.get(myLayout);

        System.out.println("myIPTCKeywords: ");
        for (String kw: myIPTCKeywords){
            System.out.println ("kw: " + kw);
        }
        // Insert an image into the database.
        try (ImageDB db = new ImageDB(Paths.get(myLayout), -1, false)) {
            db.imageTable.insert(myDirectory, myFile, myIPTCKeywords, myImage);
        }
    }

    public int readAllRowsFromImageTable(String myLayout) {
        Path myPath = Paths.get(myLayout);
        int anzahlRows = 0;

        try (ImageDB db = new ImageDB(Paths.get(myLayout), -1, false)) {

            ImageTable myTable = db.imageTable;
            anzahlRows = (int) myTable.rows(myTable.DIRECTORY, myTable.FILE, myTable.IPTCKEYWORDS, myTable.IMAGE).count();

            return anzahlRows;
        }
    }

    /*
    VORBEREITUNG: (Email von Beat Hörmann an Ed Leijnsse, 2020-02-19)
    Du liebst offenbar die "Stream Pipeline"! Ich habe deshalb diese Methode noch stärker
    im Sinne von "lambda expressions", "method references" und "streams" geschrieben.
    Mir geht es aber vor allem um ACDP.
    Ich habe deshalb in meiner Version der selectFromImageTable-Methode mit Hilfe der
    Methode Table.rows(Column<?>... columns) direkt einen stream of rows Stream<Row> generiert.
    Zweitens habe ich die DB nicht "weakly typed" geöffnet,
    sondern "strongly typed", ich habe also nicht die Interfaces Database und Table verwendet,
    wie Du, sondern Deine "custom database class" example.ImageDB
    und Deine "custom table class" example.ImageTable.
    Dazu musste ich den ImageDB-Konstruktor public machen und in der
    ImageTable-Klasse die rows-Methode veröffentlichen:

    @Override
    public final Stream<Row> rows(Column<?>... columns) {
    return super.rows(columns);
}
     */
    public List<ImageRow> selectFromImageTable(boolean allKeywords, String myLayout,

                                               String myDirectory, String myFile,

                                               String myIptcKeywords) {

        final List<String> myKeywords = Arrays.stream(myIptcKeywords.split(",")).
                map(String::trim).
                filter(s -> !s.isEmpty()).
                collect(Collectors.toList());


         try (ImageDB db = new ImageDB(Paths.get(myLayout), -1, false)) {

            final ImageTable t = db.imageTable;

            return t.rows(t.DIRECTORY, t.FILE, t.IPTCKEYWORDS, t.IMAGE).filter(row -> {
                if (row.get(t.DIRECTORY).equals(myDirectory) ||
                        row.get(t.FILE).equals(myFile)
                         )
                    return true;

                else if (myKeywords.isEmpty())
                    return false;
                else {
                    final Set<String> kwSet = new HashSet<>();

                    System.out.println("IPTTCKEYWORDS:" + row.get(t.IPTCKEYWORDS).toString());
                    for (String kw : row.get(t.IPTCKEYWORDS)) {
                        if (kw != null && !kw.isEmpty()) {
                            kwSet.add(kw);
                            System.out.println("kw:" + kw);
                        }
                    }

                    if (kwSet.isEmpty()) {
                        return false;
                    }


// kwSet is not empty and does not contain null nor an empty string.
                    boolean oneMatch = false, allMatch = true;

                    for (String kw : myKeywords) {
                        final boolean match = kwSet.contains(kw);
                        oneMatch |= match;
                        allMatch &= match;
                    }
                    return allKeywords && allMatch || !allKeywords && oneMatch;
                }
            }).collect(ArrayList::new, (list, row) -> {
                ImageRow myImageRow = new ImageRow();
                myImageRow.setDirectory(row.get(t.DIRECTORY));
                myImageRow.setFile(row.get(t.FILE));
                myImageRow.setIptcKeywords(row.get(t.IPTCKEYWORDS));
                myImageRow.setImage(row.get(t.IMAGE));

                list.add(myImageRow);
            }, ArrayList::addAll);
        }
    }


    public void copyLayout(String fromLayout, String toLayout) throws IOException {
        Path sourcePath = Paths.get(fromLayout);
        Path destinationPath = Paths.get(toLayout);
        File directoryToPurge = new File(toLayout);
        purgeDirectory(directoryToPurge);
        copyFolder(sourcePath, destinationPath);


    }

    public void copyFolder(Path src, Path dest) throws IOException {
        Files.walk(src)
                .forEach(source -> copy(source, dest.resolve(src.relativize(source))));
    }

    public void copy(Path source, Path dest) {
        try {
            Files.copy(source, dest, REPLACE_EXISTING);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void purgeDirectory(File dir) {
        try {
            for (File file : dir.listFiles()) {
                if (file.isDirectory())
                    purgeDirectory(file);
                file.delete();

            }
        } catch (Exception e) {
        }
    }

    public static <T> Stream<T>
    convertIterator(Iterator<T> iterator) {
        return StreamSupport.stream(((Iterable) () -> iterator).spliterator(), false);
    }

}
