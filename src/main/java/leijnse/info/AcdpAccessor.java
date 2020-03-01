package leijnse.info;

import acdp.*;
import example.ImageDB;
import example.ImageTable;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardCopyOption.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.*;
import java.util.*;


public class AcdpAccessor {
    public int readImageTableColums(String myLayout, String copyDirectory) {
        Path myPath = Paths.get(myLayout);
        int anzColumns = 0;

        try (Database db = Database.open(myPath, -1, false)) {
            Table myTable = db.getTable("Image");
            // System.out.println("Number of columns: " + myTable.getColumns().length);
            anzColumns = myTable.getColumns().length;
            // System.out.println("Number of rows: " + myTable.numberOfRows());
            //do something with myTable
        }
        return anzColumns;
    }

    public void writeRowToImageTable(String myLayout, String myDirectory, String myFile, BigInteger myId, String myIPTCKeywords) {
        Path myPath = Paths.get(myLayout);

        try (Database db = Database.open(myPath, -1, false)) {
            Table myTable = db.getTable("Image");
            myTable.insert(myDirectory, myFile, myId, myIPTCKeywords);
            // System.out.println("Number of columns: " + myTable.getColumns().length);
            // System.out.println("Number of rows: " + myTable.numberOfRows());
            db.forceWrite();
            db.close();
            //do something with myTable
        }
    }

    public int readAllRowsFromImageTable(String myLayout) {
        Path myPath = Paths.get(myLayout);
        int anzahlRows = 0;

        try (Database db = Database.open(myPath, -1, false)) {
            Table myTable = db.getTable("Image");
            //  myTable.iterator().next();
            Column<?> myDirectoryColumn = myTable.getColumn("Directory");
            Column<?> myFileColumn = myTable.getColumn("File");
            Column<?> myID = myTable.getColumn("ID");
            Column<?> myIpctKeywordsColumn = myTable.getColumn("IptcKeywords");


            Table.TableIterator myIterator = myTable.iterator(myDirectoryColumn, myFileColumn, myID, myIpctKeywordsColumn);
            while (myIterator.hasNext()) {
                anzahlRows++;
                System.out.println("Row " + anzahlRows);
                Row myRow = myIterator.next();
                String fieldDirectory = (String) myRow.get(myDirectoryColumn);
                String fieldFile = (String) myRow.get(myFileColumn);
                BigInteger fieldID = (BigInteger) myRow.get(myID);
                String fiedIptcKeywords = (String) myRow.get(myIpctKeywordsColumn);
                System.out.println("field Directory: " + fieldDirectory);
                System.out.println("field File: " + fieldFile);
                System.out.println("field ID: " + fieldID);
                System.out.println("field IptcKeywords: " + fiedIptcKeywords);

            }

            //do something with myTable
        }
        return anzahlRows;
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

                                               String myDirectory, String myFile, BigInteger myId,

                                               String myIptcKeywords) {

        final List<String> myKeywords = Arrays.stream(myIptcKeywords.split(",")).
                map(String::trim).
                filter(s -> !s.isEmpty()).
                collect(Collectors.toList());


        try (ImageDB db = new ImageDB(Paths.get(myLayout), -1, false, 0)) {

            final ImageTable t = db.imageTable;

            return t.rows(t.DIRECTORY, t.FILE, t.ID, t.IPTCKEYWORDS).filter(row -> {
                if (row.get(t.DIRECTORY).equals(myDirectory) ||
                        row.get(t.FILE).equals(myFile) ||
                        row.get(t.ID).compareTo(myId) == 0)
                    return true;

                else if (myKeywords.isEmpty())
                    return false;
                else {
                    final Set<String> kwSet = new HashSet<>();

                    for (String kw : row.get(t.IPTCKEYWORDS).split("\\.")) {
                        if (kw != null && !kw.isEmpty()) {
                            kwSet.add(kw);
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
                myImageRow.setId(row.get(t.ID).toString());
                myImageRow.setIptcKeywords(row.get(t.IPTCKEYWORDS));

                list.add(myImageRow);
            }, ArrayList::addAll);
        }
    }

    public List<ImageRow> selectFromImageTableOriginalEd(boolean allKeywords, String myLayout, String myDirectory, String myFile, BigInteger myId, String myIptcKeywords) {
        Path myPath = Paths.get(myLayout);
        List<ImageRow> myImageRows = new ArrayList();

        try (Database db = Database.open(myPath, -1, false)) {
            Table myTable = db.getTable("Image");
            //  myTable.iterator().next();
            Column<?> myDirectoryColumn = myTable.getColumn("Directory");
            Column<?> myFileColumn = myTable.getColumn("File");
            Column<?> myID = myTable.getColumn("ID");
            Column<?> myIpctKeywordsColumn = myTable.getColumn("IptcKeywords");

            Table.TableIterator myIterator = myTable.iterator(myDirectoryColumn, myFileColumn, myID, myIpctKeywordsColumn);

            // see sample https://www.tutorialspoint.com/convert-an-iterator-to-stream-in-java

            Stream<Row> myStream = convertIterator(myIterator);
            try (Stream<Row> rowStream = myStream.filter(myRow -> {
                        String fieldDirectory = (String) myRow.get(myDirectoryColumn);
                        String fieldFile = (String) myRow.get(myFileColumn);
                        BigInteger fieldID = (BigInteger) myRow.get(myID);
                        String fieldIpctKeywords = (String) myRow.get(myIpctKeywordsColumn);

                        if (fieldDirectory.contentEquals(myDirectory)) {
                            return true;
                        }
                        if (fieldFile.contentEquals(myFile)) {
                            return true;
                        }
                        int comparevalue = fieldID.compareTo(myId);
                        if (comparevalue == 0) {
                            return true;
                        }
                        ArrayList<String> ipctKeywordsArray = new ArrayList<>(
                                Arrays.asList(myIptcKeywords.split(",")));
                        AtomicBoolean gefunden = new AtomicBoolean(false);
                        AtomicBoolean nichtGefunden = new AtomicBoolean(false);
                        ipctKeywordsArray.forEach(ipctItem -> {
                            if (fieldIpctKeywords.contains(ipctItem.trim())) {
                                if (ipctItem.isEmpty() == false) {
                                    gefunden.set(true);
                                }
                            } else {
                                nichtGefunden.set(true);
                            }
                        });
                        if (allKeywords) {
                            if ((gefunden.get() == true) && (nichtGefunden.get() == false)) {
                                return true;
                            }
                        } else {
                            if (gefunden.get() == true) {
                                return true;
                            }
                        }

                        return false;
                    }
            )) {
                rowStream.forEach(myRow -> {
                    ImageRow myImageRow = new ImageRow();
                    String fieldDirectory = (String) myRow.get(myDirectoryColumn);
                    String fieldFile = (String) myRow.get(myFileColumn);
                    BigInteger fieldID = (BigInteger) myRow.get(myID);
                    String fieldIpctKeywords = (String) myRow.get(myIpctKeywordsColumn);
                    myImageRow.setDirectory(fieldDirectory);
                    myImageRow.setFile(fieldFile);
                    myImageRow.setId(fieldID.toString());
                    myImageRow.setIptcKeywords(fieldIpctKeywords);
                    myImageRows.add(myImageRow);
                });
            }
        }

        return myImageRows;
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

    private void copy(Path source, Path dest) {
        try {
            Files.copy(source, dest, REPLACE_EXISTING);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void purgeDirectory(File dir) {
        for (File file : dir.listFiles()) {
            if (file.isDirectory())
                purgeDirectory(file);
            file.delete();
        }
    }

    public static <T> Stream<T>
    convertIterator(Iterator<T> iterator) {
        return StreamSupport.stream(((Iterable) () -> iterator).spliterator(), false);
    }

}
