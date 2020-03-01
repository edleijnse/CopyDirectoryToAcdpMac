package leijnse.info;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CopyDirectory {
    public void copyFilesToDirectory(String startsWithDirectory, String copyDirectory) {
        try {
            // https://www.codejava.net/java-core/concurrency/java-concurrency-understanding-thread-pool-and-executors
            // Better: completable Future https://www.deadcoderising.com/java8-writing-asynchronous-code-with-completablefuture/


            System.out.println("handleDirectoryCopyFileToDatabase start: " + startsWithDirectory) ;
            Files.walk(Paths.get(startsWithDirectory))
                    .filter(p -> {
                        return ((p.toString().toLowerCase().endsWith(".cr2")) || (p.toString().toLowerCase().endsWith(".cr3")) || (p.toString().toLowerCase().endsWith(".jpg")));
                    })
                    .forEach(item -> {
                        File file = item.toFile();
                        if (file.isFile()) {
                            String sourceFile = "";
                            sourceFile = file.getAbsolutePath();
                            String destFile = copyDirectory + "/" + file.getName();
                            try {
                                copyFile(sourceFile,destFile);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    });
            System.out.println("handleDirectoryCopyFile end");

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("handleDirectoryCopyFile completed");
    }
    public void copyFilesToACDP(String startsWithDirectory, String layOut) {
        try {
            // https://www.codejava.net/java-core/concurrency/java-concurrency-understanding-thread-pool-and-executors
            // Better: completable Future https://www.deadcoderising.com/java8-writing-asynchronous-code-with-completablefuture/


            System.out.println("copyFilesToACDP start: " + startsWithDirectory) ;
            AcdpAccessor acdpAccessor = new AcdpAccessor();
            final int[] ii = {0};
            Files.walk(Paths.get(startsWithDirectory))
                    .filter(p -> {
                        return ((p.toString().toLowerCase().endsWith(".cr2")) ||
                                (p.toString().toLowerCase().endsWith(".cr3")) ||
                                (p.toString().toLowerCase().endsWith(".jpg_original")) ||
                                (p.toString().toLowerCase().endsWith(".jpg")));
                    })
                    .forEach(item -> {
                        File file = item.toFile();
                        if (file.isFile()) {
                            PictureMetaData pictureMetaData = new PictureMetaData();
                            ExtractPictureMetaData extractPictureMetaData = new ExtractPictureMetaData();
                            try {
                                pictureMetaData = extractPictureMetaData.getPictureMetaDataExif(file);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            String myIptcKeywords = "";
                            if (pictureMetaData.getIPTC_KEYWORDS().isPresent()){
                                myIptcKeywords = pictureMetaData.getIPTC_KEYWORDS().get();
                            }

                            String sourceFileAbsolutePath = "";
                            sourceFileAbsolutePath = file.getAbsolutePath();
                            String sourceFileName = file.getName();
                            String sourceParentName = file.getParent();
                            try {
                                ii[0]++;
                                System.out.println("absolute: " + sourceFileAbsolutePath + ", parent: " + sourceParentName  +", filename: " + sourceFileName);
                                acdpAccessor.writeRowToImageTable(layOut,sourceParentName,sourceFileName, BigInteger.valueOf(ii[0]),myIptcKeywords);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });
            System.out.println("handleDirectoryCopyFile end");

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("handleDirectoryCopyFile completed");
    }
    public void copyFilesDirectoryNameToACDP(String startsWithDirectory, String layOut) {
        try {
            // https://www.codejava.net/java-core/concurrency/java-concurrency-understanding-thread-pool-and-executors
            // Better: completable Future https://www.deadcoderising.com/java8-writing-asynchronous-code-with-completablefuture/


            System.out.println("copyFilesDirectoryNameToACDP start: " + startsWithDirectory) ;
            AcdpAccessor acdpAccessor = new AcdpAccessor();
            final int[] ii = {0};
            Files.walk(Paths.get(startsWithDirectory))
                    .filter(p -> {
                        return ((p.toString().toLowerCase().endsWith(".cr2")) ||
                                (p.toString().toLowerCase().endsWith(".cr3")) ||
                                (p.toString().toLowerCase().endsWith(".jpg_original")) ||
                                (p.toString().toLowerCase().endsWith(".jpg")));
                    })
                    .forEach(item -> {
                        File file = item.toFile();
                        if (file.isFile()) {
                            PictureMetaData pictureMetaData = new PictureMetaData();
                            ExtractPictureMetaData extractPictureMetaData = new ExtractPictureMetaData();
                           /* try {
                                pictureMetaData = extractPictureMetaData.getPictureMetaDataExif(file);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }*/


                            String sourceFileAbsolutePath = "";
                            sourceFileAbsolutePath = file.getAbsolutePath();
                            String sourceFileName = file.getName();
                            String sourceParentName = file.getParent();
                            String sourceDirectoryName = "";
                            int lastIndex = sourceParentName.lastIndexOf("/");
                            if (lastIndex < 0){
                                lastIndex = sourceParentName.lastIndexOf("\\");
                            }
                            sourceDirectoryName = sourceParentName.substring(lastIndex+1);
                            // System.out.println("Parentname: "+ sourceParentName);
                            System.out.println(sourceParentName);
                            // System.out.println("sourceDirectoryName: "+ sourceDirectoryName);
                            String myIptcKeyWords = sourceDirectoryName.replaceAll(" ", ".");
                            System.out.println("myIpctKeyWords: " + myIptcKeyWords);
                            try {
                                ii[0]++;
                                System.out.println("Keywords: "+ myIptcKeyWords + ", absolute: " + sourceFileAbsolutePath + ", parent: " + sourceParentName  +", filename: " + sourceFileName);
                                acdpAccessor.writeRowToImageTable(layOut,sourceParentName,sourceFileName, BigInteger.valueOf(ii[0]),myIptcKeyWords);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });
            System.out.println("handleDirectoryCopyFile end");

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("handleDirectoryCopyFile completed");
    }
    /**
     * Java 7 way to copy a file from one location to another
     * @param from
     * @param to
     * @throws IOException
     */
    public static void copyFile(String from, String to) throws IOException{
        Path src = Paths.get(from);
        Path dest = Paths.get(to);
        try {
            Files.copy(src, dest);
        } catch (FileAlreadyExistsException ex){
            // do nothing
            System.out.println("Datei bereits vorhanden: " +dest.toString());
        }
    }

}
