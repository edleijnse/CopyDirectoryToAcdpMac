package leijnse.info;

import java.io.*;

// Quelle: php http://www.computer-masters.de/cmd-kommandozeile-in-java-ausfuehren.php
class Command {

    private String basedir;

    public Command(String basedir) {
        this.basedir = basedir;
    }

    public void exec(String command) throws InterruptedException {
        System.out.println("executing command: " + command);
        Process p = null;
        try {
            p = Runtime.getRuntime().exec(
                    //"cmd /c " + //Nur unter Windows notwendig!
                    command, null, new File(basedir));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        System.out.println("OUTPUT");
        printStream(p.getInputStream());
        System.out.println("ERROR-OUTPUT start");
        printStream(p.getErrorStream());
        System.out.println("ERROR-OUTPUT end");
    }


    public void printStream(InputStream stream) {
        BufferedReader in = new BufferedReader(
                new InputStreamReader(stream));
        String line = "";
        try {
            while ((line = in.readLine()) != null) {
                System.out.println(line);
            }
            in.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }
}