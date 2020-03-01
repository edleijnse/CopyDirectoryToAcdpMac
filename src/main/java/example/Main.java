/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package example;

import acdp.Database;
import acdp.Table;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Beat Hoermann
 */
public final class Main {

    public static void main(String[] args) {
        // try (PersonDB db = new PersonDB(Paths.get(
        //							"pathToPersonDatabaseLayoutFile"), -1, false, 0)) {
        //
     /*   try (PersonDB db = new PersonDB(Paths.get(
                "/home/parallels/IdeaProjects/acdpTest/layout"), -1, false, 0)) {
        }*/
        Path myPath = Paths.get("/home/parallels/IdeaProjects/acdpTest/layout");

        try (Database db = Database.open(myPath, 0,false)) {
            Table myTable = db.getTable("Person");
            System.out.println("Number of columns: " + myTable.getColumns().length);
            System.out.println("Number of rows: " + myTable.numberOfRows());
            //do something with myTable

        }
    }

}
