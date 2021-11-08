package com.yugabyte;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalTime;
import java.util.Date;
import java.util.Properties;

public class App 
{
  public static Connection conn;

  public void runSampleScript(int ins, int upd, int del) throws Exception {
    Statement statement = conn.createStatement();

    PreparedStatement insert = conn.prepareStatement("insert into testuniverse values (?, ?, 32.34, \'{1, 2, 3}\')");
    PreparedStatement delete = conn.prepareStatement("delete from testuniverse where a = ?");
    PreparedStatement selectb = conn.prepareStatement("select b from testuniverse where a = ?");

    PreparedStatement insert2 = conn.prepareStatement("insert into testuniverse2 values (?, ?, 32.34, \'{1, 2, 3}\')");
    PreparedStatement delete2 = conn.prepareStatement("delete from testuniverse2 where a = ?");
    PreparedStatement selectb2 = conn.prepareStatement("select b from testuniverse2 where a = ?");

    Properties prop = new Properties();
    File file = new File("~/app.properties");
    if (file.createNewFile()) {
      System.out.println("File didn't exist, created successfully...");
    } else {
      System.out.println("Using values from the file...");
    }
    prop.load(new FileInputStream(file));

    ins = (prop.getProperty("inserts") == null) ? ins : Integer.parseInt(prop.getProperty("inserts"));
    upd = (prop.getProperty("updates") == null) ? upd : Integer.parseInt(prop.getProperty("updates"));
    del = (prop.getProperty("deletes") == null) ? del : Integer.parseInt(prop.getProperty("deletes"));

    System.out.println("Deleting table rows now...");
    statement.execute("delete from testuniverse;");

    long iterationCounter = 0;
    int numOfIterations = 100;
    int internalOps = 1000;
    for (int cnt = 0; cnt > -1; ++cnt) {
      ++iterationCounter;

      System.out.println("Starting row insert...");
      for (int i = 0; i < internalOps; ++i) {
        insert.setString(1, "vaibhav"+i);
        insert.setInt(2, i);
        int res = insert.executeUpdate();
        if (res != 1) {
          System.out.println(String.format("Error while inserting (%s, %d), exiting...", "vaibhav"+i, i));
          System.exit(0);
        }
        insert2.setString(1, "vaibhav"+i);
        insert2.setInt(2, i);
        int res2 = insert2.executeUpdate();
        if (res2 != 1) {
          System.out.println(String.format("Error while inserting (%s, %d), exiting...", "vaibhav"+i, i));
          System.exit(0);
        }

        System.out.println("Inserted row with a = " + "vaibhav"+i + ", b = " + (i) + ", c = 32.34 and d = {1, 2, 3}");
        ++ins;
        insert.clearParameters();
        insert2.clearParameters();
      }

      System.out.println("Starting row update now...");
      for (int i = 0; i < internalOps; ++i) {
        statement.execute("begin;");
        int res = statement.executeUpdate(String.format("update testuniverse set b = b + 1 where a = \'vaibhav"+i+"\';"));
        if (res == 1) {
          ++upd;
        } else {
          System.out.println("Some error occured while updating row...");
          System.exit(0);
        }
        statement.execute("commit;");

        statement.execute("begin;");
        int res = statement.executeUpdate(String.format("update testuniverse2 set b = b + 1 where a = \'vaibhav"+i+"\';"));
        if (res == 1) {
          ++upd;
        } else {
          System.out.println("Some error occured while updating row...");
          System.exit(0);
        }
        statement.execute("commit;");

        System.out.println("Row after update, a = " + "vaibhav"+i + " b = " + (i+1));
      }

      System.out.println("Starting row delete...");
      for (int i = 0; i < internalOps; ++i) {
        statement.execute("begin;");
        delete.setString(1, "vaibhav"+i);
        int res = delete.executeUpdate();

        if (res == 1) {
          ++del;
        } else {
          System.out.println(String.format("Error while deleting key %s, exiting...", "vaibhav"+i));
          System.exit(0);
        }

        statement.execute("commit;");

        statement.execute("begin;");
        delete2.setString(1, "vaibhav"+i);
        int res2 = delete2.executeUpdate();
        if (res2 == 1) {
          ++del;
        } else {
          System.out.println(String.format("Error while deleting key in testuniverse2 %s, exiting...", "vaibhav"+i));
          System.exit(0);
        }
        statement.execute("commit;");

        System.out.println("Deleted row with a = " + "vaibhav"+i);
        delete.clearParameters();
        delete2.clearParameters();
      }

      ResultSet rs = statement.executeQuery("select * from testuniverse;");
      if (rs.next()) {
        System.out.println("Row val, a = " + rs.getInt(2));
        System.out.println("Not all the rows are deleted, exiting...");
        System.exit(0);
      }

      ResultSet rs2 = statement.executeQuery("select * from testuniverse2;");
      if (rs2.next()) {
        System.out.println("Row val, a = " + rs.getInt(2));
        System.out.println("Not all the rows are deleted, exiting...");
        System.exit(0);
      }

      if (true) {
        System.out.println("Starting another iteration, take a look at op count...");
        System.out.println(String.format("Inserts = %d\t Updates = %d\t Deletes = %d", ins, upd, del));
        System.out.println("Iteration count: " + iterationCounter);
        Thread.sleep(3000);
      }
    }

    insert.close();
    delete.close();
    selectb.close();
    insert2.close();
    delete2.close();
    selectb2.close();
    conn.close();
  }

  public static void main(String[] args) {
    System.out.println("Starting CDC Load tester...");
    String[] connectionPoints = {"127.0.0.1:5433"};
    int ptrIdx = 0;
    int i = 0, u = 0, d = 0;
    while (true) {
      try {
        System.out.println("\nTrying to connect via JDBC...\nConnection point = " + connectionPoints[ptrIdx] + "\n");
        String connectionString = String.format("jdbc:postgresql://%s/yugabyte", connectionPoints[ptrIdx]);

        conn = DriverManager.getConnection(connectionString, "yugabyte", "yugabyte");
        conn.setAutoCommit(true);

        App appObject = new App();
        appObject.runSampleScript(i, u, d);

      } catch (Exception e) {
        System.out.println("Exception raised while performing operations...");
        File file = new File("~/app.properties");
        try {
          if (file.createNewFile()) {
            System.out.println("Creating a new file to store the count...");
          }
          Properties prop = new Properties();
          prop.setProperty("inserts", String.valueOf(i));
          prop.setProperty("updates", String.valueOf(u));
          prop.setProperty("deletes", String.valueOf(d));

          prop.store(new FileOutputStream(file), "Storing the opCount at " + LocalTime.now());
        } catch (IOException io) {
          System.out.println("IO Exception thrown");
        }

        ++ptrIdx;
        if (ptrIdx >= 1) {
          ptrIdx = 0;
        }
        System.out.println("Trying again with next connection point = " + connectionPoints[ptrIdx]);
        System.out.println(e);
        e.printStackTrace();
      }
    }
  }
}