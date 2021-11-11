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

  public void runSampleScript() throws Exception {
    Statement statement = conn.createStatement();

    PreparedStatement insert = conn.prepareStatement("insert into testuniverse values (?, ?, 32.34, \'{1, 2, 3}\')");

    PreparedStatement insert2 = conn.prepareStatement("insert into testuniverse2 values (?, ?, 32.34, \'{1, 2, 3}\')");

    // create table testuniverse (a text primary key, b int, c numeric, d int[]);
    // create table testuniverse2 (a text primary key, b int, c numeric, d int[]);
    System.out.println("Deleting table rows now...");
    statement.execute("delete from testuniverse;");
    statement.execute("delete from testuniverse2;");

    long insertionCounter = 0;
    int numOfIterations = 100;
    int internalOps = 1000;
    for (long cnt = 1; cnt > -1; ++cnt) {
      ++insertionCounter;
      String insertString = "vaibhavInsert" + cnt;
      insert.setString(1, insertString);
      insert.setLong(2, cnt);
      insert2.setString(1, insertString);
      insert2.setLong(2, cnt);

      int ins1 = insert.executeUpdate();
      int ins2 = insert2.executeUpdate();

      if (!(ins1 == 1 && ins2 == 1)) {
        throw new RuntimeException("Insertion didnn't happen properly");
      }

      if (cnt % 1000 == 0) {
        System.out.println("Number of insertions so far: " + insertionCounter);
        Thread.sleep(700);
      }

      insert.clearParameters();
      insert2.clearParameters();
    }

    insert.close();
    insert2.close();
    statement.close();
  }

  public static void main(String[] args) {
    System.out.println("Starting CDC Load tester...");
    String[] connectionPoints = {"172.151.63.210:5433"};
    int ptrIdx = 0;
    while (true) {
      try {
        System.out.println("\nConnnecting via JDBC...\nConnection point = " + connectionPoints[ptrIdx] + "\n");
        String connectionString = String.format("jdbc:postgresql://%s/yugabyte", connectionPoints[ptrIdx]);

        conn = DriverManager.getConnection(connectionString, "yugabyte", "yugabyte");
        conn.setAutoCommit(true);
        if (!conn.isClosed()) {
          System.out.println("JDBC connection successful...");
        }

        App appObject = new App();
        appObject.runSampleScript();

      } catch (RuntimeException re) {
        System.out.println("Received runtime exception while inserting");
        re.printStackTrace();
      } catch (Exception e) {
        System.out.println("Exception raised while performing operations...");
        System.out.println(e);
        e.printStackTrace();
        System.out.println("Trying again...");
      }
    }
  }
}