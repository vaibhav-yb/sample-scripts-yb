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

  public void runSampleScript(String uniquePhrase, long counter) throws Exception {
    Statement statement = conn.createStatement();

    PreparedStatement insert = conn.prepareStatement("insert into testuniverse values (?, ?, 32.34, \'{1, 2, 3}\')");

//    PreparedStatement insert2 = conn.prepareStatement("insert into testuniverse2 values (?, ?, 32.34, \'{1, 2, 3}\')");

    // create table testuniverse (a text primary key, b int, c numeric, d int[]);
    // create table testuniverse2 (a text primary key, b int, c numeric, d int[]);
    System.out.println("Deleting table rows now...");
    statement.execute("delete from testuniverse;");
//    statement.execute("delete from testuniverse2;");

    long insertionCounter = counter;
    int numOfIterations = 100;
    int internalOps = 1000;
    for (long cnt = 1; cnt > -1; ++cnt) {
      ++insertionCounter;
      String insertString = "vaibhavInsert" + uniquePhrase.toUpperCase() + System.currentTimeMillis();
      insert.setString(1, insertString);
      insert.setLong(2, cnt);
//      insert2.setString(1, insertString);
//      insert2.setLong(2, cnt);

      int ins1 = insert.executeUpdate();
//      int ins2 = insert2.executeUpdate();

      if (!(ins1 == 1)) {
        throw new RuntimeException("Insertion didnn't happen properly");
      }

      if (cnt % 1000 == 0) {
        System.out.println(Thread.currentThread().getName() + "Number of insertions so far: " + insertionCounter);
        Thread.sleep(1500);
      }

      insert.clearParameters();
//      insert2.clearParameters();
    }

    insert.close();
//    insert2.close();
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

        int count1 = 0, count2 = 0;
        String uw = "thread_One", uw2 = "thread_two";

        Thread t1 = new Thread(() -> {
          App appObject = new App();
          try {
            appObject.runSampleScript(uw, count1);
          } catch (Exception e) {
          }
        });

        Thread t2 = new Thread(() -> {
          App appObject = new App();
          try {
            appObject.runSampleScript(uw2, count2);
          } catch (Exception e) {
          }
        });

        t1.start();
        t2.start();

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