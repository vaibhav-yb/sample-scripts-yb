package com.yugabyte;

import org.apache.commons.dbcp2.BasicDataSource;

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
import java.util.Scanner;

public class App 
{
  public static Connection conn;

  private static BasicDataSource dataSource = null;
  private static String[] connectionPoints = {"172.151.56.111:5433"};

  static {
    dataSource = new BasicDataSource();
    String connectionString = String.format("jdbc:postgresql://%s/yugabyte", connectionPoints[0]);
    dataSource.setUrl(connectionString);
    dataSource.setUsername("yugabyte");
    dataSource.setPassword("yugabyte");
    dataSource.setMinIdle(5);
    dataSource.setMaxIdle(7);
    dataSource.setMaxTotal(15);
  }

  public void runSampleScript(int iVal) throws Exception {
    Statement statement = conn.createStatement();

    PreparedStatement insert = conn.prepareStatement("insert into testuniverse values (?, ?, 32.34, \'{1, 2, 3}\')");

    PreparedStatement insert2 = conn.prepareStatement("insert into testuniverse2 values (?, ?, 32.34, \'{1, 2, 3}\')");

    // create table testuniverse (a text primary key, b int, c numeric, d int[]);
    // create table testuniverse2 (a text primary key, b int, c numeric, d int[]);
//    System.out.println("Deleting table rows now...");
//    statement.execute("delete from testuniverse3;");
//    statement.execute("delete from testuniverse4;");

    long insertionCounter = 0;
    int numOfIterations = 100;
    int internalOps = 1000;
    for (long cnt = iVal; cnt > -1; ++cnt, ++iVal) {
      ++insertionCounter;
      String insertString = "vaibhavInsert" + cnt;
      insert.setString(1, insertString);
      insert.setLong(2, cnt);
      insert2.setString(1, insertString);
      insert2.setLong(2, cnt);

      statement.execute("begin;");
      int ins1 = insert.executeUpdate();
      statement.execute("commit;");

      statement.execute("begin;");
      int ins2 = insert2.executeUpdate();
      statement.execute("commit;");

      if (!(ins1 == 1 && ins2 == 1)) {
        throw new RuntimeException("Insertion didn't happen properly");
      }

      if (cnt % 1000 == 0) {
        System.out.println("Number of insertions so far: " + insertionCounter);
        System.out.println("Do you want to continue inserting? (y/n)... ");
        Scanner sc = new Scanner(System.in);
        char ch = sc.next().charAt(0);
        if (ch == 'y' || ch== 'Y') {
          System.out.println("Continuing inserts...");
          Thread.sleep(400);
        } else {
          System.out.println("Exiting from the app...");
          System.exit(0);
        }
      }

      insert.clearParameters();
      insert2.clearParameters();
    }

    insert.close();
    insert2.close();
    statement.close();
  }

  public static void main(String[] args) {
    int iVal = 1;
    System.out.println("Starting CDC Load tester...");
    while (true) {
      try {
        conn = dataSource.getConnection();
        conn.setAutoCommit(true);
        if (!conn.isClosed()) {
          System.out.println("JDBC connection successful...");
        }

        App appObject = new App();
        appObject.runSampleScript(iVal);

      } catch (RuntimeException re) {
        System.out.println("Received runtime exception while inserting");
        re.printStackTrace();
      } catch (Exception e) {
        System.out.println("Exception raised while performing operations...");
        System.out.println(e);
        e.printStackTrace();
        System.out.println("\n\nTrying again...\n");
      }
    }
  }
}