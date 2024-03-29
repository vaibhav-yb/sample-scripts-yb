package com.yugabyte;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class App 
{
  public static void main(String[] args) {
    try {
      System.out.println("Starting CDC Load tester...");
      Connection conn = DriverManager.getConnection(
      "jdbc:postgresql://127.0.0.1:5433/yugabyte",
      "yugabyte",
      "yugabyte");
      conn.setAutoCommit(true);
      System.out.println("Network timeout is: " + conn.getNetworkTimeout());
      conn.setNetworkTimeout(0);

      // PreparedStatement drop = conn.prepareStatement("drop table if exists test;");
      // PreparedStatement create = conn.prepareStatement("create table test (a int primary key, b int);");
      Statement statement = conn.createStatement();

      // statement.execute("drop table if exists test;");
      // statement.execute("create table test (a int primary key, b int);");
      PreparedStatement insert = conn.prepareStatement("insert into test values (?, ?, ?)");
      PreparedStatement delete = conn.prepareStatement("delete from test where a = ?");
      PreparedStatement selectb = conn.prepareStatement("select b from test where a = ?");
      // System.out.println("Table created, waiting for 10 seconds to proceed...");
      // Thread.sleep(10000);

      int numOfIterations = 1000;
      int internalOps = 1000;
      long totalOps = 0;
      for (int cnt = 0; cnt > -1; ++cnt) {
        System.out.println("Starting row insert...");
        // Thread.sleep(300);
        for (int i = 0; i < internalOps; ++i) {
          insert.setInt(1, i);
          insert.setInt(2, i+1);
          insert.setString(3, "vaibhav");
          int res = insert.executeUpdate();
          if (res != 1) {
            System.out.println(String.format("Error while inserting (%d, %d, %s), exiting...", i, i + 1, "vaibhav"));
            System.exit(0);
          }
          System.out.println("Inserted row with a = " + i + " and b = " + (i + 1) + " and c = vaibhav");
          insert.clearParameters();
        }

        System.out.println("Starting row update now...");
        // Thread.sleep(300);
        for (int i = 0; i < internalOps; ++i) {
          statement.execute("begin;");
          int res = statement.executeUpdate(String.format("update test set b = b + 1 where a = %d;", i));
          statement.execute("commit;");
          selectb.setInt(1, i);
          ResultSet rs = selectb.executeQuery();
          rs.next();
          int bVal = rs.getInt(1);
          if (bVal != (i+2)) {
            System.out.println("Update not performed successfully on a = " + i);
            System.exit(0);
          }
          if (res != 1) {
            System.out.println(String.format("Error while updating key %d, exiting...", i));
            System.exit(0);
          }
          System.out.println("Row after update, a = " + i + " b = " + (i + 2) + " c = vaibhav");
          selectb.clearParameters();
        }

        System.out.println("Starting row delete...");
        // Thread.sleep(300);
        for (int i = 0; i < internalOps; ++i) {
          statement.execute("begin;");
          delete.setInt(1, i);
          int res = delete.executeUpdate();
          if (res != 1) {
            System.out.println(String.format("Error while deleting key %d, exiting...", i));
            System.exit(0);
          }
          statement.execute("commit;");
          System.out.println("Deleted row with a = " + i);
          delete.clearParameters();
        }
        ResultSet rs = statement.executeQuery("select * from test;");
        if (rs.next() != false) {
          System.out.println("Row val, a = " + rs.getInt(1));
          System.out.println("Not all the rows are deleted, exiting...");
          System.exit(0);
        }
        
        if (cnt != numOfIterations - 1) {
          System.out.println("Starting another iteration, take a look at op count...");
        }

        Thread.sleep(500);
        totalOps += (3 * internalOps);
        System.out.println("Total Insert+Update+Delete = " + totalOps);
      }

      // drop.close();
      // create.close();
      insert.close();
      delete.close();
      selectb.close();
      conn.close();
    } catch (Exception e) {
      System.out.println("Exception raised while performing operations...");
      e.printStackTrace();
    }
  }
}