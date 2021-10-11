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

      // PreparedStatement drop = conn.prepareStatement("drop table if exists test;");
      // PreparedStatement create = conn.prepareStatement("create table test (a int primary key, b int);");
      Statement statement = conn.createStatement();

      // statement.execute("drop table if exists test;");
      // statement.execute("create table test (a int primary key, b int);");
      PreparedStatement insert = conn.prepareStatement("insert into test values (?, ?)");
      PreparedStatement delete = conn.prepareStatement("delete from test where a = ?");
      PreparedStatement selectb = conn.prepareStatement("select b from test where a = ?");
      System.out.println("Table created, waiting for 10 seconds to proceed...");
      Thread.sleep(10000);

      int numOfIterations = 1;
      int internalOps = 50;
      for (int cnt = 0; cnt < numOfIterations; ++cnt) {
        System.out.println("Starting row insert...");
        Thread.sleep(3000);
        for (int i = 0; i < internalOps; ++i) {
          insert.setInt(1, i);
          insert.setInt(2, i+1);
          int res = insert.executeUpdate();
          if (res != 1) {
            System.out.println(String.format("Error while inserting (%d, %d), exiting...", i, i + 1));
            System.exit(0);
          }
          System.out.println("Inserted row with a = " + i + " and b = " + (i + 1));
          insert.clearParameters();
        }

        System.out.println("Starting row update now...");
        Thread.sleep(3000);
        for (int i = 0; i < internalOps; ++i) {
          // statement.execute("begin;");
          int res = statement.executeUpdate(String.format("update test set b = b + 1 where a = %d;", i));
          // statement.execute("commit;");
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
          System.out.println("Row after update, a = " + i + " b = " + (i + 2));
          selectb.clearParameters();
        }

        System.out.println("Starting row delete...");
        Thread.sleep(3000);
        for (int i = 0; i < internalOps; ++i) {
          // statement.execute("begin;");
          delete.setInt(1, i);
          int res = delete.executeUpdate();
          if (res != 1) {
            System.out.println(String.format("Error while deleting key %d, exiting...", i));
            System.exit(0);
          }
          // statement.execute("commit;");
          System.out.println("Deleted row with a = " + i);
          delete.clearParameters();
        }
        ResultSet rs = statement.executeQuery("select * from test;");
        if (rs.next() != false) {
          System.out.println("Row val, a = " + rs.getInt(1));
          System.out.println("Not all the rows are deleted, exiting...");
          System.exit(0);
        }
        System.out.println("Starting another iteration, take a look at op count...");
        Thread.sleep(10000);
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