package com.yugabyte;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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

      statement.execute("drop table if exists test;");
      statement.execute("create table test (a int primary key, b int);");
      PreparedStatement insert = conn.prepareStatement("insert into test values (?, ?)");
      PreparedStatement delete = conn.prepareStatement("delete from test where a = ?");
      System.out.println("Table created, waiting for 10 seconds to proceed...");
      Thread.sleep(10000);

      for (int cnt = 0; cnt < 1000; ++cnt) {
        for (int i = 0; i < 100; ++i) {
          insert.setInt(1, i);
          insert.setInt(2, i+1);
          int res = insert.executeUpdate();
          if (res != 1) {
            System.out.println(String.format("Error while inserting (%d, %d), exiting...", i, i + 1));
            System.exit(0);
          }
          insert.clearParameters();
        }

        for (int i = 0; i < 100; ++i) {
          int res = statement.executeUpdate("update test set b = b + 1 where a = " + i + ";");
          if (res != 1) {
            System.out.println(String.format("Error while updating key %d, exiting...", i));
            System.exit(0);
          }
        }

        for (int i = 0; i < 100; ++i) {
          statement.execute("begin;");
          delete.setInt(1, i);
          int res = delete.executeUpdate();
          if (res != 1) {
            System.out.println(String.format("Error while deleting key %d, exiting...", i));
            System.exit(0);
          }
          statement.execute("commit;");
          delete.clearParameters();
        }
      }

      // drop.close();
      // create.close();
      insert.close();
      delete.close();

      conn.close();
    } catch (Exception e) {
      System.out.println("Exception raised while performing operations...");
    }
  }
}