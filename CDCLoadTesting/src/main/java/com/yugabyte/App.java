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
      "jdbc:postgresql://172.151.16.124:5433/yugabyte", "yugabyte", "yugabyte");
      conn.setAutoCommit(true);

      if (conn.isValid(10)) {
        System.out.println("Connection established to universe...");
      } else {
        throw new Exception("Cannot establish connection to universe...");
      }

      // PreparedStatement drop = conn.prepareStatement("drop table if exists test;");
      // PreparedStatement create = conn.prepareStatement("create table test (a int primary key, b int);");
      Statement statement = conn.createStatement();

       statement.execute("drop table if exists test;");
       statement.execute("create table test (a text primary key, b int, c numeric, d int[]);");
      PreparedStatement insert = conn.prepareStatement("insert into test values (?, ?, 32.34, \'{1, 2, 3}\')");
      PreparedStatement delete = conn.prepareStatement("delete from test where a = ?");
      PreparedStatement selectb = conn.prepareStatement("select b from test where a = ?");
      // System.out.println("Table created, waiting for 10 seconds to proceed...");
      // Thread.sleep(10000);

      int numOfIterations = 100;
      int internalOps = 1000;
      for (int cnt = 0; cnt > -1; ++cnt) {
        System.out.println("Starting row insert...");
        for (int i = 0; i < internalOps; ++i) {
          insert.setString(1, "vaibhav"+i);
          insert.setInt(2, i);
          int res = insert.executeUpdate();
          if (res != 1) {
            System.out.println(String.format("Error while inserting (%s, %d), exiting...", "vaibhav"+i, i));
            System.exit(0);
          }
          System.out.println("Inserted row with a = " + "vaibhav"+i + ", b = " + (i) + ", c = 32.34 and d = {1, 2, 3}");
          insert.clearParameters();
        }

        System.out.println("Starting row update now...");
        for (int i = 0; i < internalOps; ++i) {
           statement.execute("begin;");
//          System.out.println("[UPDATE]:" + String.format("update test set b = b + 1 where a = vaibhav"+i+";"));
          int res = statement.executeUpdate(String.format("update test set b = b + 1 where a = \'vaibhav"+i+"\';"));
           statement.execute("commit;");
//          selectb.setString(1, "vaibhav"+i);
//          ResultSet rs = selectb.executeQuery();
//          rs.next();
//          int bVal = rs.getInt(1);
//          if (bVal != (i+2)) {
//            System.out.println("Update not performed successfully on a = " + i);
//            System.exit(0);
//          }
//          if (res != 1) {
//            System.out.println(String.format("Error while updating key %s, exiting...", "vaibhav"+i));
//            System.exit(0);
//          }
          System.out.println("Row after update, a = " + "vaibhav"+i + " b = " + (i+1));
          selectb.clearParameters();
        }

        System.out.println("Starting row delete...");
        for (int i = 0; i < internalOps; ++i) {
           statement.execute("begin;");
          delete.setString(1, "vaibhav"+i);
          int res = delete.executeUpdate();
          if (res != 1) {
            System.out.println(String.format("Error while deleting key %s, exiting...", "vaibhav"+i));
            System.exit(0);
          }
           statement.execute("commit;");
          System.out.println("Deleted row with a = " + "vaibhav"+i);
          delete.clearParameters();
        }
        ResultSet rs = statement.executeQuery("select * from test;");
        if (rs.next() != false) {
          System.out.println("Row val, a = " + rs.getInt(2));
          System.out.println("Not all the rows are deleted, exiting...");
          System.exit(0);
        }

        if (true) {
          System.out.println("Starting another iteration, take a look at op count...");
        }
      }

      // drop.close();
      // create.close();
      insert.close();
      delete.close();
      selectb.close();
      conn.close();
    } catch (Exception e) {
      System.out.println("Exception raised while performing operations...");
      System.out.println(e);
      e.printStackTrace();
    }
  }
}