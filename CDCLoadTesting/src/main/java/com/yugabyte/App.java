package com.yugabyte;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class App 
{
  public static Connection conn;

  public void runSampleScript() throws Exception {
    Statement statement = conn.createStatement();

    PreparedStatement insert = conn.prepareStatement("insert into testuniverse values (?, ?, 32.34, \'{1, 2, 3}\')");
    PreparedStatement delete = conn.prepareStatement("delete from testuniverse where a = ?");
    PreparedStatement selectb = conn.prepareStatement("select b from testuniverse where a = ?");

    int ins = 0, upd = 0, del = 0;
    long iterationCounter = 0;
    int numOfIterations = 2;
    int internalOps = 200;
    int mul = 1;

    for (int cnt = 0; cnt < numOfIterations; ++cnt) {
      ++iterationCounter;

//      System.out.println("Truncating table now...");
//      statement.execute("delete from testuniverse;");

      System.out.println("Starting row insert...");
      for (int i = 1; i <= internalOps; ++i) {
        insert.setString(1, "vaibhav"+(i*mul));
        insert.setInt(2, (i * mul));
        int res = insert.executeUpdate();
        if (res != 1) {
          System.out.println(String.format("Error while inserting (%s, %d), exiting...", "vaibhav"+(i*mul), i*mul));
          System.exit(0);
        }
        System.out.println("Inserted row with a = " + "vaibhav"+(i*mul) + ", b = " + (i*mul) + ", c = 32.34 and d = {1, 2, 3}");
        ++ins;
        insert.clearParameters();
        if (cnt == numOfIterations) {
          System.out.println("Exiting out of app...");
          System.exit(0);
        }
      }
      ++mul;
      Thread.sleep(30000);

//      System.out.println("Starting row update now...");
//      for (int i = 0; i < internalOps; ++i) {
//        statement.execute("begin;");
//        int res = statement.executeUpdate(String.format("update testuniverse set b = b + 1 where a = \'vaibhav"+i+"\';"));
//        if (res == 1) {
//          ++upd;
//        } else {
//          System.out.println("Some error occured while updating row...");
//          System.exit(0);
//        }
//        statement.execute("commit;");
//        System.out.println("Row after update, a = " + "vaibhav"+i + " b = " + (i+1));
//        selectb.clearParameters();
//      }
//
//      System.out.println("Starting row delete...");
//      for (int i = 0; i < internalOps; ++i) {
//        statement.execute("begin;");
//        delete.setString(1, "vaibhav"+i);
//        int res = delete.executeUpdate();
//
//        if (res == 1) {
//          ++del;
//        } else {
//          System.out.println(String.format("Error while deleting key %s, exiting...", "vaibhav"+i));
//          System.exit(0);
//        }
//
//        statement.execute("commit;");
//        System.out.println("Deleted row with a = " + "vaibhav"+i);
//        delete.clearParameters();
//      }
//      ResultSet rs = statement.executeQuery("select * from testuniverse;");
//      if (rs.next() != false) {
//        System.out.println("Row val, a = " + rs.getInt(2));
//        System.out.println("Not all the rows are deleted, exiting...");
//        System.exit(0);
//      }

      if (true) {
        System.out.println("Starting another iteration, take a look at op count...");
        System.out.println(String.format("Inserts = %d\t Updates = %d\t Deletes = %d", ins, upd, del));
        System.out.println("Iteration count: " + iterationCounter);
        ins = 0;
        upd = 0;
        del = 0;
        Thread.sleep(3000);
      }
    }

    insert.close();
    delete.close();
    selectb.close();
    conn.close();
  }

  public static void main(String[] args) {
    System.out.println("Starting CDC Load tester...");
    String[] connectionPoints = {"172.151.62.251:5433"};
    int ptrIdx = 0;
    while (true) {
      try {
        System.out.println("\nTrying to connect via JDBC...\nConnection point = " + connectionPoints[ptrIdx] + "\n");

        String connectionString = String.format("jdbc:yugabytedb://%s/yugabyte?user=yugabyte&password=yugabyte", connectionPoints[ptrIdx]);

        conn = DriverManager.getConnection(connectionString);
        conn.setAutoCommit(true);

        App appObject = new App();
        appObject.runSampleScript();

      } catch (Exception e) {
        System.out.println("Exception raised while performing operations...");
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