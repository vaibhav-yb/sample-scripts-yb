import java.lang.System.Logger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Main {
  public static void main(String[] args) {
    java.util.logging.Logger LOG = java.util.logging.Logger.getLogger(Main.class.toString());
    Connection conn;
    try {
      conn = DriverManager.getConnection(
      "jdbc:postgresql://127.0.0.1:5433/yugabyte",
      "yugabyte",
      "yugabyte");
      conn.setAutoCommit(true);
    } catch (SQLException e) {
      System.out.println("SQL Exception caught...");
    }

    try {
      if (conn == null)
        throw new Exception("Connection conn not initialized...");
      Statement statement = conn.createStatement();
      statement.execute("drop table if exists test;");
      statement.execute("create table test (a int primary key, b int);");
      System.out.println("Sleeping for 10 seconds after creating table...");
      Thread.sleep(10000);
      PreparedStatement ps = conn.prepareStatement("insert into test values (?, ?)");

      for (long i = 0; i < 10000; ++i) {
        ps.setLong(1, i);
        ps.setLong(1, i+1);

        int cnt = ps.executeUpdate();
        if (cnt == 1) {
          System.out.println("Inserted row with a = " + i + " and b = " + (i + 1));
          Thread.sleep(500);
        } else {
          System.out.println("Single row not inserted, breaking from loop...");
          break;
        }
        ps.clearParameters();
      }
      ps.close();
      conn.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
