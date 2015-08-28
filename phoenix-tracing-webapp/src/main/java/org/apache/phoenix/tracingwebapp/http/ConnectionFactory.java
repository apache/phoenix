package org.apache.phoenix.tracingwebapp.http;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
*
* ConnectionFactory is to handle database connection
*
*/
public class ConnectionFactory {
  
  private static Connection con;
  protected static String PHOENIX_HOST = "localhost";
  protected static int PHOENIX_PORT = 2181;
  
  public static Connection getConnection() throws SQLException, ClassNotFoundException {
    if (con == null || con.isClosed()) {
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
      con = DriverManager.getConnection("jdbc:phoenix:"+PHOENIX_HOST+":"+PHOENIX_PORT);
    }
    return con;
  }
}
