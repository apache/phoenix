package org.apache.phoenix.zipkin;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;

@RestController
public class PhoenixZipkinController {

    @RequestMapping("/")
    public String index() {
        return "Greetings from Spring Boot!";
    }
    @RequestMapping("/foo")
    public String foo() throws SQLException{
                Statement stmt = null;
                ResultSet rset = null;
String rString = "";
try{
                Connection con = DriverManager.getConnection("jdbc:phoenix:localhost:2181");
                stmt = con.createStatement();

                PreparedStatement statement = con.prepareStatement("select * from SYSTEM.TRACING_STATS");
                rset = statement.executeQuery();
                while (rset.next()) {
                        System.out.println(rset.getString("mycolumn"));
rString += rset.getString("mycolumn");
                }
                statement.close();
                con.close();
}catch(SQLException e){
throw (e);
}

        return "Greetings from foo!, "+rString;
    }
@RequestMapping("/api/v1/services")
    public String services() {
        return "[\"phoenix-server\"]";
    }
@RequestMapping("/api/v1/spans")
    public String spans() {
        return "[]";
    }
}
