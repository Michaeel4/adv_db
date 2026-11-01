import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
public class ConnectionFactory {


    // this class is required by the blocking class template.
    // there are two connection requests made in that class
    // so we have to provide here the connection string to the local db. - Michael


    // this is used in blocking class to inject the connection static variable - Michael
    public static final String POSTGRESQL = "postgres";


    // returns valid connection to db or throws exceptions.
    public static Connection getDefaultParameterConnection(String postgres){
        try{
            // establishes a connection to localhost port 5432:adb db
            // user name, no pw - Michael
            return DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/adb",
                    System.getProperty("user.name"),
                    "");

        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }


}
