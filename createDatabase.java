package helper;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.ibatis.jdbc.ScriptRunner;

public class createDatabase {
    static void create() {
        try {
            // Create a connection to the database
            Connection connection = DriverManager.getConnection(database.DATABASE);

            // If not present, create a new database
            if (connection != null) {
                System.out.println("A new database has been created.");
            }

            // Create a ScriptRunner object
            ScriptRunner scriptRunner = new ScriptRunner(connection);

            // Set the script file path
            ClassLoader classLoader = database.class.getClassLoader();
            File file = new File(classLoader.getResource("clmchangeCreateTable.sql").getFile());
            String scriptFilePath = file.getAbsolutePath();

            // Run the SQL script file
            scriptRunner.runScript(new FileReader(scriptFilePath));

            // Close the connection
            connection.close();

            System.out.println("SQL script executed successfully.");
        } catch (Exception e) {
            System.out.println("Error executing SQL script: " + e.getMessage());
        }
    }
}
