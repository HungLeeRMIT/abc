package helper;

import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import com.opencsv.*;
import com.opencsv.exceptions.CsvValidationException;

public class stateTempCSV {
    private static final String CSV_FILE = "database/GlobalYearlyLandTempByState.csv";

    static void processState() {
        System.out.println("Processing state data...");
        try (Connection connection = DriverManager.getConnection(database.DATABASE)) {
            //Check query statement
            String check_sql = "SELECT 1\n" + //
                    "        FROM State\n" + //
                    "        WHERE name = ? and country_code = ?\n" + //
                    "        LIMIT 1;";
            PreparedStatement checkStatement = connection.prepareStatement(check_sql);
            //Insert Values statement
            String sql = "INSERT INTO State(name, country_code) VALUES (?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(CSV_FILE));

            Map<String, String> countryDict = countryTempCSV.country_Dict();
            Map<String, String> line;
            int insertCount = 0;
            while ((line = reader.readMap()) != null) {
                String countryCode = countryDict.get(line.get("Country"));
                String stateName = line.get("State");
                // Check if the data already exists
                checkStatement.setString(1, stateName);
                checkStatement.setString(2, countryCode);
                ResultSet resultSet = checkStatement.executeQuery();
                if (!resultSet.next()) {
                    // Data does not exist, so insert it
                    preparedStatement.setString(1, stateName);
                    preparedStatement.setString(2, countryCode);
                    preparedStatement.executeUpdate();
                    insertCount++;
                }
            }
        connection.close();
        System.out.println("Data inserted successfully! Total inserts:" + insertCount);
        } catch (SQLException | IOException ex) {
        ex.printStackTrace();
        } catch (CsvValidationException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }

    public static Map<Map<String, String>, String> state_Dict() {
        Map<Map<String, String>, String> stateDict = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(database.DATABASE)) {
            // Create a SQL statement
            Statement statement = connection.createStatement();
    
            // Execute the query to fetch state data
            String getAllState = "SELECT state_id, name, country_code FROM State";
            ResultSet resultSet = statement.executeQuery(getAllState);
    
            // Iterate over the result set and populate the map
            while (resultSet.next()) {
                String stateName = resultSet.getString("name");
                String countryCode = resultSet.getString("country_code");
                int state_id = resultSet.getInt("state_id");
    
                Map<String, String> stateData = new HashMap<>();
                stateData.put("state_name", stateName);
                stateData.put("country_code", countryCode);
    
                stateDict.put(stateData, Integer.toString(state_id));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return stateDict;
    }

    static void process() {
    System.out.println("Processing state temperature data...");
    try (Connection connection = DriverManager.getConnection(database.DATABASE)) {
        //Check query statement
        String check_sql = "SELECT 1\n" + //
                    "        FROM temperature\n" + //
                    "        WHERE year = ?\n" + //
                    "        AND (city_id IS NULL)\n" + //
                    "        AND (state_id = ?)\n" + //
                    "        AND (country_code = ?)" + //
                    "        LIMIT 1;";
        PreparedStatement checkStatement = connection.prepareStatement(check_sql);
        //Insert Values statement
        String sql = "INSERT INTO temperature (\n" + //
            "                    year, average_temp, min_temp, max_temp, country_code, state_id\n" + //
            "                )\n" + //
            "                VALUES (?, ?, ?, ?, ?, ?);";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(CSV_FILE));

        Map<Map<String, String>, String> stateDict = state_Dict();
        Map<String, String> countryDict = countryTempCSV.country_Dict();
        Map<String, String> line;
        int insertCount = 0;
        while ((line = reader.readMap()) != null) {
            String year = line.get("Year");
            String stateName = line.get("State");
            String countryCode = countryDict.get(line.get("Country"));
            Map<String, String> stateData = new HashMap<>();
            stateData.put("state_name", stateName);
            stateData.put("country_code", countryCode);
            String stateId = stateDict.get(stateData);    
            //check if the data already exists
            checkStatement.setInt(1, Integer.parseInt(year));
            checkStatement.setString(2, stateId);
            checkStatement.setString(3, countryCode);
            ResultSet resultSet = checkStatement.executeQuery();
            if (!resultSet.next()) {
                String average_temp = line.get("AverageTemperature");
                String min_temp = line.get("MinimumTemperature");
                String max_temp = line.get("MaximumTemperature");
                // Data does not exist, so insert it
                preparedStatement.setInt(1, Integer.parseInt(year));
                if (average_temp != null && !average_temp.isEmpty()) {
                    preparedStatement.setDouble(2, Double.parseDouble(average_temp));
                } else{
                    preparedStatement.setObject(2, null);
                }
                if (min_temp != null && !min_temp.isEmpty()) {
                    preparedStatement.setDouble(3, Double.parseDouble(min_temp));
                } else{
                    preparedStatement.setObject(3, null);
                }
                if (max_temp != null && !max_temp.isEmpty()) {
                    preparedStatement.setDouble(4, Double.parseDouble(max_temp));
                } else{
                    preparedStatement.setObject(4, null);
                }
                preparedStatement.setString(5, countryCode);
                preparedStatement.setString(6, stateId);
                preparedStatement.executeUpdate();
                insertCount++;
            }
        }    
        connection.close();
        System.out.println("Data inserted successfully! Total inserts:" + insertCount);
        } catch (SQLException | IOException ex) {
            ex.printStackTrace();
        } catch (CsvValidationException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }
}    