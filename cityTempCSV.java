package helper;

import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import com.opencsv.*;
import com.opencsv.exceptions.CsvValidationException;

public class cityTempCSV {
    final static String CSV_FILE = "database/GlobalYearlyLandTempByCity.csv";

    static void processCity() {
        System.out.println("Processing city data...");
        try (Connection connection = DriverManager.getConnection(database.DATABASE)) {
            //Check query statement
            String check_sql = "SELECT 1\n" + //
                    "        FROM City\n" + //
                    "        WHERE name = ? and country_code = ?\n" + //
                    "        LIMIT 1;";
            PreparedStatement checkStatement = connection.prepareStatement(check_sql);
            //Insert Values statement
            String sql = "INSERT INTO City(name, country_code) VALUES (?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(CSV_FILE));

            Map<String, String> countryDict = countryTempCSV.country_Dict();
            Map<String, String> line;
            int insertCount = 0;
            while ((line = reader.readMap()) != null) {
                String countryCode = countryDict.get(line.get("Country"));
                String cityName = line.get("City");
                // Check if the data already exists
                checkStatement.setString(1, cityName);
                checkStatement.setString(2, countryCode);
                ResultSet resultSet = checkStatement.executeQuery();
                if (!resultSet.next()) {
                    // Data does not exist, so insert it
                    preparedStatement.setString(1, cityName);
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

    public static Map<Map<String, String>, String> city_Dict() {
        Map<Map<String, String>, String> cityDict = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(database.DATABASE)) {
            // Create a SQL statement
            Statement statement = connection.createStatement();
    
            // Execute the query to fetch state data
            String getAllCity = "SELECT city_id, name, country_code FROM City";
            ResultSet resultSet = statement.executeQuery(getAllCity);
    
            // Iterate over the result set and populate the map
            while (resultSet.next()) {
                String cityName= resultSet.getString("name");
                String countryCode = resultSet.getString("country_code");
                int city_id = resultSet.getInt("city_id");
    
                Map<String, String> cityData = new HashMap<>();
                cityData.put("city_name", cityName);
                cityData.put("country_code", countryCode);
    
                cityDict.put(cityData, Integer.toString(city_id));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return cityDict;
    }
    
    static void process() {
        System.out.println("Processing city temperature data...");
        try (Connection connection = DriverManager.getConnection(database.DATABASE)) {
            // Check query statement
            String check_sql = "SELECT 1\n" + //
                    "        FROM temperature\n" + //
                    "        WHERE year = ?\n" + //
                    "        AND (city_id = ?)\n" + //
                    "        AND (state_id IS NULL)\n" + //
                    "        AND (country_code = ?)" + //
                    "        LIMIT 1;";
            PreparedStatement checkStatement = connection.prepareStatement(check_sql);
            // Insert Values statement
            String sql = "INSERT INTO temperature (\n" + //
                    "                    year, average_temp, min_temp, max_temp, country_code, city_id\n" + //
                    "                )\n" + //
                    "                VALUES (?, ?, ?, ?, ?, ?);";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(CSV_FILE));

            Map<Map<String, String>, String> cityDict = city_Dict();
            Map<String, String> countryDict = countryTempCSV.country_Dict();
            Map<String, String> line;
            int insertCount = 0;
            while ((line = reader.readMap()) != null) {
                String year = line.get("Year");
                String stateName = line.get("City");
                String countryCode = countryDict.get(line.get("Country"));
                Map<String, String> cityData = new HashMap<>();
                cityData.put("city_name", stateName);
                cityData.put("country_code", countryCode);
                String cityID = cityDict.get(cityData);
                // check if the data already exists
                checkStatement.setInt(1, Integer.parseInt(year));
                checkStatement.setString(2, cityID);
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
                    } else {
                        preparedStatement.setObject(2, null);
                    }
                    if (min_temp != null && !min_temp.isEmpty()) {
                        preparedStatement.setDouble(3, Double.parseDouble(min_temp));
                    } else {
                        preparedStatement.setObject(3, null);
                    }
                    if (max_temp != null && !max_temp.isEmpty()) {
                        preparedStatement.setDouble(4, Double.parseDouble(max_temp));
                    } else {
                        preparedStatement.setObject(4, null);
                    }
                    preparedStatement.setString(5, countryCode);
                    preparedStatement.setString(6, cityID);
                    System.out.println(preparedStatement);
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
