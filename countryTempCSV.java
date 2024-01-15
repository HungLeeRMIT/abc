package helper;

import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import com.opencsv.*;
import com.opencsv.exceptions.CsvValidationException;

public class countryTempCSV {
    private static final String CSV_FILE = "database/GlobalYearlyLandTempByCountry.csv";
    
    public static Map<String, String> country_Dict() {
        Map<String, String> countryDict = new HashMap<String, String>();
        try (Connection connection = DriverManager.getConnection(database.DATABASE)) {
            // Create a SQL statement
            Statement statement = connection.createStatement();

            // Execute the query to fetch country data
            String getAllCountry = "SELECT Country_code, Country FROM Country";
            ResultSet resultSet = statement.executeQuery(getAllCountry);

            // Iterate over the result set and populate the map
            while (resultSet.next()) {
            String countryCode = resultSet.getString("Country_code");
            String countryName = resultSet.getString("Country");
            countryDict.put(countryName, countryCode);
        }  
        // Close the result set, statement, and connection
        resultSet.close();
        statement.close();
        connection.close();      
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return countryDict;
    }

        static void process(){
        System.out.println("Processing country temperature data...");
        Map<String, String> countryDict = country_Dict();
        try (Connection connection = DriverManager.getConnection(database.DATABASE)) {
            //Check query statement
            String check_sql = "SELECT 1\n" + //
                "        FROM temperature\n" + //
                "        WHERE year = ?\n" + //
                "        AND (city_id IS NULL)\n" + //
                "        AND (state_id IS NULL)\n" + //
                "        AND (country_code = ?)" + //
                "        LIMIT 1;";
            PreparedStatement checkStatement = connection.prepareStatement(check_sql);
            //Insert Values statement
            String sql = "INSERT INTO temperature (\n" + //
            "                    year, average_temp, min_temp, max_temp, country_code\n" + //
            "                )\n" + //
            "                VALUES (?, ?, ?, ?, ?);";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(CSV_FILE));

            Map<String, String> line;
            int insertCount = 0;
            while ((line = reader.readMap()) != null) {
                String year = line.get("Year");
                String countryCode = countryDict.get(line.get("Country"));
                checkStatement.setInt(1, Integer.parseInt(year));
                checkStatement.setString(2, countryCode);
                ResultSet resultSet = checkStatement.executeQuery();
                checkStatement.clearParameters();
                if (!resultSet.next()) {
                    String avgTemp = line.get("AverageTemperature");
                    String minTemp = line.get("MinimumTemperature");
                    String maxTemp = line.get("MaximumTemperature");
                    // Data does not exist, so insert it
                        preparedStatement.setInt(1, Integer.parseInt(year));
                        if (avgTemp != null && !avgTemp.isEmpty()) {
                            preparedStatement.setDouble(2, Double.parseDouble(avgTemp));
                        } else{
                            preparedStatement.setObject(2, null);
                        }
                        if (minTemp != null && !minTemp.isEmpty()) {
                            preparedStatement.setDouble(3, Double.parseDouble(minTemp));
                        } else{
                            preparedStatement.setObject(3, null);
                        }
                        if (maxTemp != null && !maxTemp.isEmpty()) {
                            preparedStatement.setDouble(4, Double.parseDouble(maxTemp));
                        } else{
                            preparedStatement.setObject(4, null);
                        }
                        preparedStatement.setString(5, countryCode);
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
