package helper;

import java.io.*;
import java.sql.*;
import java.util.Map;
import com.opencsv.*;
import com.opencsv.exceptions.CsvValidationException;

public class globalTempCSV {
    private static final String CSV_FILE = "database/GlobalYearlyTemp.csv";

    static void process() {
        System.out.println("Processing global temperature data...");
        try (Connection connection = DriverManager.getConnection(database.DATABASE)) {
        //Check query statement
        String check_sql = "SELECT 1\n" + //
                "        FROM temperature\n" + //
                "        WHERE year = ?\n" + //
                "        AND (city_id IS NULL)\n" + //
                "        AND (state_id IS NULL)\n" + //
                "        AND (country_code IS NULL)" + //
                "        LIMIT 1;";
        PreparedStatement checkStatement = connection.prepareStatement(check_sql);
        //Insert Values statement
        String sql = "INSERT INTO temperature (\n" + //
                "                    year, average_temp, min_temp, max_temp,\n" + //
                "                    land_ocean_average_temperature, land_ocean_min_temperature, land_ocean_max_temperature\n" + //
                "                )\n" + //
                "                VALUES (?, ?, ?, ?, ?, ?, ?);";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(CSV_FILE));

        Map<String, String> line;
        int insertCount = 0;
        while ((line = reader.readMap()) != null) {
            int year = Integer.parseInt(line.get("Year"));
            // Check if the data already exists 
                checkStatement.setInt(1, year);
                ResultSet resultSet = checkStatement.executeQuery();
                if (!resultSet.next()) {
                    String average_temp = line.get("AverageTemperature");
                    String min_temp = line.get("MinimumTemperature");
                    String max_temp = line.get("MaximumTemperature");
                    String land_ocean_average_temperature = line.get("LandOceanAverageTemperature");
                    String land_ocean_min_temperature = line.get("LandOceanMinimumTemperature");
                    String land_ocean_max_temperature = line.get("LandOceanMaximumTemperature");
                    // Data does not exist, so insert it
                        preparedStatement.setInt(1, year);
                        preparedStatement.setDouble(2, Double.parseDouble(average_temp));
                        preparedStatement.setDouble(3, Double.parseDouble(min_temp));
                        preparedStatement.setDouble(4, Double.parseDouble(max_temp));
                        //check if the value is null or not
                        //if it is null, set it to null
                        //if it is not null, set it to the value
                        if (land_ocean_average_temperature != null && !land_ocean_average_temperature.isEmpty()) {
                            preparedStatement.setDouble(5, Double.parseDouble(land_ocean_average_temperature));
                        } else{
                            preparedStatement.setObject(5, null);
                        }
                        if (land_ocean_min_temperature != null && !land_ocean_min_temperature.isEmpty()) {
                            preparedStatement.setDouble(6, Double.parseDouble(land_ocean_min_temperature));
                        } else{
                            preparedStatement.setObject(6, null);
                        }
                        if (land_ocean_max_temperature != null && !land_ocean_max_temperature.isEmpty()) {
                            preparedStatement.setDouble(7, Double.parseDouble(land_ocean_max_temperature));
                        } else{
                            preparedStatement.setObject(7, null);
                        }
                        preparedStatement.executeUpdate();
                    }
                    insertCount++;
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




