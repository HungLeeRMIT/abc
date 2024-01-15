package helper;

import java.io.*;
import java.sql.*;
import java.util.Map;
import com.opencsv.*;
import com.opencsv.exceptions.CsvValidationException;

public class populationCSV {
    private static final String CSV_FILE = "database/Population.csv";
  
    static void processPopulation(){
        System.out.println("Processing population data...");
        try (Connection connection = DriverManager.getConnection(database.DATABASE)) {
            //Check query statement
            String check_sql = "SELECT 1 FROM population WHERE year = ? AND country_code = ? LIMIT 1";
            PreparedStatement checkStatement = connection.prepareStatement(check_sql);
            //Insert Values statement
            String sql = "INSERT INTO Population(year, country_code, amount) VALUES (?, ?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(CSV_FILE));

            Map<String, String> line;
            int insertCount = 0;
            while ((line = reader.readMap()) != null) {
                String countryCode = line.get("Country Code");

                for (int year = 1960; year <= 2013; year++) {
                    // Check if the data already exists 
                    checkStatement.setInt(1, year);
                    checkStatement.setString(2, countryCode);
                    ResultSet resultSet = checkStatement.executeQuery();
                    if (!resultSet.next()) {
                        String amount = line.get(Integer.toString(year));
                        // Data does not exist, so insert it
                        preparedStatement.setInt(1, year);
                        preparedStatement.setString(2, countryCode);
                        if (amount != null && !amount.isEmpty()) {
                            preparedStatement.setLong(3, Long.parseLong(amount));
                        } else{
                            preparedStatement.setObject(3, null);
                        }
                        preparedStatement.executeUpdate();
                        insertCount++;
                    }
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

    static void processCountry(){
        System.out.println("Processing country data...");
        try (Connection connection = DriverManager.getConnection(database.DATABASE)) {
            //Check query statement
            String check_sql = "SELECT 1 FROM Country WHERE country_code = ? LIMIT 1";
            PreparedStatement checkStatement = connection.prepareStatement(check_sql);
            //Insert Values statement
            String sql = "INSERT INTO Country(country_code, Country) VALUES (?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(CSV_FILE));

            Map<String, String> line;
            int insertCount = 0;
            while ((line = reader.readMap()) != null) {
                String countryCode = line.get("Country Code");
                String country = line.get("Country Name");
                checkStatement.setString(1, countryCode);
                ResultSet resultSet = checkStatement.executeQuery();
                if (!resultSet.next()) {
                    // Data does not exist, so insert it
                    preparedStatement.setString(1, countryCode);
                    preparedStatement.setString(2, country);
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

                    
                
            

           

