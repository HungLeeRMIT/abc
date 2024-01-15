PRAGMA foreign_keys = OFF;
drop table if exists Country;
drop table if exists City;
drop table if exists State;
drop table if exists Population;
drop table if exists Temperature;
PRAGMA foreign_keys = ON;

-- Country Table
CREATE TABLE Country (
    country_code VARCHAR(3) PRIMARY KEY,
    country VARCHAR(255) NOT NULL
);

-- City Table
CREATE TABLE City (
    city_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255) NOT NULL,
    country_code VARCHAR(3) NOT NULL,
    FOREIGN KEY (country_code) REFERENCES Country(country_code)
);

-- State Table
CREATE TABLE State (
    state_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255) NOT NULL,
    country_code VARCHAR(3) NOT NULL,
    FOREIGN KEY (country_code) REFERENCES Country(country_code)
);

-- Population Table
CREATE TABLE Population (
    population_id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    country_code VARCHAR(3),
    amount INTEGER,
    FOREIGN KEY (country_code) REFERENCES Country(country_code)
);

-- Temperature Table
CREATE TABLE Temperature (
    temperature_id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    average_temp DECIMAL(5,2),
    min_temp DECIMAL(5,2),
    max_temp DECIMAL(5,2),
    land_ocean_average_temperature DECIMAL(5,2), 
    land_ocean_min_temperature DECIMAL(5,2), 
    land_ocean_max_temperature DECIMAL(5,2),
    country_code VARCHAR(3),
    city_id INT,
    state_id INT,
    FOREIGN KEY (country_code) REFERENCES Country(country_code),
    FOREIGN KEY (city_id) REFERENCES City(city_id),
    FOREIGN KEY (state_id) REFERENCES State(state_id)
);
