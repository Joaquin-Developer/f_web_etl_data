CREATE DATABASE IF NOT EXISTS tour_dates;
USE tour_dates;


CREATE TABLE IF NOT EXISTS countries (
    country_id INT AUTO_INCREMENT PRIMARY KEY,
    country_name varchar(100) not null
);


CREATE TABLE states (
    state_id INT AUTO_INCREMENT PRIMARY KEY,
    state_name VARCHAR(100) NOT NULL,
    country_id INT,
    FOREIGN KEY (country_id) REFERENCES countries(country_id)
);


CREATE TABLE cities (
    city_id INT AUTO_INCREMENT PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    state_id INT,
    FOREIGN KEY (state_id) REFERENCES states(state_id)
);


CREATE TABLE tours (
    tour_id INT AUTO_INCREMENT PRIMARY KEY,
    tour_name VARCHAR(100) NOT NULL
);


CREATE TABLE shows (
    show_id INT AUTO_INCREMENT PRIMARY KEY,
    tour_id INT,
    city_id INT,
    show_date DATE,
    scenary VARCHAR(100),
    FOREIGN KEY (tour_id) REFERENCES tours(tour_id),
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);
