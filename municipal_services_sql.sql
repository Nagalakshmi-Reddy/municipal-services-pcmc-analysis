-- 1. Create Database
CREATE DATABASE municipal_services;
USE municipal_services;

-- 2. Create tables for each file
CREATE TABLE shelter_homes(area_address varchar(255), ward int, vidhan_sabha varchar(100), latitude double, longitude double,contact_name varchar(100),contact_info varchar(100));
CREATE TABLE hospital_locations(site_name varchar(100), category varchar(100), address varchar(255), types_of_hos varchar(50), latitude double, longitude double);
CREATE TABLE municipal_boundaries_raw(zone_no varchar(10), latitude double, longitude double);
CREATE TABLE food_distribution(ward int,vidhan_sabha varchar(100),latitude double,longitude double,contact_name varchar(50),contact_info varchar(100),address varchar(255));
	
-- 3. Import raw data & read the datasets
SELECT * FROM shelter_homes;
SELECT * FROM hospital_locations;
SELECT * FROM food_distribution;
SELECT * FROM municipal_boundaries_raw;

-- 4. Data Cleaning - remove duplicates, null values
SELECT count(*) FROM municipal_boundaries_raw;

-- since the datasets are csv files, the null values are imported as empty spaces 
SELECT * FROM municipal_boundaries_raw WHERE zone_no = ' ' OR latitude = ' ' OR longitude = ' ';
SELECT count(*) FROM municipal_boundaries_raw WHERE zone_no = ' ';
UPDATE municipal_boundaries_raw SET zone_no = NULL WHERE zone_no = ' ';

-- convert empty spaces in the data into null to remove null values
SELECT * FROM municipal_boundaries_raw WHERE zone_no is NULL;

-- Create a new table to remove duplicates and null values
CREATE TABLE municipal_boundaries_clean as SELECT DISTINCT * FROM municipal_boundaries_raw WHERE zone_no is not NULL;
SELECT * FROM municipal_boundaries_clean;

-- Check for null values in remaining tables
SELECT count(*) FROM shelter_homes WHERE area_address = ' ';
SELECT count(*) FROM hospital_locations WHERE site_name=' ';
SELECT count(*) FROM food_distribution WHERE ward=' ';

-- Add ID column for each table as an unique identifier of rows
ALTER TABLE municipal_boundaries_clean
	ADD id int AUTO_INCREMENT PRIMARY KEY;
    
ALTER TABLE shelter_homes
	ADD id int AUTO_INCREMENT PRIMARY KEY;
    
ALTER TABLE hospital_locations
	ADD id int AUTO_INCREMENT PRIMARY KEY;
    
ALTER TABLE food_distribution
	ADD id int AUTO_INCREMENT PRIMARY KEY;
    
-- 5. Understand the structure of data
SELECT Distinct zone_no FROM municipal_boundaries_clean ORDER BY zone_no;
SELECT count(*) FROM  municipal_boundaries_clean;
SELECT * FROM municipal_boundaries_clean LIMIT 10;

SELECT * FROM shelter_homes;
SELECT distinct ward, area_address FROM shelter_homes;

SELECT * FROM food_distribution;
SELECT distinct ward, address FROM food_distribution; 
SELECT distinct ward FROM food_distribution ORDER BY ward;

SELECT * FROM hospital_locations;
SELECT distinct types_of_hos, Count(*) FROM hospital_locations GROUP BY types_of_hos;
UPDATE hospital_locations SET types_of_hos = 'Maternity hospital' WHERE types_of_hos IN ('maternitey hospital','maternity');

UPDATE hospital_locations SET types_of_hos = trim(replace(replace(types_of_hos,'\r',''),'\n',''));
-- OR
SELECT id, types_of_hos FROM hospital_locations WHERE types_of_hos LIKE 'maternity%';
UPDATE hospital_locations SET types_of_hos = 'Maternity hospital' WHERE ID = 1;

UPDATE hospital_locations SET types_of_hos = 'Dispensary' WHERE types_of_hos IN ('Dispensary type','Dispensory','PCMC DISPENSARY','Dispensary type govt');
UPDATE hospital_locations SET types_of_hos = 'OPD' WHERE types_of_hos = 'O.P.D';
SELECT distinct types_of_hos, Count(*) FROM hospital_locations GROUP BY types_of_hos;

-- 6. Analyze the datasets
# A. Count of Entries per Zone
SELECT ZONE_NO, COUNT(*) AS total_locations
FROM municipal_boundaries_clean
GROUP BY ZONE_NO
ORDER BY total_locations ASC;

# B. Count of food distribution points per ward
CREATE TABLE food_distribution_summary AS
SELECT Ward, COUNT(distinct address) AS food_points,
COUNT(distinct contact_name) as contact_persons
FROM food_distribution
GROUP BY Ward;
select * from food_distribution_summary order by ward;

# C. Count of shelters homes per ward
CREATE TABLE shelter_summary AS
SELECT Ward, COUNT(distinct area_address) AS shelter_count,
COUNT(distinct contact_name) AS contact_persons
FROM shelter_homes
GROUP BY Ward;
select * from shelter_summary;

# D. Compare food distribution counts and shelter homes per ward
CREATE TABLE ward_summary AS
SELECT
    COALESCE(f.Ward, s.Ward) AS Ward,
    COALESCE(food_points, 0) AS food_distribution_points,
    COALESCE(f.contact_persons, 0) AS food_contact_persons,
    COALESCE(shelter_count, 0) AS shelter_homes,
    COALESCE(s.contact_persons, 0) AS shelter_contact_persons
FROM
    food_distribution_summary f
LEFT JOIN shelter_summary s ON f.Ward = s.Ward
UNION
SELECT
    COALESCE(f.Ward, s.Ward) AS Ward,
    COALESCE(food_points, 0) AS food_distribution_points,
    COALESCE(f.contact_persons, 0) AS food_contact_persons,
    COALESCE(shelter_count, 0) AS shelter_homes,
    COALESCE(s.contact_persons, 0) AS shelter_contact_persons
FROM
    shelter_summary s
LEFT JOIN food_distribution_summary f ON f.Ward = s.Ward;

SELECT * FROM ward_summary ORDER BY Ward;

# E. Count of hospitals per zone
CREATE TABLE hospital_merge AS (
WITH hospital_with_id AS (
    SELECT 
        *,
        DENSE_RANK() OVER (ORDER BY site_name) AS hospital_id
    FROM hospital_locations
)
, hospital_zone_map AS (
    SELECT
        h.hospital_id,
        h.site_name,
        h.latitude AS hospital_lat,
        h.longitude AS hospital_long,
        b.zone_no,
        b.latitude AS zone_lat,
        b.longitude AS zone_long,
        SQRT(POW(h.latitude - b.latitude, 2) + POW(h.longitude - b.longitude, 2)) AS distance
    FROM hospital_with_id h
    JOIN municipal_boundaries_clean b ON 1=1
)    
, hospital_to_zone AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY hospital_id ORDER BY distance) AS rn
        FROM hospital_zone_map
    ) sub
    WHERE rn = 1
)
SELECT
    zone_no,
    COUNT(*) AS hospital_count
FROM hospital_to_zone
GROUP BY zone_no
ORDER BY zone_no);

# F. Count of food_distribution_points per zone
CREATE TABLE food_merge AS(
WITH food_with_id AS (
    SELECT 
        *,
        DENSE_RANK() OVER (ORDER BY latitude) AS food_id
    FROM food_distribution
)
, food_zone_map AS (
    SELECT 
        f.food_id,
        f.address,
        f.latitude AS food_lat,
        f.longitude AS food_long,
        b.zone_no,
        b.latitude AS zone_lat,
        b.longitude AS zone_long,
        SQRT(POW(f.latitude - b.latitude, 2) + POW(f.longitude - b.longitude, 2)) AS distance
    FROM food_with_id f
    JOIN municipal_boundaries_clean b ON 1=1
)                                                                                                                                                                                                                                                                                                                      
 , food_to_zone AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY food_id ORDER BY distance) AS rn
        FROM food_zone_map
    ) sub
    WHERE rn = 1
)
SELECT 
    zone_no,
    COUNT(*) AS food_distribution_points
FROM food_to_zone
GROUP BY zone_no
ORDER BY zone_no);

# G. Count of shelter_homes per zone
CREATE TABLE shelter_merge AS(
WITH shelter_with_id AS (
    SELECT 
        *,
        DENSE_RANK() OVER (ORDER BY area_address) AS shelter_id
    FROM shelter_homes
)
, shelter_zone_map AS (
    SELECT 
        s.shelter_id,
        s.area_address,
        s.latitude AS shelter_lat,
        s.longitude AS shelter_long,
        b.zone_no,
        b.latitude AS zone_lat,
        b.longitude AS zone_long,
        SQRT(POW(s.latitude - b.latitude, 2) + POW(s.longitude - b.longitude, 2)) AS distance
    FROM shelter_with_id s
    JOIN municipal_boundaries_clean b ON 1=1
)                                                                                                                                                                                                                                                                                                                      
 , shelter_to_zone AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY shelter_id ORDER BY distance) AS rn
        FROM shelter_zone_map
    ) sub
    WHERE rn = 1
)
SELECT 
    zone_no,
    COUNT(*) AS shelter_homes
FROM shelter_to_zone
GROUP BY zone_no
ORDER BY zone_no);

# H. Compare all three tables per zone
CREATE TABLE zone_summary AS (
SELECT 
    COALESCE(h.zone_no, f.zone_no, s.zone_no) AS zone_no,
    COALESCE(h.hospital_count, 0) AS hospital_count,
    COALESCE(f.food_distribution_points, 0) AS food_distribution_points,
    COALESCE(s.shelter_homes, 0) AS shelter_homes
FROM hospital_merge h
LEFT JOIN food_merge f ON h.zone_no = f.zone_no
LEFT JOIN shelter_merge s ON h.zone_no = s.zone_no

UNION

SELECT 
    COALESCE(h.zone_no, f.zone_no, s.zone_no) AS zone_no,
    COALESCE(h.hospital_count, 0),
    COALESCE(f.food_distribution_points, 0),
    COALESCE(s.shelter_homes, 0)
FROM food_merge f
LEFT JOIN hospital_merge h ON f.zone_no = h.zone_no
LEFT JOIN shelter_merge s ON f.zone_no = s.zone_no

UNION

SELECT 
    COALESCE(h.zone_no, f.zone_no, s.zone_no) AS zone_no,
    COALESCE(h.hospital_count, 0),
    COALESCE(f.food_distribution_points, 0),
    COALESCE(s.shelter_homes, 0)
FROM shelter_merge s
LEFT JOIN hospital_merge h ON s.zone_no = h.zone_no
LEFT JOIN food_merge f ON s.zone_no = f.zone_no
ORDER BY zone_no);

-- Count how many types of hospitals are available per zone
CREATE TABLE hospType_zone AS(
WITH hospital_with_id AS (
    SELECT 
        *,
        DENSE_RANK() OVER (ORDER BY site_name) AS hospital_id
    FROM hospital_locations
)
, hospital_zone_map AS (
    SELECT
        h.hospital_id,
        h.types_of_hos,
        h.site_name,
        h.latitude AS hospital_lat,
        h.longitude AS hospital_long,
        b.zone_no,
        b.latitude AS zone_lat,
        b.longitude AS zone_long,
        SQRT(POW(h.latitude - b.latitude, 2) + POW(h.longitude - b.longitude, 2)) AS distance
    FROM hospital_with_id h
    JOIN municipal_boundaries_clean b ON 1=1
)    
, hospital_to_zone AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY hospital_id ORDER BY distance) AS rn
        FROM hospital_zone_map
    ) sub
    WHERE rn = 1
)
SELECT
    zone_no,
	upper(types_of_hos),
    COUNT(*) AS hospital_count
FROM hospital_to_zone
GROUP BY zone_no, types_of_hos
ORDER BY zone_no);

