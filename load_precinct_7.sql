-- End-to-end script to load precinct data, openaddresses data, and pull voter list for a precinct

-- https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter32.zip  Durham is district 32.
-- curl https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter32.zip > ncvoter32.zip
-- unzip ncvoter32.zip
-- convert the file:  iconv -f UTF-8 -t UTF-8 -c ncvoter32.txt > ncvoter32_feb10.tsv

drop table if exists voters_feb_10 cascade;
create table voters_feb_10 ( 
  county_id SMALLINT,
  county VARCHAR(255),
  voter_id VARCHAR(55),
  status VARCHAR(10),
  status_desc VARCHAR(55),
  reason VARCHAR(10),
  reason_desc VARCHAR(255),
  absentee_indication VARCHAR(10),
  name_prefix_code VARCHAR(10),
  last_name VARCHAR(55),
  first_name VARCHAR(55),
  middle_name VARCHAR(55),
  name_suffix_label VARCHAR(10),
  residential_address VARCHAR(255),
  residential_city VARCHAR(55),
  residential_state VARCHAR(3),
  residential_zip_code VARCHAR(12),
  mailing_address_1 VARCHAR(255),
  mailing_address_2 VARCHAR(255),
  mailing_address_3 VARCHAR(255),
  mailing_address_4 VARCHAR(255),
  mailing_address_city VARCHAR(255),
  mailing_address_state VARCHAR(3),
  mailing_address_zip_code VARCHAR(12),
  full_phone_number VARCHAR(16),
  race_code VARCHAR(10),
  ethnic_code VARCHAR(10),
  party_code VARCHAR(10),
  gender_code VARCHAR(10),
  birth_age SMALLINT,
  birth_state VARCHAR(3),
  drivers_license_id VARCHAR(20),
  registration_date DATE,
  precinct_abbreviation VARCHAR(10),
  precinct_description VARCHAR(55),
  municipality_abbreviation VARCHAR(10),
  municipality_full VARCHAR(55),
  ward_abbreviation VARCHAR(10),
  ward_full VARCHAR(55),
  congressional_district_abbreviation VARCHAR(10),
  supreme_court_district_abbreviation VARCHAR(10),
  judicial_district_abbreviation VARCHAR(10),
  nc_senate_district_abbreviation VARCHAR(10),
  nc_house_district_abbreviation VARCHAR(10),
  county_commissioner_abbreviation VARCHAR(10),
  county_commissioner_full VARCHAR(55),
  township_abbreviation VARCHAR(10),
  township_full VARCHAR(55),
  school_district_abbrev VARCHAR(10), --- "school_dist_abbrv"
  school_district_full VARCHAR(55), --- "school_dist_desc"
  fire_district_abbrev VARCHAR(10), --- "fire_dist_abbrv"
  fire_district_full VARCHAR(55), --- "fire_dist_desc"
  water_district_abbrev VARCHAR(10), --- "water_dist_abbrv"
  water_district_full VARCHAR(55), --- "water_dist_desc"
  sewer_district_abbrev VARCHAR(10), --- "sewer_dist_abbrv"
  sewer_district_full VARCHAR(55), --- "sewer_dist_desc"
  sanitation_district_abbrev VARCHAR(10), --- "sanit_dist_abbrv"
  sanitation_district_full VARCHAR(55), --- "sanit_dist_desc"
  rescue_district_abbrev VARCHAR(10), --- "rescue_dist_abbrv"
  rescue_district_full VARCHAR(55), --- "rescue_dist_desc"
  municipal_district_abbrev VARCHAR(10), --- "munic_dist_abbrv"
  municipal_district_full VARCHAR(55), --- "munic_dist_desc"
  district_1_abbrev VARCHAR(10), --- "dist_1_abbrv"
  district_1_full VARCHAR(55), --- "dist_1_desc"
  district_2_abbrev VARCHAR(10), --- "dist_2_abbrv"
  district_2_full VARCHAR(55), --- "dist_2_desc"
  confidential_id VARCHAR(20), --- "confidential_ind"
  age_string VARCHAR(55), --- "age"
  ncid VARCHAR(10), --- "ncid"
  vtd_abbrev VARCHAR(10), --- "vtd_abbrv"
  vtd_full VARCHAR(55) --- "vtd_desc"
);

COPY voters_feb_10 from '/Users/dave/cfd/ncvoter32_feb10.tsv' with csv delimiter '	' header;

-- Now we have all the recent voter records.  Now get the OpenAddresses data:

-- http://results.openaddresses.io/sources/us/nc/durham
-- https://s3.amazonaws.com/data.openaddresses.io/runs/155716/us/nc/durham.zip
-- unzip durham.zip
-- cp us/nc/durham.csv ./

DROP TABLE IF EXISTS openaddresses_feb_10 CASCADE;
CREATE TABLE openaddresses_feb_10 (
  gps_lat FLOAT,
  gps_long FLOAT,
  address_number VARCHAR(15),
  address_street VARCHAR(255),
  address_suffix VARCHAR(15),
  city_abbreviated VARCHAR(255),
  district VARCHAR(15),
  region VARCHAR(15),
  zip VARCHAR(15),
  id VARCHAR(55),
  hash VARCHAR(16)
 );

 COPY openaddresses_feb_10 from '/Users/dave/cfd/durham.csv' with csv header;

-- Modify openaddresses to remove apartments and non-integer nonsense
DROP VIEW IF EXISTS openaddresses_feb_10_no_apartments CASCADE;
 CREATE VIEW openaddresses_feb_10_no_apartments AS
   SELECT distinct(address_number || '  ' || address_street) address_full_no_apartment,
   regexp_replace(address_number, '\s+.*$', '')::integer address_number_cleaned,
   address_street address_street_cleaned
 FROM openaddresses_feb_10;

-- Create precinct 7 view
DROP VIEW IF EXISTS voters_p7 CASCADE;
CREATE VIEW voters_p7 AS (
  SELECT 
    voters_feb_10.*,
    regexp_replace(
	  regexp_replace(
        regexp_replace(
        	residential_address, '\s+\#.+$', ''),
        '^\d+\s+', ''),
      '\s+$', '')  street_name_cleaned,
    regexp_replace(
   	  	  left(residential_address, position(' ' in residential_address)), 
   	  	'[a-zA-Z]+\s*$', '')::integer street_number_cleaned,
    regexp_replace(
    	regexp_replace(residential_address, '\s+\#.+$', ''),
    	  '\s+$', '') residential_address_no_apartment
  FROM voters_feb_10 
  WHERE precinct_abbreviation = '07' AND residential_address NOT LIKE '%CONFIDENTIAL%'
);

-- Find bounding addresses for precinct streets
CREATE VIEW p7_bounds AS (
  SELECT min(street_number_cleaned), max(street_number_cleaned), street_name_cleaned
  FROM voters_p7
  GROUP BY 3 ORDER BY 3 ASC);

-- All OpenAddresses within p7 bounds
DROP TABLE IF EXISTS openaddresses_p7;
CREATE TABLE openaddresses_p7 AS (
	SELECT address_full_no_apartment precinct_7_address,
	       address_number_cleaned,
	       address_street_cleaned
	FROM 
	  openaddresses_feb_10_no_apartments oa
	RIGHT JOIN 
	  p7_bounds
	ON 
	  oa.address_street_cleaned = p7_bounds.street_name_cleaned
	  AND address_number_cleaned <= p7_bounds.max
	  AND address_number_cleaned >= p7_bounds.min
);

-- Now, join to get all addresses with voter count
DROP TABLE IF EXISTS p7_addresses_summary;
CREATE TABLE p7_addresses_summary AS (
SELECT
  precinct_7_address,
  address_number_cleaned,
  address_street_cleaned,
  count(voters.*)
FROM
  openaddresses_p7
LEFT JOIN
  (select * from voters_p7 where status_desc = 'ACTIVE') voters
ON openaddresses_p7.precinct_7_address = (voters.street_number_cleaned || '  ' || voters.street_name_cleaned)
GROUP BY 1, 2, 3 ORDER BY 3,2
);
COPY p7_addresses_summary to '/Users/dave/Desktop/p7.csv' DELIMITER ',' CSV;





