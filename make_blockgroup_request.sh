curl --form addressFile=@sample_address_input.csv --form vintage=4 --form benchmark=4 https://geocoding.geo.census.gov/geocoder/geographies/addressbatch --output geocoderesult.csv
# Input file format:
# unique_id,street_address_with_number,city,state,zip
# Response format:
# id,street_address_with_number,city,state,zip,match_or_not,match_type,canonical_address_city_state_zip,latitude,longitude,some_id_number,some_letter_code,census_state_id,census_county_id,census_tract,census_block
