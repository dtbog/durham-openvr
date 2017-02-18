-- curl https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvhis32.zip > ncvhis32.zip
-- unzip ncvhis32.zip

DROP TABLE IF EXISTS voter_history_feb_10;
CREATE TABLE voter_history_feb_10 (
  county_id SMALLINT,
  count_desc VARCHAR(55),
  voter_reg_num VARCHAR(15),
  election_lbl DATE,
  election_desc VARCHAR(55),
  voting_method VARCHAR(55),
  voted_party_code VARCHAR(5),
  voted_party_desc VARCHAR(55),
  precinct_label VARCHAR(10),
  precinct_desc VARCHAR(55),
  ncid VARCHAR(15),
  voted_county_id SMALLINT,
  voted_county_desc VARCHAR(55),
  vtd_precinct_label VARCHAR(10),
  vtd_precinct_desc VARCHAR(55)
);

COPY voter_history_feb_10 from '/Users/dave/repos/durham-openvr/ncvhis32.txt' with csv delimiter '	' header;