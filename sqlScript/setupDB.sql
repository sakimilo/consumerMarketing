DROP TABLE IF EXISTS business_vertical_tbl;
DROP TABLE IF EXISTS channel_name_tbl;
DROP TABLE IF EXISTS country_tbl;
DROP TABLE IF EXISTS camp_data_tbl;

CREATE TABLE business_vertical_tbl
(
	business_vertical_id int NOT NULL,
  	business_vertical character varying(200)
);

CREATE TABLE channel_name_tbl
(
	channel_name_id int NOT NULL,
  	channel_name character varying(200)
);

CREATE TABLE country_tbl
(
	country_id int NOT NULL,
  	country character varying(200)
);

CREATE TABLE camp_data_tbl
(
	capturedDate DATE,
	business_vertical_id int,
	country_id int,
	region character varying(200),
	city_code character varying(200),
	strategy_id int,
	channel_name_id int,
	goal_type double precision,
	total_spend_cpm double precision, 
	impressions double precision,
	clicks double precision,
	conversions double precision
);

\copy business_vertical_tbl(business_vertical_id, business_vertical) FROM 'C:\Users\sakimilo\Documents\Projects\Assessment\data\business_vertical.csv' DELIMITER ',' CSV HEADER;
\copy channel_name_tbl(channel_name_id, channel_name) FROM 'C:\Users\sakimilo\Documents\Projects\Assessment\data\channel_name.csv' DELIMITER ',' CSV HEADER;
\copy country_tbl(country_id, country) FROM 'C:\Users\sakimilo\Documents\Projects\Assessment\data\country.csv' DELIMITER ',' CSV HEADER;
\copy camp_data_tbl(capturedDate, business_vertical_id, country_id, region, city_code, strategy_id, channel_name_id, goal_type, total_spend_cpm, impressions, clicks, conversions) FROM 'C:\Users\sakimilo\Documents\Projects\Assessment\data\camp_data.csv' DELIMITER ',' CSV HEADER;