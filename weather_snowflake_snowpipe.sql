
-- Create Database
	CREATE DATABASE weather_db;


--Create S3 Integration
	CREATE OR REPLACE STORAGE INTEGRATION s3_init
		TYPE = EXTERNAL_STAGE
		STORAGE_PROVIDER = 'S3'
		ENABLED = TRUE
		STORAGE_AWS_ROLE_ARN = '<iam_role>'
		STORAGE_ALLOWED_LOCATIONS = 's3://weather-etl-project-sakshi/'
		COMMENT = 'Creating connection to S3'
	
--Describe Storage Integration
	DESC integration s3_init
	
		
-- Create file format Object
	CREATE OR REPLACE FILE FORMAT csv_file
		TYPE = CSV
		FIELD_DELIMITER = ','
		SKIP_HEADER = 1
		NULL_IF = ('NULL','null')
		EMPTY_FIELD_AS_NULL = TRUE ;
		
--Create Stage
	CREATE OR REPLACE STAGE weather_stage
		URL = 's3://weather-etl-project-sakshi/transformed_data/'
		STORAGE_INTEGRATION = s3_init
		FILE_FORMAT = csv_file
		
		
--List the files in s3 storage 		
	LIST @weather_stage
	
	
--Create Tables tbl_astro, tbl_city and tbl_hourly_forecast
	
	CREATE OR REPLACE TABLE weather_db.tbl_hourly_forecast (
		Date DATE,
		Time TIME,
		City STRING,
		Temperature INT,
		Condition STRING,
		Wind_speed INT,
		Humidity INT,
		Cloud INT,
		Feels_like INT,
		UV INT,
		Wind_direction STRING,
		Visibility INT );
		
		
	CREATE OR REPLACE TABLE weather_db.tbl_city (
		City STRING,
		Region STRING,
		Country STRING,
		Latitude STRING,
		Longitude STRING,
		Timezone STRING,
		Local_time DATETIME );
		
		
	CREATE OR REPLACE TABLE weather_db.tbl_astro (
		Date DATE,
		City STRING,
		Sunrise TIME,
		Sunset TIME,
		Moonrise TIME,
		Moonset TIME,
		Moon_phase STRING );
		
	
--Create Snow Pipe to Ingest data Automatically
	CREATE OR REPLACE PIPE weather_db.pipe.tbl_city_pipe
		auto_ingest = TRUE
		AS
		COPY INTO weather_db.public.tbl_city
		FROM @weather_db.public.weather_stage/city/ ;
		
		
	CREATE OR REPLACE PIPE weather_db.pipe.tbl_astro_pipe
		auto_ingest = TRUE
		AS
		COPY INTO weather_db.public.tbl_astro
		FROM @weather_db.public.weather_stage/astro/;
		
		
	CREATE OR REPLACE PIPE weather_db.pipe.tbl_hourly_forecast_pipe
		auto_ingest = TRUE
		AS
		COPY INTO weather_db.public.tbl_hourly_forecast
		FROM @weather_db.public.weather_stage/hourly_forecast/ ;
		
		
--Describe Snow Pipe
	DESC PIPE pipe.tbl_hourly_forecast_pipe
		
		
--get status of the Pipe
	SELECT SYSTEM$PIPE_STATUS('pipe.tbl_hourly_forecast_pipe')
		
		