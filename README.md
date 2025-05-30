# Data Engineering Weather Data Analysis Project

## **Overview**  
  This project aims to securely manage, streamline, and perform analysis on the structured and semi-structured weather data based on the cities in India  

## **Project Goals**  
  Data Ingestion — Build a mechanism to ingest data from different sources  
  ETL System — We are getting data in raw format, transforming this data into the proper format  
  Data lake — We will be getting data from multiple sources so we need centralized repo to store them  
  Scalability — As the size of our data increases, we need to make sure our system scales with it  
  Cloud — We can’t process vast amounts of data on our local computer so we need to use the cloud, in this case, we will use AWS  
   

## **Services we will be using**  
  Amazon S3: Amazon S3 is an object storage service that provides manufacturing scalability, data availability, security, and performance.  
  AWS IAM: This is nothing but identity and access management which enables us to manage access to AWS services and resources securely.  
  AWS Glue: A serverless data integration service that makes it easy to discover, prepare, and combine data for analytics, machine learning, and application development.  
  AWS Lambda: Lambda is a computing service that allows programmers to run code without creating or managing servers.  
  Snowflake: Transformed data from Amazon S3 is automatically loaded into Snowflake using Snowpipe, which provides real-time or near-real-time ingestion. Snowflake efficiently stores the weather data for querying              and analysis.


## **API used**    
Rapid API's Weather API is the external data source providing historical weather data (e.g., temperature, humidity, wind speed, etc.).

## **Architecture Diagram**    
![WeatherProj_Architecture](https://github.com/user-attachments/assets/ddeff797-f8ff-488e-b4ba-447fd55f1619)
