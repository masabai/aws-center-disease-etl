# Databricks notebook source
# Extract Chronic Disease datasets from S3 CSV files into Spark DataFrame
df_chronic = spark.read.option("header", "true").csv("s3://center-disease-control/raw/Chronic_Disease.csv")

# Extract Heart datasets from S3 CSV files into Spark DataFrame
df_heart = spark.read.option("header", "true").csv("s3://center-disease-control/raw/Heart_Disease.csv")

# Extract Heart datasets from S3 CSV files into Spark DataFrame
df_nutri = spark.read.option("header", "true").csv("s3://center-disease-control/raw/Nutrition.csv")
