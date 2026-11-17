import os
from io import BytesIO

import requests
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum as spark_sum, col

# Online agricultural dataset
URL = (
    "https://raw.githubusercontent.com/"
    "dibyendubiswas1998/Crop-Production-Analysis/"
    "main/DATA/crop_production.csv"
)

# Create output folder
os.makedirs("output", exist_ok=True)

print("Connecting to online agricultural data...")

# Download the CSV directly from the internet
response = requests.get(URL, timeout=60)
response.raise_for_status()

# Load the downloaded CSV into pandas
data = pd.read_csv(BytesIO(response.content))

print("Dataset downloaded successfully!")
print("Total records:", len(data))

# Clean text columns
for column in ["State_Name", "District_Name", "Crop", "Season"]:
    data[column] = data[column].astype("string").str.strip()

# Select Karnataka records
data = data[
    data["State_Name"].str.lower() == "karnataka"
].copy()

if data.empty:
    raise ValueError("No Karnataka records were found.")

# Save the filtered dataset
data.to_csv("output/karnataka_crop_data.csv", index=False)

print("\nKARNATAKA AGRICULTURAL DATA")
print("Karnataka records:", len(data))
print(data.head(10).to_string(index=False))

# Start Apache Spark
spark = (
    SparkSession.builder
    .appName("KarnatakaCropProductionAnalysis")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    # Load Karnataka data into Spark
    spark_df = spark.read.csv(
        "output/karnataka_crop_data.csv",
        header=True,
        inferSchema=True
    )

    # Remove rows with missing or invalid area/production
    spark_df = spark_df.filter(
        col("Area").isNotNull()
        & col("Production").isNotNull()
        & (col("Area") > 0)
        & (col("Production") >= 0)
    )

    print("\nDATA PROCESSED USING APACHE SPARK")
    spark_df.show(10, truncate=False)

    # 1. Crop production summary
    crop_summary = spark_df.groupBy("Crop").agg(
        spark_sum("Production").alias("Total_Production"),
        avg("Production").alias("Average_Production")
    ).orderBy(
        col("Total_Production").desc()
    )

    print("\nTOP CROPS BY TOTAL RECORDED PRODUCTION")
    crop_summary.show(15, truncate=False)

    crop_pd = crop_summary.toPandas()
    crop_pd.to_csv(
        "output/crop_production_summary.csv",
        index=False
    )

    # 2. Year-wise total production
    yearly = spark_df.groupBy("Crop_Year").agg(
        spark_sum("Production").alias("Total_Production")
    ).orderBy("Crop_Year")

    yearly_pd = yearly.toPandas()
    yearly_pd.to_csv(
        "output/yearly_production.csv",
        index=False
    )

    # 3. Graph: production by year
    plt.figure(figsize=(10, 5))
    plt.plot(
        yearly_pd["Crop_Year"],
        yearly_pd["Total_Production"],
        marker="o"
    )
    plt.title("Karnataka Crop Production by Year")
    plt.xlabel("Year")
    plt.ylabel("Recorded production (dataset units)")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("output/yearly_production.png")
    plt.close()

    # 4. Graph: top 10 crops
    top_crops = crop_pd.head(10).sort_values(
        "Total_Production"
    )

    plt.figure(figsize=(10, 6))
    plt.barh(
        top_crops["Crop"],
        top_crops["Total_Production"]
    )
    plt.title("Top 10 Crops by Recorded Production in Karnataka")
    plt.xlabel("Recorded production (dataset units)")
    plt.ylabel("Crop")
    plt.tight_layout()
    plt.savefig("output/top_10_crops.png")
    plt.close()

    # 5. District-wise production
    district_summary = spark_df.groupBy("District_Name").agg(
        spark_sum("Production").alias("Total_Production")
    ).orderBy(
        col("Total_Production").desc()
    )

    district_summary.toPandas().to_csv(
        "output/district_production_summary.csv",
        index=False
    )

    print("\nPROJECT COMPLETED SUCCESSFULLY!")
    print("Check the output folder for:")
    print("- karnataka_crop_data.csv")
    print("- crop_production_summary.csv")
    print("- yearly_production.csv")
    print("- district_production_summary.csv")
    print("- yearly_production.png")
    print("- top_10_crops.png")

finally:
    spark.stop()

