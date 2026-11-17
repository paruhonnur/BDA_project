import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from datetime import datetime
from io import BytesIO

import requests
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, desc


# --------------------------------------------------
# 1. GET DATE FROM USER
# --------------------------------------------------

date_input = input("Enter date (YYYY-MM-DD): ").strip()

try:
    selected_date = datetime.strptime(date_input, "%Y-%m-%d")
except ValueError:
    print("Invalid date! Please use YYYY-MM-DD.")
    sys.exit(1)

year = selected_date.strftime("%Y")
month = selected_date.strftime("%m")

URL = (
    "https://raw.githubusercontent.com/"
    "kunal-rajput-317/mandi-data/main/"
    f"data/{year}/{month}/{date_input}.csv"
)

print("\nFetching mandi data for:", date_input)
print("URL:", URL)


# --------------------------------------------------
# 2. DOWNLOAD MANDI DATA
# --------------------------------------------------

print("\nDownloading mandi data...")

try:
    response = requests.get(URL, timeout=30)
    response.raise_for_status()

    pdf = pd.read_csv(BytesIO(response.content))

except requests.exceptions.RequestException as e:
    print("Could not download the data.")
    print("The selected date may not exist in the dataset.")
    print("Error:", e)
    sys.exit(1)

except (pd.errors.ParserError, pd.errors.EmptyDataError) as e:
    print("Could not read the downloaded CSV file.")
    print("Error:", e)
    sys.exit(1)

print("Downloaded records:", len(pdf))


# --------------------------------------------------
# 3. CHECK REQUIRED COLUMNS
# --------------------------------------------------

required_columns = [
    "date",
    "state",
    "district",
    "market",
    "commodity",
    "variety",
    "grade",
    "min_price",
    "max_price",
    "modal_price"
]

missing_columns = [
    column for column in required_columns
    if column not in pdf.columns
]

if missing_columns:
    print("Required columns are missing:", missing_columns)
    print("Available columns:", list(pdf.columns))
    sys.exit(1)


# --------------------------------------------------
# 4. FILTER KARNATAKA DATA
# --------------------------------------------------

pdf["state"] = pdf["state"].astype(str).str.strip()

karnataka_pdf = pdf[
    pdf["state"].str.lower() == "karnataka"
].copy()

if karnataka_pdf.empty:
    print("No Karnataka records found for this date.")
    sys.exit(0)

print("Karnataka records:", len(karnataka_pdf))

print("\nSample Karnataka records:")
print(karnataka_pdf.head())


# --------------------------------------------------
# 5. CREATE OUTPUT FOLDER
# --------------------------------------------------

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# --------------------------------------------------
# 6. SAVE KARNATAKA MANDI DATA
# --------------------------------------------------

karnataka_csv = os.path.join(
    OUTPUT_DIR,
    "karnataka_mandi_prices.csv"
)

karnataka_pdf.to_csv(karnataka_csv, index=False)


# --------------------------------------------------
# 7. START APACHE SPARK
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Karnataka Mandi Price Analysis")
    .config(
        "spark.sql.execution.arrow.pyspark.enabled",
        "false"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    # Convert Pandas DataFrame to Spark DataFrame
    df = spark.createDataFrame(karnataka_pdf)

    # Convert prices to numeric values
    for price_column in [
        "min_price",
        "max_price",
        "modal_price"
    ]:
        df = df.withColumn(
            price_column,
            col(price_column).cast("double")
        )

    # Remove records without valid modal prices
    df = df.filter(col("modal_price").isNotNull())

    if df.limit(1).count() == 0:
        print("No valid modal prices found.")
        sys.exit(0)

    # --------------------------------------------------
    # 8. TOP CROPS BY AVERAGE MODAL PRICE
    # --------------------------------------------------

    crop_summary = (
        df.groupBy("commodity")
        .agg(
            avg("modal_price").alias("Average_Modal_Price"),
            count("*").alias("Record_Count")
        )
        .orderBy(desc("Average_Modal_Price"))
    )

    print("\nTOP CROPS BY AVERAGE MODAL PRICE")
    crop_summary.show(15, truncate=False)

    crop_summary.toPandas().to_csv(
        os.path.join(OUTPUT_DIR, "crop_price_summary.csv"),
        index=False
    )

    # --------------------------------------------------
    # 9. MARKET PRICE COMPARISON
    # --------------------------------------------------

    market_comparison = (
        df.groupBy("commodity", "district", "market")
        .agg(
            avg("modal_price").alias("Average_Modal_Price"),
            count("*").alias("Record_Count")
        )
        .orderBy("commodity", "district", "market")
    )

    print("\nMARKET PRICE COMPARISON")
    market_comparison.show(20, truncate=False)

    market_comparison.toPandas().to_csv(
        os.path.join(OUTPUT_DIR, "market_price_comparison.csv"),
        index=False
    )

    # --------------------------------------------------
    # 10. CREATE TOP 10 CROPS PRICE GRAPH
    # --------------------------------------------------

    top_10_pdf = crop_summary.limit(10).toPandas()

    if not top_10_pdf.empty:
        plt.figure(figsize=(12, 6))

        plt.bar(
            top_10_pdf["commodity"],
            top_10_pdf["Average_Modal_Price"]
        )

        plt.xlabel("Crop")
        plt.ylabel("Average Modal Price")
        plt.title(
            f"Top 10 Crop Prices in Karnataka - {date_input}"
        )

        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        plt.savefig(
            os.path.join(OUTPUT_DIR, "top_10_crop_prices.png"),
            dpi=300
        )

        plt.close()

    print("\nPROJECT COMPLETED SUCCESSFULLY!")
    print("Selected date:", date_input)
    print("Open the output folder to view:")

    print("- karnataka_mandi_prices.csv")
    print("- crop_price_summary.csv")
    print("- market_price_comparison.csv")
    print("- top_10_crop_prices.png")

finally:
    spark.stop()

