from src.bronze import run_bronze
from src.silver import run_silver
from src.gold import run_gold

# Databricks provides the SparkSession as `spark`.

run_bronze(spark)
run_silver(spark)

# Supply the order years affected by this ingestion batch.
affected_years = [2016, 2017]

results = run_gold(spark, affected_years)

for title, df in results.items():
    print(title)
    display(df)