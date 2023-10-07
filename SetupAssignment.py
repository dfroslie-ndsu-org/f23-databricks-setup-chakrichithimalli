# Databricks notebook source

storage_end_point = "chakristorageaccount.dfs.core.windows.net" 
my_scope = "Chakri-ADB"
my_key = "ChakriADB-key"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

# Replace the container name (assign-1-blob) and storage account name (assign1storage) in the uri.
uri = "abfss://chakricontainer@chakristorageaccount.dfs.core.windows.net/"


# Read the data file from the storage account.  This the same datafile used in assignment 1.  It is also available in the InputData folder of this assignment's repo.
sp_df = spark.read.csv(uri+'SandP500Daily.csv', header=True)

# Creating a new column with the range for the day.
sp_range_df = sp_df.withColumn('Range', sp_df.High - sp_df.Low)

# Save this range file to a single CSV.  Use coalesce to output it to a single file.
sp_range_df.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"output/Range")

# Use the range from the previous cells to find the percent change for each day.  Use the Open column for the denominator.
# Percent change calculation using Range, Open values
percent_value = (sp_range_df.Range/sp_range_df.Open) * 100

# Creating column with percent change value
sp_range_df_percent_change = sp_range_df.withColumn('Percent change', percent_value )

# Sorting the dataset descending based on the percent change (High to Low).
sorted_df_percent_change=sp_range_df_percent_change.orderBy('Percent Change', ascending = False)

# Saving the file to a single CSV file to your storage account to a single CSV file in the location output/PercentChange.
sorted_df_percent_change.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"output/PercentChange")
