from pyspark.sql import SparkSession

# create the spark session with the name 'demo_spark'
spark = SparkSession.builder.appName('demo_spark').getOrCreate()
# read from the .csv file that holds the data
df = spark.read.csv('pipeline_demo.csv', header=True, inferSchema=True)

# show the schema and the table of pipline_demo.csv
df.printSchema()
df.show()

# show all female smokers
F_smokers = df.where((df.smoker == 'YES') & (df.sex == 'F'))
print("Female smokers")
F_smokers.show()

# show all bills that are more than $55 and the party size is less than or equal to 4
total_party = df.where((df.total_bill >= '55') & (df.party_size <= '4'))
print("Bills that are more than $55 and the party size is less than or equal to 4")
total_party.show()
