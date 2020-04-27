// --------------------------------
// Q.2 (a)
// --------------------------------

// Load library object
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// Read the parquet file into dataframe
val df = sqlContext.read.parquet("Documents/Datasets/flight_2008_pq.parquet")

// Show the content of dataframe
df.show

// Create a temporary view of the table on memory
df.createOrReplaceTempView("Flights")

// fetch the maximum data of DepDelay column
val result = spark.sql("select FlightNum, max(CAST(DepDelay AS INT)) from Flights where DepDelay != 'NA' group by FlightNum")

// Print the result
result.show()

// fetch the maximum data of DepDelay column in descending order
val result_desc = spark.sql("select FlightNum, max(CAST(DepDelay AS INT)) as maxdepdelay from Flights where DepDelay != 'NA' group by FlightNum order by maxdepdelay desc")

// Print the result
result_desc.show()
