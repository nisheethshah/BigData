// --------------------------------
// Q.1 (a)
// --------------------------------
// Read csv in dataframe
val df = spark.read.format("csv").option("header","true").load("Documents/Datasets/simple.csv")

df.show()

// --------------------------------
// Q.1 (b)
// --------------------------------
// Print Schema
df.printSchema()

// --------------------------------
// Q.1 (c)
// --------------------------------
// Convert df to RDD
val myRDD = df.select($"a",$"b",$"c")

// Print RDD
myRDD.collect().foreach(println)

// --------------------------------
// Q.1 (d)
// --------------------------------
// Save filename in variable
val filename = "spark-2.2.1-bin-hadoop2.7/README.md"

// Read textfile
val textData = spark.read.textFile(filename)

// Convert data to RDD
val RDDData = textData.rdd


// --------------------------------
// Q.1 (e)
// --------------------------------
// Fetch length of each line and print
val line_length = textData.flatMap(_.split("\n")).map(li => (li,li.length))
line_length.collect().foreach(println)


// --------------------------------
// Q.1 (f)
// --------------------------------
// Save filename in variable
val filename = "spark-2.2.1-bin-hadoop2.7/README.md"

// Save each line in list
val listOfLines = scala.io.Source.fromFile(filename).getLines.toList

// Split lines by space for each word
val words = listOfLines.flatMap(_.split(" "))

// Show the counts
words.groupBy((word:String) => word)
words.groupBy((word:String) => word).mapValues(_.length)


// --------------------------------
// Q.1 (g)
// --------------------------------
// Read textfile
val text = sc.textFile(filename)

// Use reduction operation
val counts = text.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
// "reduceByKey" helps to keep adding the values for the same key. Additionally, it performs merging locally using reduce function and than sends records across the partitions for preparing the final results.
counts.collect().foreach(println)


// --------------------------------
// Q.1 (g)
// --------------------------------
// Recursive factorial fucntion
def factorial(n: Int): Int = {
      if (n <= 1)
         1
      else
      n * factorial(n - 1)
   }

// Integer Array
val X = Array(1, 2, 3, 4, 5)

// Initialise sum of factorials
var sum=0

// For loop for to find factorial of each element of X and sum up all
for (e <- X) {
  sum = sum + factorial(e)
}

// Print the sum of factorials
println(sum)

