import org.apache.spark.sql.types._

val rawDataDir = "/home/mac/datalake/raw/states.txt"
val schema = StructType(Array(
  StructField("name", StringType, true))
)

val df = spark.read
  .format("csv")
  .option("sep", ",")
  .option("inferSchema", "false")
  .option("header", "true")
  .schema(schema)
  .load(rawDataDir)

df.show()


spark.sql("CREATE TABLE IF NOT EXISTS iceberg.db.states (name string) USING iceberg")
df.writeTo("iceberg.db.states").append()

spark.read.table("iceberg.db.states").show