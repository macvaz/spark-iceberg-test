import org.apache.spark.sql._
import org.apache.spark.sql.types._

val dataPath = "/home/mac/IdeaProjects/spark-iceberg-test/data/"

val schemaClose = StructType(Array(
  StructField("fn_periodo", IntegerType, true),
  StructField("cod_operacion", IntegerType, true),
  StructField("deuda_neta", IntegerType, true)
))

val schemaRect = StructType(Array(
  StructField("fn_periodo", IntegerType, true),
  StructField("cod_operacion", IntegerType, true),
  StructField("fn_periodo_rect", IntegerType, true),
  StructField("deuda_neta", IntegerType, true)
))

def readCsv(schema: StructType, dataPath: String): DataFrame = {
  spark.read
    .format("csv")
    .option("sep", ",")
    .option("inferSchema", "false")
    .option("header", "true")
    .schema(schema)
    .load(dataPath)
}

def readOperationsClose(fn_periodo: Int): DataFrame = {
  readCsv(schemaClose, dataPath + s"operations-close/$fn_periodo/operations.csv")
}

def readOperationsRect(fn_periodo: Int): DataFrame = {
  readCsv(schemaRect, dataPath + s"operations-rect/$fn_periodo/operations-rect.csv")
}


def ddl() = {

  // Not working yet
  spark.sql("CREATE TABLE IF NOT EXISTS iceberg.db.states (name string) USING iceberg")
  df.writeTo("iceberg.db.states").append()

  spark.read.table("iceberg.db.states").show

  spark.sql("""
  MERGE INTO iceberg.db.states d 
  USING (SELECT * from iceberg.db.states) s   
  ON s.name = d.name
  WHEN MATCHED THEN UPDATE SET d.name = 1
  WHEN NOT MATCHED THEN INSERT *
  """)

}

