import org.apache.spark.sql._
import org.apache.spark.sql.types._

val dataPath = "/home/mac/IdeaProjects/spark-iceberg-test/data/"
val masterTable = "iceberg.db.operations"

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

///////////
// DDL

def createTempTable(df: DataFrame, tableName: String) = {
  dropTempTable(tableName)
  df.writeTo(tableName).using("iceberg").create()
}

def dropTempTable(tableName: String) = {
  spark.sql(s"DROP TABLE IF EXISTS $tableName")
}

def ddl(): Any = {
  // Creating EMPTY table manually
  spark.sql(s"CREATE TABLE IF NOT EXISTS $masterTable (fn_periodo integer, cod_operacion integer, deuda_neta integer) USING iceberg")
}

///////////
// DML

def dml(fn_periodo: Int) = {
  val tempTable = s"${masterTable}_rect_${fn_periodo}"

  val operationsRectDf = readOperationsRect(fn_periodo)
  createTempTable(operationsRectDf, tempTable)

  spark.sql(s"""
  MERGE INTO $masterTable t
  USING (SELECT * from $tempTable) s
  ON t.fn_periodo = s.fn_periodo_rect and t.cod_operacion = s.cod_operacion
  WHEN MATCHED THEN UPDATE SET t.deuda_neta = s.deuda_neta
  WHEN NOT MATCHED THEN INSERT *
  """)

}

