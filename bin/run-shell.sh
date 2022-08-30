 #! /bin/bash

SPARK_VERSION=3.2
SCALA_VERSION=2.12
ICEBERG_VERSION=0.13.2

./spark-shell \
   --packages org.apache.iceberg:iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}:${ICEBERG_VERSION} \
   --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
   --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
   --conf spark.sql.catalog.spark_catalog.type=hive \
   --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
   --conf spark.sql.catalog.iceberg.type=hadoop \
   --conf spark.sql.catalog.iceberg.warehouse=/home/mac/datalake/warehouse
