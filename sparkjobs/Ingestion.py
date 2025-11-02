from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, array, lit, expr
from pyspark.sql.types import StructType, StringType
import sys

# Validate args
if len(sys.argv) < 2:
    print("Usage: spark-submit Ingestion.py <KafkaTopic1> <KafkaTopic2> ...")
    sys.exit(1)

# Define schemas for known topics
schemas = {
    "FactDeclaration": StructType()
        .add("ID", StringType())
        .add("NumAff", StringType())
        .add("NumImm", StringType())
        .add("IDPeriode", StringType())
        .add("MontantDeclaration", StringType()),

    "DimAssure": StructType()
        .add("NumImm", StringType())
        .add("Nom", StringType())
        .add("Prenom", StringType())
        .add("CIN", StringType())
        .add("Adresse", StringType())
        .add("Tel", StringType())
        .add("Status", StringType()),

    "DimAffilie": StructType()
        .add("NumAffilie", StringType())
        .add("RaisonSociale", StringType())
        .add("FormeJuridique", StringType())
        .add("ICE", StringType())
        .add("RegCom", StringType())
        .add("Patente", StringType())
        .add("AffilieType", StringType())
        .add("Activite", StringType())
        .add("DateCreation", StringType())
        .add("DateAffiliation", StringType())
        .add("RepresantantNom", StringType())
        .add("RepresantantPrenom", StringType())
        .add("RepresantantCIN", StringType())
        .add("Deleg", StringType())
        .add("Ville", StringType())
        .add("CodeDeleg", StringType())
        .add("Status", StringType())
}

topics = sys.argv[1:]

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("BronzeIngestion") \
    .getOrCreate()

for topic in topics:
    if topic not in schemas:
        print(f"[WARNING] No schema found for topic: {topic}, skipping...")
        continue

    print(f"[INFO] Processing topic: {topic}")
    schema = schemas[topic]
    expected_fields = set(field.name for field in schema.fields)

    # Read from Kafka
    raw_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    #Cast Kafka json into String df
    string_df = raw_df.selectExpr("CAST(value AS STRING) as json_string")

    parsed_df = string_df \
        .withColumn("data", from_json(col("json_string"), schema)) \
        .withColumn("raw_json", from_json(col("json_string"), "map<string,string>"))

    for field in expected_fields:
        parsed_df = parsed_df.withColumn(f"missing_{field}", col(f"data.{field}").isNull())

    has_missing_expr = " + ".join([f"CAST(missing_{field} AS INT)" for field in expected_fields])
    parsed_df = parsed_df.withColumn("has_missing", expr(f"{has_missing_expr} > 0"))

    expected_fields_str = ', '.join([f'"{f}"' for f in expected_fields])
    extra_expr = f"filter(map_keys(raw_json), x -> NOT array_contains(array({expected_fields_str}), x))"
    parsed_df = parsed_df.withColumn("extra_fields", expr(extra_expr))
    parsed_df = parsed_df.withColumn("has_extra", expr("size(extra_fields) > 0"))

    # Split
    valid_df = parsed_df.filter("has_missing = false AND has_extra = false").select("data.*")

    extra_df = parsed_df.filter("has_missing = false AND has_extra = true")
    extra_valid_df = extra_df.select("data.*")
    extra_error_df = extra_df.withColumn("expectedSchema", array(*[lit(f) for f in expected_fields])) \
        .withColumn("error_type", lit("extra_columns")) \
        .withColumn("original", col("json_string")) \
        .selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(original, expectedSchema, error_type)) AS value")

    missing_df = parsed_df.filter("has_missing = true")
    missing_error_df = missing_df.withColumn("expectedSchema", array(*[lit(f) for f in expected_fields])) \
        .withColumn("error_type", lit("missing_columns")) \
        .withColumn("original", col("json_string")) \
        .selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(original, expectedSchema, error_type)) AS value")

    # Save to HDFS
    valid_df.write.mode("append").parquet(f"hdfs://namenode:8020/user/hive/bronze/{topic}")
    extra_valid_df.write.mode("append").parquet(f"hdfs://namenode:8020/user/hive/bronze/{topic}")

    # Send errors to Kafka
    if not missing_error_df.isEmpty():
        missing_error_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "ErrorLogs") \
            .save()

    if not extra_error_df.isEmpty():
        extra_error_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "ErrorLogs") \
            .save()

    print(f"[INFO] Finished processing topic: {topic}")

spark.stop()
