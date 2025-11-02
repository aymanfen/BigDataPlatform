from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, array, lit
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
import sys

# CLI Argument Check
if len(sys.argv) < 2:
    print("Usage: spark-submit Processing.py <Topic1> <Topic2> ...")
    sys.exit(1)

schemas = {
    "FactDeclaration": StructType()
        .add("ID", IntegerType())
        .add("NumAff", StringType())
        .add("NumImm", StringType())
        .add("IDPeriode", StringType())
        .add("MontantDeclaration", FloatType()),

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

# Start Spark session once
spark = SparkSession.builder \
    .appName("SilverProcessing") \
    .getOrCreate()

for topic in topics:
    if topic not in schemas:
        print(f"[WARNING] No schema found for topic: {topic}, skipping...")
        continue

    print(f"[INFO] Processing topic: {topic}")
    silver_schema = schemas[topic]
    columns_to_cast = {field.name: field.dataType for field in silver_schema.fields}

    bronze_path = f"hdfs://namenode:8020/user/hive/bronze/{topic}/"

    try:
        bronze_df = spark.read.parquet(bronze_path)
    except AnalysisException:
        print(f"[WARNING] No data found at {bronze_path}, skipping...")
        continue

    # Type casting
    casted_df = bronze_df
    for col_name, target_type in columns_to_cast.items():
        casted_df = casted_df.withColumn(col_name + "_casted", col(col_name).cast(target_type))

    # Identify errors
    error_conditions = [col(col_name).isNotNull() & col(col_name + "_casted").isNull()
                        for col_name in columns_to_cast.keys()]
    error_condition = error_conditions[0]
    for cond in error_conditions[1:]:
        error_condition = error_condition | cond

    invalid_rows_df = casted_df.filter(error_condition)
    valid_rows_df = casted_df.filter(~error_condition)

    # Select only casted columns
    selected_cols = [col(col_name + "_casted").alias(col_name) for col_name in columns_to_cast.keys()]
    final_df = valid_rows_df.select(*selected_cols)

    # Save valid rows to silver zone
    silver_path = f"hdfs://namenode:8020/user/hive/silver/{topic}"
    final_df.write.mode("append").parquet(silver_path)

    # send invalid rows to kafka
    if not invalid_rows_df.isEmpty():
        error_df = invalid_rows_df.withColumn("expectedSchema", array(*[lit(f.name) for f in silver_schema.fields])) \
            .withColumn("error_type", lit("type_cast_error")) \
            .withColumn("original", to_json(struct(*bronze_df.columns))) \
            .selectExpr("CAST(NULL AS STRING) as key", "to_json(struct(original, expectedSchema, error_type)) as value")

        error_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "ErrorLogs") \
            .save()

    print(f"[INFO] Finished processing topic: {topic}")


spark.stop()
