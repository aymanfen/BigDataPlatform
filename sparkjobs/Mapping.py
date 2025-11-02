import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import col, expr, concat_ws, abs

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

mappings = {
    "DimAssure": {
        "NumImm": lambda df : col("NumImm"),                                
        "FullName": lambda df : concat_ws("",col("Nom"),col("Prenom")),       
        "CIN": lambda df : col("CIN"),                                      
        "Coord": lambda df : concat_ws(" | ",col("Adresse"),col("Tel")),      
        "Status":lambda df : col("Status")
    },
    
    "FactDeclaration": {
        "ID":lambda df : col("ID"),
        "NumAff":lambda df : col("NumAff"),
        "NumImm":lambda df : col("NumImm"),
        "IDPeriode":lambda df : col("IDPeriode"),
        "MontantDeclaration":lambda df : abs(col("MontantDeclaration"))
    }
}

# --- Validate CLI args ---
if len(sys.argv) < 2:
    print("Usage: spark-submit mapper.py <Topic>")
    sys.exit(1)

topic = sys.argv[1]

if topic not in schemas or topic not in mappings:
    print(f"[ERROR] Topic '{topic}' not defined in schemas or mappings.")
    sys.exit(1)

# --- Initialize Spark ---
spark = SparkSession.builder.appName(f"{topic}Mapping").getOrCreate()


# --- Load Data ---
df=spark.read.option("header","true").csv(f"/opt/sparkjobs/{topic}.csv")


# --- Apply Mappings ---
mapping_rules = mappings[topic]
transformed_df = df.select([expr(df).alias(name) for name, expr in mapping_rules.items()])


# --- Save Results ---
transformed_df.write.csv(f"/opt/sparkjobs/output/{topic}")

spark.stop()


