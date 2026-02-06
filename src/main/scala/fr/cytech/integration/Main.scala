package fr.cytech.integration

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.DriverManager
import scala.io.Source


object Main {

  def main(args: Array[String]): Unit = {

    // ---- CONFIG ----
    val cleanBucket = "nyc-clean"
    val cleanFileName = "yellow_tripdata_2024-01_clean.parquet"

    val rawPath = "s3a://nyc-raw/yellow_tripdata_2024-01.parquet"
    val cleanPathS3 = s"s3a://$cleanBucket/$cleanFileName"

    val jdbcUrl  = "jdbc:postgresql://localhost:5432/nyc_taxi_dw"
    val jdbcUser = "dw_user"
    val jdbcPwd  = "Big_data"
    val jdbcTable = "nyc_taxi.t_taxi_jaune" // c'est la table que nous allons utiliser

    // ---- SPARK ----
    val spark = SparkSession.builder()
      .appName("nyc-taxi-simple-ingestion")
      .master("local[*]")
      .getOrCreate()

    val hconf = spark.sparkContext.hadoopConfiguration
    hconf.set("fs.s3a.endpoint", "http://localhost:9000")
    hconf.set("fs.s3a.access.key", "minio")
    hconf.set("fs.s3a.secret.key", "minio123")
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.connection.ssl.enabled", "false")

    spark.sparkContext.setLogLevel("WARN")

    // ---- Lecture du fichier parquet ----
    val df = spark.read.parquet(rawPath)

    println(s"Lignes lues : ${df.count()}")

    // 2. Nettoyage
    // ==========================
    val cleanDF = df
      .filter(col("trip_distance") > 0)
      .filter(col("fare_amount") > 0)
      .filter(col("total_amount") > 0)
      .filter(col("passenger_count") >= 0)
      .filter(col("tpep_pickup_datetime").isNotNull)
      .filter(col("tpep_dropoff_datetime").isNotNull)
      .withColumn(
        "trip_duration_min",
        (unix_timestamp(col("tpep_dropoff_datetime")) -
          unix_timestamp(col("tpep_pickup_datetime"))) / 60
      )
      .filter(col("trip_duration_min") > 0)

    println(s"Nombre de lignes après nettoyage : ${cleanDF.count()}")

    // Écriture parquet clean dans MinIO
    println(s"Écriture du parquet nettoyé dans MinIO : $cleanPathS3")
    cleanDF.write.mode(SaveMode.Overwrite).parquet(cleanPathS3)
    println("   -> Parquet clean écrit avec succès dans nyc-clean ")

    // Création des tables et schémas dans le DWH

    def runSqlFile(path: String, jdbcUrl: String, user: String, pwd: String): Unit = {
      val sql = Source.fromFile(path).mkString
      val conn = DriverManager.getConnection(jdbcUrl, user, pwd)
      try {
        conn.createStatement().execute(sql)
      } finally {
        conn.close()
      }
    }


    runSqlFile("/home/cytech/IdeaProjects/new_projet_big_data/exercice_3/creation.sql", jdbcUrl, jdbcUser, jdbcPwd)


    // étape 3
    // On insère les données néttoyées dans le DWH
    val dfToInsert = cleanDF.select(
      col("tpep_pickup_datetime").cast("timestamp"),
      col("tpep_dropoff_datetime").cast("timestamp"),
      col("passenger_count").cast("int"),
      col("trip_distance"),
      col("RatecodeID").cast("int"),
      col("store_and_fwd_flag").cast("string"),
      col("PULocationID").cast("int"),
      col("DOLocationID").cast("int"),
      col("payment_type").cast("int"),
      col("fare_amount"),
      col("extra"),
      col("mta_tax"),
      col("tip_amount"),
      col("tolls_amount"),
      col("improvement_surcharge"),
      col("total_amount"),
      col("congestion_surcharge"),
      col("airport_fee")
    )

    // ---- WRITE POSTGRES ----
    dfToInsert.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", jdbcTable)
      .option("user", jdbcUser)
      .option("password", jdbcPwd)
      .option("driver", "org.postgresql.Driver")
      .mode(SaveMode.Append)
      .save()

    println("Insertion PostgreSQL terminée ")

    spark.stop()
  }


}
