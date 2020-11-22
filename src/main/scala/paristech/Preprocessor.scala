package paristech

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{bround, concat, length, lit, lower, udf, when}


object Preprocessor {

  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    // Des réglages optionnels du job spark. Les réglages par défaut fonctionnent très bien pour ce TP.
    // On vous donne un exemple de setting quand même
    val conf = new SparkConf().setAll(Map(
      "spark.master" -> "local",
      "spark.scheduler.mode" -> "FIFO",
      "spark.speculation" -> "false",
      "spark.reducer.maxSizeInFlight" -> "48m",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryoserializer.buffer.max" -> "1g",
      "spark.shuffle.file.buffer" -> "32k",
      "spark.default.parallelism" -> "12",
      "spark.sql.shuffle.partitions" -> "12"
    ))

    // Initialisation du SparkSession qui est le point d'entrée vers Spark SQL (donne accès aux dataframes, aux RDD,
    // création de tables temporaires, etc., et donc aux mécanismes de distribution des calculs)
    val spark = SparkSession
      .builder
      .config(conf)
      .appName("TP Spark : Preprocessor")
      .getOrCreate()

    import spark.implicits._

    /** *****************************************************************************
      *
      * TP 2
      *
      *       - Charger un fichier csv dans un dataFrame
      *       - Pre-processing: cleaning, filters, feature engineering => filter, select, drop, na.fill, join, udf, distinct, count, describe, collect
      *       - Sauver le dataframe au format parquet
      *
      * if problems with unimported modules => sbt plugins update
      *
      * ****************************************************************************** */

    val df: DataFrame = spark
      .read
      .option("header", true) // utilise la première ligne du (des) fichier(s) comme header
      .option("inferSchema", "true") // pour inférer le type de chaque colonne (Int, String, etc.)
      .csv("/home/tlefievre/MSBGD/Spark/spark_project_kickstarter_2020_2021/data/train_clean.csv")

    println(s"Nombre de lignes : ${df.count}")
    println(s"Nombre de colonnes : ${df.columns.length}")

    df.show()

    val dfCasted: DataFrame = df
      .withColumn("goal", $"goal".cast("Int"))
      .withColumn("deadline", $"deadline".cast("Int"))
      .withColumn("state_changed_at", $"state_changed_at".cast("Int"))
      .withColumn("created_at", $"created_at".cast("Int"))
      .withColumn("launched_at", $"launched_at".cast("Int"))
      .withColumn("backers_count", $"backers_count".cast("Int"))
      .withColumn("final_status", $"final_status".cast("Int"))

    dfCasted.printSchema()

    dfCasted
      .select("goal", "backers_count", "final_status")
      .describe()
      .show

    val df2: DataFrame = dfCasted.drop("disable_communication")

    val dfNoFutur: DataFrame = df2.drop("backers_count", "state_changed_at")

    dfCasted.groupBy("disable_communication").count.orderBy($"count".desc).show(100)
    dfCasted.groupBy("country").count.orderBy($"count".desc).show(100)
    dfCasted.groupBy("currency").count.orderBy($"count".desc).show(100)
    dfCasted.select("deadline").dropDuplicates.show()
    dfCasted.groupBy("state_changed_at").count.orderBy($"count".desc).show(100)
    dfCasted.groupBy("backers_count").count.orderBy($"count".desc).show(100)
    dfCasted.select("goal", "final_status").show(30)
    dfCasted.groupBy("country", "currency").count.orderBy($"count".desc).show(50)


    df.filter($"country" === "False")
      .groupBy("currency")
      .count
      .orderBy($"count".desc)
      .show(50)

    val dfCountry: DataFrame = dfNoFutur
      .withColumn("country2", when($"country" === "False", $"currency").otherwise($"country"))
      .withColumn("currency2", when($"country".isNotNull && length($"currency") =!= 3, null).otherwise($"currency"))
      .drop("country", "currency")

    val dfNoUnix: DataFrame = dfCountry
      .withColumn(colName = "days_campaign", bround(($"deadline" - $"launched_at") / 86400, scale = 3))
      .withColumn(colName = "hours_prepa", bround(($"launched_at" - $"created_at") / 3600, scale = 3))
      .drop("deadline", "launched_at", "created_at")

    val dfTextConcat: DataFrame = dfNoUnix
      .withColumn("text", concat(lower($"name"), lit(' '), lower($"desc"), lit(" "), lower($"keywords")))
      .drop("name", "desc", "keywords")

    val dfFillNull: DataFrame = dfTextConcat
      .withColumn("days_campaign", when($"days_campaign".isNull, -1).otherwise($"days_campaign"))
      .withColumn("hours_prepa", when($"hours_prepa".isNull, -1).otherwise($"hours_prepa"))
      .withColumn("country", when($"country2".isNull, "unknown").otherwise($"country2"))
      .withColumn("currency", when($"currency2".isNull, "unknown").otherwise($"currency2"))
      .drop("country2","currency2")

    dfFillNull.write.parquet("/home/tlefievre/MSBGD/Spark/spark_project_kickstarter_2020_2021/data/train_clean.parquet")

    dfFillNull.show()


  }




}
