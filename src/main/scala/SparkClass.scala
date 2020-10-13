import org.apache.spark.sql._


object SparkClass extends App {

    val sorce_file = args(0)
    val sorce_file_dict = args(1)
    val result_file = args(2)

    import org.apache.spark.sql.SparkSession

    val spark = {
    SparkSession.builder()
        //.config("spark.sql.autoBroadcastJoinThreshold", 0)
        //.config("spark.driver.memory", "271859200")
        .master("local[*]")
        .config("spark.testing.memory", "471859200")
        .getOrCreate()
    }

    spark.sparkContext.setLogLevel("ERROR")

    def sc = spark.sparkContext

    val crimeFacts = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(sorce_file)

    val crimeFacts_ = crimeFacts.na.fill("Other", Seq("DISTRICT"))

    //crimeFacts_.describe("DISTRICT").show

    val offenseCodes = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv(sorce_file_dict)
    //offenseCodes.show(1000, false)

    val offenseCodes_ = offenseCodes
      .dropDuplicates("code")
      .orderBy("code")

    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.functions.callUDF

    var t1 = crimeFacts_
      .groupBy($"DISTRICT", $"YEAR", $"MONTH")
      .agg(
        count($"INCIDENT_NUMBER").as("crimes_total"),
        avg($"Lat").as("lat"),
        avg($"Long").as("lng")
      )
      .orderBy($"crimes_total".desc)
      .groupBy($"DISTRICT")
      .agg(
        sum($"crimes_total").as("crimes_total"),
        callUDF("percentile_approx", $"crimes_total", lit(0.5)).as("crimes_monthly"),
        avg($"lat").as("lat"),
        avg($"lng").as("lng")
      )
    //t1.show(false)

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.broadcast

    val offenseCodesBroadcast = broadcast(offenseCodes_)

    // Window definition
    val w = Window.partitionBy($"DISTRICT").orderBy(desc("count"))

    var t2 = crimeFacts_
      .join(offenseCodesBroadcast, $"CODE" === $"OFFENSE_CODE")
      .withColumn("NAME_GROUP", split(col("NAME"), "\\ - ").getItem(0))
      .groupBy($"DISTRICT", $"NAME_GROUP")
      .agg(
        count("INCIDENT_NUMBER").as("count")
      )
      .orderBy($"DISTRICT", $"count".desc)
      //groupBy($"DISTRICT")
      // Filter by rank
      .withColumn("rank", rank.over(w)).where($"rank" <= 3)
      .groupBy('DISTRICT).agg(concat_ws(", ", collect_set($"NAME_GROUP")).as("frequent_crime_types")
    )
    //t2.show(100,false)


    // final join
    val final_df = t1
      .join(t2, t1("DISTRICT") === t2("DISTRICT"))
      .withColumn("frequent_crime_types", regexp_replace($"frequent_crime_types", " ,", ",")).as("frequent_crime_types")
      .select(t1("district"),
        $"crimes_total",
        $"crimes_monthly",
        $"frequent_crime_types",
        $"lat",
        $"lng"
      )
      .orderBy($"DISTRICT".asc)
    final_df.show(false)

    //final_df.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("boston_crimes_final.parquet")
    final_df.coalesce(1).write.mode("overwrite").parquet(result_file)

}