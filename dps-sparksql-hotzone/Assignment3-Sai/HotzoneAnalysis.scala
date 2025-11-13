package cse511

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object HotzoneAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotZoneAnalysis(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = {
    var pointDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter",";")
      .option("header","false")
      .load(pointPath)
    pointDf.createOrReplaceTempView("point")

    spark.udf.register("stripParens",(s: String) => s.replace("(", "").replace(")", ""))
    pointDf = spark.sql("select stripParens(_c5) as p from point")
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter","\t")
      .option("header","false")
      .load(rectanglePath)
    rectangleDf.createOrReplaceTempView("rectangle")

    spark.udf.register("ST_Contains",(r:String, p:String) => HotzoneUtils.ST_Contains(r, p))
    val joinDf = spark.sql(
      "select rectangle._c0 as rectangle, point.p as point " +
      "from rectangle, point where ST_Contains(rectangle._c0, point.p)"
    )
    joinDf.createOrReplaceTempView("joinResult")

    val resultDf = spark.sql(
      "select rectangle, count(*) as count from joinResult group by rectangle order by rectangle asc"
    )
    resultDf.coalesce(1)
  }
}
