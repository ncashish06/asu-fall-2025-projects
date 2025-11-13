package cse511

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    var pickupInfo = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter",";")
      .option("header","false")
      .load(pointPath)
    pickupInfo.createOrReplaceTempView("nyctaxitrips")

    spark.udf.register("CalculateX", (pickupPoint: String) => HotcellUtils.CalculateCoordinate(pickupPoint, 0))
    spark.udf.register("CalculateY", (pickupPoint: String) => HotcellUtils.CalculateCoordinate(pickupPoint, 1))
    spark.udf.register("CalculateZ", (pickupTime:   String) => HotcellUtils.CalculateCoordinate(pickupTime, 2))

    pickupInfo = spark.sql(
      """
        |SELECT
        |  CalculateX(nyctaxitrips._c5) AS x,
        |  CalculateY(nyctaxitrips._c5) AS y,
        |  CalculateZ(nyctaxitrips._c1) AS z
        |FROM nyctaxitrips
      """.stripMargin)

    val minXi = (-74.50 / HotcellUtils.coordinateStep).toInt
    val maxXi = (-73.70 / HotcellUtils.coordinateStep).toInt
    val minYi = ( 40.50 / HotcellUtils.coordinateStep).toInt
    val maxYi = ( 40.90 / HotcellUtils.coordinateStep).toInt
    val minZi = 1
    val maxZi = 31

    val nCells: Double =
      (maxXi - minXi + 1).toDouble *
      (maxYi - minYi + 1).toDouble *
      (maxZi - minZi + 1).toDouble

    pickupInfo.createOrReplaceTempView("pickupinfo")

    val pointCount = spark.sql(s"""
      SELECT CAST(x AS INT) x, CAST(y AS INT) y, CAST(z AS INT) z, COUNT(*) AS cnt
      FROM pickupinfo
      WHERE x >= $minXi AND x <= $maxXi
        AND y >= $minYi AND y <= $maxYi
        AND z >= $minZi AND z <= $maxZi
      GROUP BY x, y, z
    """).cache()
    pointCount.createOrReplaceTempView("pointcount")

    val sumsRow = spark.sql("""SELECT SUM(cnt) AS sumX, SUM(cnt * cnt) AS sumX2 FROM pointcount""").first()
    val sumX  = if (sumsRow.isNullAt(0)) 0.0 else sumsRow.getAs[Number]("sumX").doubleValue()
    val sumX2 = if (sumsRow.isNullAt(1)) 0.0 else sumsRow.getAs[Number]("sumX2").doubleValue()
    val meanX = sumX / nCells
    val varX  = math.max(0.0, (sumX2 / nCells) - (meanX * meanX))
    val S     = math.sqrt(varX)

    val neighborWeightUDF = udf { (x: Int, y: Int, z: Int) =>
      val xl = math.max(x - 1, minXi); val xh = math.min(x + 1, maxXi)
      val yl = math.max(y - 1, minYi); val yh = math.min(y + 1, maxYi)
      val zl = math.max(z - 1, minZi); val zh = math.min(z + 1, maxZi)
      val nx = (xh - xl + 1).max(0)
      val ny = (yh - yl + 1).max(0)
      val nz = (zh - zl + 1).max(0)
      nx * ny * nz
    }

    val neighborSum = pointCount.as("a")
      .join(
        pointCount.as("b"),
        (col("b.x") >= col("a.x") - 1) && (col("b.x") <= col("a.x") + 1) &&
        (col("b.y") >= col("a.y") - 1) && (col("b.y") <= col("a.y") + 1) &&
        (col("b.z") >= col("a.z") - 1) && (col("b.z") <= col("a.z") + 1),
        "left"
      )
      .groupBy(col("a.x").as("x"), col("a.y").as("y"), col("a.z").as("z"))
      .agg(sum(coalesce(col("b.cnt"), lit(0))).as("sumNbr"))
      .withColumn("W", neighborWeightUDF(col("x"), col("y"), col("z")))

    val giUDF = udf { (sumNbr: Double, W: Int) =>
      if (W <= 0 || S == 0.0 || nCells <= 1.0) 0.0
      else {
        val num = sumNbr - (meanX * W)
        val den = S * math.sqrt((nCells * W - W.toDouble * W.toDouble) / (nCells - 1.0))
        if (den == 0.0) 0.0 else num / den
      }
    }

    val withGi = neighborSum
      .withColumn("Gi", giUDF(col("sumNbr").cast("double"), col("W")))
      .orderBy(col("Gi").desc)

    withGi.select("x", "y", "z")
  }
}
