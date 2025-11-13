package cse511

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

    def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
    {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART
    
    pickupInfo.createOrReplaceTempView("pickupInfo")

    val aggregatedData = spark.sql("""
      SELECT x, y, z, COUNT(*) AS count
      FROM pickupInfo
      GROUP BY x, y, z
    """)
    aggregatedData.createOrReplaceTempView("aggregatedData")

    // Register UDF to check if neighbor cell is within range
    spark.udf.register("isNeighbor", (x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int) => {
      val dx = Math.abs(x1 - x2)
      val dy = Math.abs(y1 - y2)
      val dz = Math.abs(z1 - z2)
      dx <= 1 && dy <= 1 && dz <= 1
    })

    // Compute total sum and squared sum of counts
    val stats = spark.sql("SELECT SUM(count) AS sum, SUM(count * count) AS sumSquares, COUNT(*) AS cellCount FROM aggregatedData").first()
    val totalSum = stats.getLong(0).toDouble
    val sumSquares = stats.getLong(1).toDouble
    val cellCount = stats.getLong(2)
    val mean = totalSum / cellCount
    val stdDev = math.sqrt((sumSquares / cellCount) - (mean * mean))

    // Broadcast stats
    val bcMean = spark.sparkContext.broadcast(mean)
    val bcStdDev = spark.sparkContext.broadcast(stdDev)
    val bcNumCells = spark.sparkContext.broadcast(cellCount)

    // Self join to compute neighborhood sums
    val neighbors = spark.sql("""
      SELECT a.x, a.y, a.z, SUM(b.count) AS wSum, COUNT(*) AS weight
      FROM aggregatedData a, aggregatedData b
      WHERE isNeighbor(a.x, a.y, a.z, b.x, b.y, b.z)
      GROUP BY a.x, a.y, a.z
    """)
    neighbors.createOrReplaceTempView("neighbors")

    // Compute G-score using SQL expression
    val gScore = spark.sql(s"""
      SELECT x, y, z,
        (wSum - ${bcMean.value} * weight) /
        (${bcStdDev.value} * SQRT( (${bcNumCells.value} * weight - weight * weight) / (${bcNumCells.value} - 1) )) AS z_score
      FROM neighbors
    """)
    gScore.createOrReplaceTempView("gScore")

    // Select top 50 hottest cells ordered by descending z-score
    val result = spark.sql("""
      SELECT x, y, z
      FROM gScore
      ORDER BY z_score DESC
      LIMIT 50
    """)

    return result
    }
}
