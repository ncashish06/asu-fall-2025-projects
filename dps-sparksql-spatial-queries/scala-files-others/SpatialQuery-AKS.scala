package cse511

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point")

	// YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
	spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>({
	val pointCoords = pointString.split(",")
	val px = pointCoords(0).toDouble
	val py = pointCoords(1).toDouble
	val rectCoords = queryRectangle.split(",")
	val rx1 = rectCoords(0).toDouble
	val ry1 = rectCoords(1).toDouble
	val rx2 = rectCoords(2).toDouble
	val ry2 = rectCoords(3).toDouble

	// Check if the point is within the rectangle's bounds (inclusive)
	(px >= math.min(rx1, rx2) && px <= math.max(rx1, rx2) && py >= math.min(ry1, ry2) && py <= math.max(ry1, ry2))
	}))

	val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
	resultDf.show()

	return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point")

	val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
	rectangleDf.createOrReplaceTempView("rectangle")

	// YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
	spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>({
	val pointCoords = pointString.split(",")
	val px = pointCoords(0).toDouble
	val py = pointCoords(1).toDouble
	val rectCoords = queryRectangle.split(",")
	val rx1 = rectCoords(0).toDouble
	val ry1 = rectCoords(1).toDouble
	val rx2 = rectCoords(2).toDouble
	val ry2 = rectCoords(3).toDouble
	// Check if the point is within the rectangle's bounds (inclusive)
	(px >= math.min(rx1, rx2) && px <= math.max(rx1, rx2) && py >= math.min(ry1, ry2) && py <= math.max(ry1, ry2))
	}))

	val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
	resultDf.show()
	return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point")

	// YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
	spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>({
	val p1Coords = pointString1.split(",")
	val x1 = p1Coords(0).toDouble
	val y1 = p1Coords(1).toDouble

	// Parse the second point string "x2,y2"
	val p2Coords = pointString2.split(",")
	val x2 = p2Coords(0).toDouble
	val y2 = p2Coords(1).toDouble

	// Calculate the distance
	val euclidDistance = math.sqrt(math.pow(x2 - x1, 2) + math.pow(y2 - y1, 2))

	// Check if the calculated distance is less than or equal to the input distance
	euclidDistance <= distance
	}))

	val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
	resultDf.show()

	return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point1")

	val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
	pointDf2.createOrReplaceTempView("point2")

	// YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
	spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>({
	val p1Coords = pointString1.split(",")
	val x1 = p1Coords(0).toDouble
	val y1 = p1Coords(1).toDouble

	// Parse the second point string "x2,y2"
	val p2Coords = pointString2.split(",")
	val x2 = p2Coords(0).toDouble
	val y2 = p2Coords(1).toDouble

	// Calculate the distance
	val euclidDistance = math.sqrt(math.pow(x2 - x1, 2) + math.pow(y2 - y1, 2))

	// Check if the calculated distance is less than or equal to the input distance
	euclidDistance <= distance
	}))
	val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
	resultDf.show()

	return resultDf.count()
  }
}
