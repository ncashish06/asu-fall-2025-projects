package cse511
import org.apache.spark.sql.SparkSession
object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")
    
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>({
      val rectangleCoordinates = queryRectangle.split(",").map(_.trim.toDouble)
      val x1 = rectangleCoordinates(0)
      val y1 = rectangleCoordinates(1)
      val x2 = rectangleCoordinates(2)
      val y2 = rectangleCoordinates(3)
      
      val pointCoordinates = pointString.split(",").map(_.trim.toDouble)
      val pointX = pointCoordinates(0)
      val pointY = pointCoordinates(1)
      
      val minX = Math.min(x1, x2)
      val maxX = Math.max(x1, x2)
      val minY = Math.min(y1, y2)
      val maxY = Math.max(y1, y2)
      
      pointX >= minX && pointX <= maxX && pointY >= minY && pointY <= maxY
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
    
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>({
      val rectangleCoordinates = queryRectangle.split(",").map(_.trim.toDouble)
      val x1 = rectangleCoordinates(0)
      val y1 = rectangleCoordinates(1)
      val x2 = rectangleCoordinates(2)
      val y2 = rectangleCoordinates(3)
      
      val pointCoordinates = pointString.split(",").map(_.trim.toDouble)
      val pointX = pointCoordinates(0)
      val pointY = pointCoordinates(1)
      
      val minX = Math.min(x1, x2)
      val maxX = Math.max(x1, x2)
      val minY = Math.min(y1, y2)
      val maxY = Math.max(y1, y2)
      
      pointX >= minX && pointX <= maxX && pointY >= minY && pointY <= maxY
    }))
    
    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()
    return resultDf.count()
  }
  
  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")
    
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>({
      val point1Coordinates = pointString1.split(",").map(_.trim.toDouble)
      val x1 = point1Coordinates(0)
      val y1 = point1Coordinates(1)
      
      val point2Coordinates = pointString2.split(",").map(_.trim.toDouble)
      val x2 = point2Coordinates(0)
      val y2 = point2Coordinates(1)
      
      val calculatedDistance = Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2))
      
      calculatedDistance <= distance
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
    
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>({
      val point1Coordinates = pointString1.split(",").map(_.trim.toDouble)
      val x1 = point1Coordinates(0)
      val y1 = point1Coordinates(1)
      
      val point2Coordinates = pointString2.split(",").map(_.trim.toDouble)
      val x2 = point2Coordinates(0)
      val y2 = point2Coordinates(1)
      
      val calculatedDistance = Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2))
      
      calculatedDistance <= distance
    }))
    
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()
    return resultDf.count()
  }
}