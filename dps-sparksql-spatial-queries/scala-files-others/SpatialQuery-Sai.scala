package cse511

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point")

	// YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
	spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{
		val Array(x1,y1,x2,y2)=queryRectangle.trim.split(",").map(_.toDouble)
		val Array(p1,p2)=pointString.trim.split(",").map(_.toDouble)
		val minx=math.min(x1,x2)
		val miny=math.min(y1,y2)
		val maxx=math.max(x1,x2)
		val maxy=math.max(y1,y2)
		p1>=minx && p2>=miny && p1<=maxx && p2<=maxy
	})

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
	spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{
		val Array(x1,y1,x2,y2)=queryRectangle.trim.split(",").map(_.toDouble)
		val Array(p1,p2)=pointString.trim.split(",").map(_.toDouble)
		val minx=math.min(x1,x2)
		val miny=math.min(y1,y2)
		val maxx=math.max(x1,x2)
		val maxy=math.max(y1,y2)
		p1>=minx && p2>=miny && p1<=maxx && p2<=maxy
	})

	val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
	resultDf.show()

	return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point")

	// YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
	spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{
		val Array(x1,y1)=pointString1.trim.split(",").map(_.toDouble)
		val Array(x2,y2)=pointString2.trim.split(",").map(_.toDouble)
		math.sqrt(math.pow(x1-x2,2)+math.pow(y1-y2,2)) <= distance

	})

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
	spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{
		val Array(x1,y1)=pointString1.trim.split(",").map(_.toDouble)
		val Array(x2,y2)=pointString2.trim.split(",").map(_.toDouble)
		math.sqrt(math.pow(x1-x2,2)+math.pow(y1-y2,2)) <= distance

	})
	val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
	resultDf.show()

	return resultDf.count()
  }
}
