package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App {
  /*
  * Input: queryRectangle:String, pointString:String
  * Output: Boolean (true or false)
  * Definition: Parse queryRectangle as x & Y coordinates of two diagonally opposite points
  *             Parse pointString as X & Y coordinates
  *             Check whether the queryRectangle fully contains the point considering on-boundary points as well.
  * Example Inputs: queryRectangle = "-155.940114, 19.081331, -155.618917, 19.5307"
  *                 pointString = "-88.331492, 32.324142"
  * */
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    if(queryRectangle == null || queryRectangle.isEmpty() || pointString == null || pointString.isEmpty())
      return false

    val rectangleCoordinates = queryRectangle.split(",")
    if(rectangleCoordinates.length < 4)
      return false

    val xOfCorner1 = rectangleCoordinates(0).trim.toDouble
    val yOfCorner1 = rectangleCoordinates(1).trim.toDouble
    val xOfCorner2 = rectangleCoordinates(2).trim.toDouble
    val yOfCorner2 = rectangleCoordinates(3).trim.toDouble

    val pointCoordinates = pointString.split(",")
    if(pointCoordinates.length < 2)
      return false

    val pointX = pointCoordinates(0).trim.toDouble
    val pointY = pointCoordinates(1).trim.toDouble

    if(pointX >=  math.min(xOfCorner1, xOfCorner2) && pointX <= math.max(xOfCorner1, xOfCorner2)
      && pointY >= math.min(yOfCorner1, yOfCorner2) && pointY <= math.max(yOfCorner1, yOfCorner2))
      return true

    return false
  }

  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean = {
    return false
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> ST_Contains(queryRectangle, pointString))

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
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> ST_Contains(queryRectangle, pointString))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=> ST_Within(pointString1, pointString2, distance))

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
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=> ST_Within(pointString1, pointString2, distance))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
