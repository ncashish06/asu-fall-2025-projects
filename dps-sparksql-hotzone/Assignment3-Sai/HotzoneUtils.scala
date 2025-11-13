package cse511

import scala.math.{min, max}

object HotzoneUtils {
  def parsePoint(pointString: String): (Double, Double) = {
    val cleaned = pointString.replace("(", "").replace(")", "").trim
    val parts = cleaned.split(",").map(_.trim)
    if (parts.length != 2)
      throw new IllegalArgumentException(s"Invalid point format: $pointString")
    val x = parts(0).toDouble
    val y = parts(1).toDouble
    (x, y)
  }

  def parseRectangle(rectangleString: String): ((Double, Double), (Double, Double)) = {
    val cleaned = rectangleString.replace("(", "").replace(")", "").trim
    val parts = cleaned.split(",").map(_.trim)
    if (parts.length != 4)
      throw new IllegalArgumentException(s"Invalid rectangle format: $rectangleString")
    val x1 = parts(0).toDouble
    val y1 = parts(1).toDouble
    val x2 = parts(2).toDouble
    val y2 = parts(3).toDouble
    ((min(x1, x2), min(y1, y2)), (max(x1, x2), max(y1, y2)))
  }

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    val ((minX, minY), (maxX, maxY)) = parseRectangle(queryRectangle)
    val (x, y) = parsePoint(pointString)
    x >= minX && x <= maxX && y >= minY && y <= maxY
  }
}
