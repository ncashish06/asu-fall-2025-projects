package cse511

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
    var result = 0
    coordinateOffset match {
      case 0 => result = Math.floor(inputString.split(",")(0).replace("(","").toDouble / coordinateStep).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble / coordinateStep).toInt
      case 2 => 
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp)
    }
    result
  }

  def timestampParser (timestampString: String): Timestamp = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    new Timestamp(parsedDate.getTime)
  }

  def dayOfYear (timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    calendar.get(Calendar.DAY_OF_MONTH)
  }

  def calculateNeighborNumbers(
      x: Int, y: Int, z: Int,
      minX: Int, minY: Int, minZ: Int,
      maxX: Int, maxY: Int, maxZ: Int
  ): Int = {
    val xLow  = math.max(x - 1, minX)
    val xHigh = math.min(x + 1, maxX)
    val yLow  = math.max(y - 1, minY)
    val yHigh = math.min(y + 1, maxY)
    val zLow  = math.max(z - 1, minZ)
    val zHigh = math.min(z + 1, maxZ)
    val nx = (xHigh - xLow + 1).max(0)
    val ny = (yHigh - yLow + 1).max(0)
    val nz = (zHigh - zLow + 1).max(0)
    nx * ny * nz
  }

  def calculateGScore(
      numCells: Double,
      sumNeighbors: Double,
      numNeighbors: Int,
      mean: Double,
      std: Double
  ): Double = {
    if (std == 0.0 || numNeighbors <= 0 || numCells <= 1.0) 0.0
    else {
      val w  = numNeighbors.toDouble
      val num = sumNeighbors - mean * w
      val den = std * math.sqrt((numCells * w - w * w) / (numCells - 1.0))
      if (den == 0.0) 0.0 else num / den
    }
  }
}
