package cse511

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    // YOU NEED TO CHANGE THIS PART  
    if (queryRectangle == null || pointString == null) return false

    val rect = queryRectangle.split(",").map(_.trim)
    val pt   = pointString.split(",").map(_.trim)
    if (rect.length != 4 || pt.length != 2) return false

    val x1 = rect(0).toDouble; 
    val y1 = rect(1).toDouble;
    val x2 = rect(2).toDouble;
    val y2 = rect(3).toDouble
    val px = pt(0).toDouble;
    val py = pt(1).toDouble;

    val minx = math.min(x1, x2); 
    val maxx = math.max(x1, x2);
    val miny = math.min(y1, y2);
    val maxy = math.max(y1, y2);

    // last line is automatically returned as value of function, return keyword is optional
    px >= minx && px <= maxx && py >= miny && py <= maxy    
  }
}
