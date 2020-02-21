package co.s4n.campus.spark.aerolines.domain

import co.s4n.campus.spark.Main.spark
import org.apache.spark.sql.DataFrame

case class AirlineDelay(
    FL_DATE: String,
    OP_CARRIER: String,
    ORIGIN: String,
    DEST: String,
    DEP_DELAY: Option[String],
    ARR_DELAY: Option[String])

case class AirlineStats(
    name: String,
    totalFlights: Long,
    largeDelayFlights: Long,
    smallDelayFlights: Long,
    onTimeFlights: Long)

case class FlightsStats(destination: String, morningFlights: Long, afternoonFlights: Long, nightFlights: Long)

case class CancelledFlight(OP_CARRIER_FL_NUM: Int, ORIGIN: String, DEST: String, CANCELLED: Long, CAUSES: List[(String)])

object Airlines{
  def load(path: String): DataFrame =
    spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(path)
}
