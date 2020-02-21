package co.s4n.campus.spark.aerolines

import co.s4n.campus.spark.SparkSessionWrapper
import co.s4n.campus.spark.aerolines.domain.{AirlineStats, Airlines}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Reto1 extends SparkSessionWrapper {

  def main(args: Array[String]) : Unit = run()

  def run(): Unit = {

    val airlines: DataFrame = Airlines.load("src/main/resources/airlines.csv")
    val airline_delays: DataFrame = Airlines.load("src/main/resources/airline-delay-and-cancellation/*.csv")

    delayedAirlines(airline_delays, airlines, None)

  }

  def delayedAirlines(ds: DataFrame, airlines: DataFrame, my_year: Option[String]): Seq[AirlineStats] = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df = my_year
      .map(y => ds.filter($"FL_DATE".startsWith(y)))
      .getOrElse(ds)

    df
      .groupBy("OP_CARRIER", "ARR_DELAY")
      .count()
      .withColumn("TIME", classifyFlight($"ARR_DELAY"))
      .stat.crosstab( "OP_CARRIER","TIME" )
      .withColumn("totalFLights", expr("largeDelayFlights + smallDelayFlights + onTimeFlights"))
      .join(airlines, $"OP_CARRIER_TIME" === $"IATA_CODE", "inner")
      .withColumnRenamed("AIRLINE", "name")
      .sort($"largeDelayFlights".desc, $"smallDelayFlights".desc)
      .as[AirlineStats]
      .collect()
      .toSeq
  }

  val classifyFlight: UserDefinedFunction = udf((time: Int) =>{
    time match {
      case x if x > 45           => "largeDelayFlights"
      case x if x > 5 && x <= 45 => "smallDelayFlights"
      case _                     => "onTimeFlights"
    }
  })
}
