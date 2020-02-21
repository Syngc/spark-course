package co.s4n.campus.spark


import co.s4n.campus.spark.Main.spark
import co.s4n.campus.spark.domain.{AirlineDelay, AirlineStats, CancelledFlight, FlightsStats}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{count, sum, udf}
import org.apache.spark.sql.functions._

import scala.collection.mutable


object Main extends SparkSessionWrapper {

  def main(args: Array[String]) : Unit = run()

  def run(): Unit = {

    val airlines : DataFrame = load("src/main/resources/airlines.csv")
    val airline_delays: DataFrame = load("src/main/resources/airline-delay-and-cancellation/*.csv")

    val reto1 = delayedAirlines(airline_delays, airlines, None)
    val reto2 = destinations(airline_delays, "DCA")
    val reto3 = flightInfo(airline_delays)
    val reto4 = daysWithDelays(airline_delays)
  }

  def load(path: String): DataFrame =
  spark.read
    .format("csv")
    .option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(path)

//Primer reto
  def delayedAirlines(ds: DataFrame, airlines: DataFrame, my_year: Option[String]): Seq[AirlineStats] = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

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


  //Segundo reto
  def destinations(ds: DataFrame, origin: String): Seq[FlightsStats] = {
    import spark.implicits._
    ds
      .filter($"ORIGIN" === origin)
      .withColumn("TIME", classsifyTime($"DEP_TIME") )
      .groupBy("DEST")
      .pivot("TIME", Seq("morningFlights", "afternoonFlights", "nightFlights")).count()
      .na.fill(0)
      .withColumnRenamed("DEST", "destination")
      .as[FlightsStats]
      .collect()
      .toSeq
  }

  val classsifyTime: UserDefinedFunction = udf((depTime: Double) =>
    depTime match {
      case x if x <= 800              => "morningFlights"
      case x if x >  800 && x <= 1600 => "afternoonFlights"
      case _                          => "nightFlights"
    }
  )

  //Tercer reto
  def flightInfo(ds: DataFrame): Seq[CancelledFlight] = {
    import spark.implicits._

    ds
      .filter($"CANCELLED" === 1)
      .groupBy("OP_CARRIER_FL_NUM", "ORIGIN", "DEST" )
      .agg(
        count("OP_CARRIER_FL_NUM").as("CANCELLED"),
        collect_list("CANCELLATION_CODE").as("CODE_LIST")
      )
      .withColumn("CAUSES", countCauses($"CODE_LIST"))
      .orderBy($"CANCELLED".desc)
      .as[CancelledFlight]
      .take(20)
      .toSeq

  }

  val countCauses: UserDefinedFunction = udf((codes :mutable.WrappedArray[String]) => {
    val transform = (code: String) => code match {
      case "A" => "Airline/Carrier"
      case "B" => "Weather"
      case "C" => "National Air System"
      case "D" => "Security"
      case _ => "Unknown"
    }
    codes
      .groupBy(identity)
      .map(x => (transform(x._1), x._2.length)).toList
  })

  //Cuarto Reto
  def daysWithDelays(ds:DataFrame) : List[(String, Long)] = {
    import spark.implicits._

    ds
      .filter($"ARR_DELAY" > 45)
        .withColumn("DAY", date_format($"FL_DATE", "E" ))
        .groupBy("DAY")
        .count()
        .sort($"count".desc)
        .as[(String, Long)]
        .collect()
        .toList
  }
}
