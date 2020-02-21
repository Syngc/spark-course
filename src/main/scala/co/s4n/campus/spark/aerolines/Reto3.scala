package co.s4n.campus.spark.aerolines

import co.s4n.campus.spark.SparkSessionWrapper
import co.s4n.campus.spark.aerolines.domain.{Airlines, CancelledFlight}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{collect_list, count, udf}

import scala.collection.mutable

object Reto3 extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {

    val airline_delays: DataFrame = Airlines.load("src/main/resources/airline-delay-and-cancellation/*.csv")

    flightInfo(airline_delays)

  }

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

}
