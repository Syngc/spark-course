package co.s4n.campus.spark.aerolines

import co.s4n.campus.spark.SparkSessionWrapper
import co.s4n.campus.spark.aerolines.domain.{Airlines, FlightsStats}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Reto2 extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {

    val airline_delays: DataFrame = Airlines.load("src/main/resources/airline-delay-and-cancellation/*.csv")

    destinations(airline_delays, "DCA")

  }

  def destinations(ds: DataFrame, origin: String): Seq[FlightsStats] = {
    import spark.implicits._
    ds
      .filter($"ORIGIN" === origin)
      .withColumn("TIME", classifyTime($"DEP_TIME") )
      .groupBy("DEST")
      .pivot("TIME", Seq("morningFlights", "afternoonFlights", "nightFlights")).count()
      .na.fill(0)
      .withColumnRenamed("DEST", "destination")
      .as[FlightsStats]
      .collect()
      .toSeq
  }

  val classifyTime: UserDefinedFunction = udf((depTime: Double) =>
    depTime match {
      case x if x <= 800              => "morningFlights"
      case x if x >  800 && x <= 1600 => "afternoonFlights"
      case _                          => "nightFlights"
    }
  )


}
