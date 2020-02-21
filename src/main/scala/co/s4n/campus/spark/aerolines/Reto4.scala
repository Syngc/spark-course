package co.s4n.campus.spark.aerolines

import co.s4n.campus.spark.SparkSessionWrapper
import co.s4n.campus.spark.aerolines.domain.Airlines
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.date_format

object Reto4 extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {

    val airline_delays: DataFrame = Airlines.load("src/main/resources/airline-delay-and-cancellation/*.csv")

    daysWithDelays(airline_delays)

  }

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
