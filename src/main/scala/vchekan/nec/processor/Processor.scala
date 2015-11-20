package vchekan.nec.processor

import java.text.SimpleDateFormat
import java.util.Date

import scala.async.Async.{async, await}
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

/**
  * Created by vadim on 11/16/15.
  */
object Processor extends App {
  val fname = "/home/vadim/projects/radio/dump1090-data/10ft-floor.csv"
  val lat = 33.050566
  val lon = -117.284466
  val elasticUrl = "http://localhost:9200/"

  println(Await.result(testDispatch(), 1.minute))

  val positions = scala.io.Source.fromFile(fname).getLines().
    filter(_.startsWith("MSG,3,")).
    map(Record(_, dist)).toSeq

  val count = positions.length
  println(s"Position records: $count")

  val planes = positions.groupBy(_.icaco).
    // (icaco, max dist, records, (lat,lon), alt)
    map(r => {
      val max = r._2.maxBy(_.dist)
      (r._1, max.dist, r._2, (max.lat, max.lon), max.alt)
    }).
    toSeq.sortBy(_._2)(Ordering[Double].reverse)
  println(s"Planes: ${planes.size}")

  println()
  planes.take(10).foreach(p => println(s"${p._1}\t${p._2}\t${p._3.size}\t(${p._4._1},${p._4._2}) ${p._5}"))

  println("Uploading...")
  positions.foreach(p => {
    Await.result(postPoint(p), 5.second)
  })
  println("Upload done")

  def toEsJson(p: Record) = {
    val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    s"""{
        |  "icaco": "${p.icaco}",
        |  "time": "${fmt.format(p.gen_date)}",
        |  "loc": [${p.lon}, ${p.lat}]
        |}""".stripMargin
  }

  def testDispatch(): Future[String] = {
    import dispatch._, Defaults._
    val svc = dispatch.url(elasticUrl) / "_cluster" / "health"
    val str = Http(svc OK as.String )
    str
  }

  def postPath(icaco: String, path: Seq[(Double,Double)]): Future[String] = {
    import dispatch._, Defaults._
    val svc = dispatch.url(elasticUrl) / "nec" / "nec_paths" / icaco
    val req = svc << ""
    val str = Http(req OK as.String )
    str
  }

  def postPoint(point: Record): Future[String] = {
    import dispatch._, Defaults._
    val svc = dispatch.url(elasticUrl) / "nec" / "nec_points"
    val req = svc << toEsJson(point)
    req.setContentType("text/json", "UTF-8")
    val str = Http(req OK as.String )
    str
  }

  def dist(lat: Double, lon: Double): Double = {
    val earthRadius = 6371000 //meters
    val dLat = Math.toRadians(this.lat - lat)
    val dLng = Math.toRadians(this.lon - lon)
    val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(Math.toRadians(this.lat)) * Math.cos(Math.toRadians(lat)) *
        Math.sin(dLng / 2) * Math.sin(dLng / 2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    earthRadius * c
  }
}

case class Record(icaco: String, gen_date: Date, lat: Double, lon: Double, dist: Double, alt: Int)

object Record {
  val data_parser = new SimpleDateFormat("yyyy/MM/ddHH:mm:ss.SSS")

  def apply(line: String, dist: (Double,Double) => Double) = {
    // MSG,3,111,11111,AA96E2,111111,2015/11/08,18:43:58.060,2015/11/08,18:43:58.063,,21000,,,33.42375,-118.38867,,,,,,0
    val parts = line.split(',')
    val icaco = parts(4)
    val date = data_parser.parse(parts(6) + parts(7))
    val alt = parts(11).toInt
    val lat = parts(14).toDouble
    val lon = parts(15).toDouble

    new Record(icaco, date, lat, lon, dist(lat, lon), alt)
  }
}