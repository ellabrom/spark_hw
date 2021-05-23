package com.epam.spark_hw.scala.handler

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties
import scala.io.{BufferedSource, Source}



class TripsHandlerScala() extends Serializable  {
  val sparkConf: SparkConf = new SparkConf().setAppName("homework").setMaster("local[*]")
  @transient  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("ERROR")
  val x = new Properties
  val propertiesFile: String = "src/main/resources/application.properties"
  val properties: Properties = new Properties()
  val source: BufferedSource = Source.fromFile(propertiesFile)
  properties.load(source.bufferedReader())
  val driversFileName: String = properties.getProperty("input_file_drivers")
  val tripsFileName: String = properties.getProperty("input_file_trips")
  val cityNameToCheck: String = properties.getProperty("city_name_to_check")
  val distance: String = properties.getProperty("min_distance")
  val lines: RDD[String] = readFile(tripsFileName);

  def readFile(fileName: String): RDD[String] = sc.textFile(fileName)

  def countNumberOfLines(): Unit = {
    System.out.println("Number of lines in files " + tripsFileName + " is " + lines.count())
  }

  def filterOutCityToCheck(): RDD[(String, String)] = lines.map((line: String) => {
    val words = line.split(" ")
    Tuple2(words(1), words(2))
  })
    .filter((words: Tuple2[String, String]) => words._1.equalsIgnoreCase(cityNameToCheck)).persist(StorageLevel.MEMORY_AND_DISK)

  def calculateNumberOfTripsLongerThanXKm(persist: RDD[(String, String)]): Unit = {
    System.out.println("Number of trips to " + cityNameToCheck + " longer than " + distance + " km is: " + persist.filter((words: Tuple2[String, String]) => words._2.toInt > distance.toInt).count)
  }

  def calculateTotalTripsDistanceToSpecificCity(persist: RDD[(String, String)]): Unit = {
    System.out.println("Total killomitrage of trips to " + cityNameToCheck + " is: " + persist.flatMap((words: Tuple2[String, String]) => words._2).sum())
  }

  def findBestDrivers(): Unit = {
    val bestDrivers = lines.map((line: String) => {
      val words = line.split(" ")
      Tuple2[String, Integer](words(0), words(2).toInt)
    }
    ).reduceByKey(_ + _).map(values => values.swap)
      .sortByKey(ascending = false).map((values: Tuple2[Integer, String]) => values._2).collect();
    if (bestDrivers.length >= 3) findBestDriversNames(bestDrivers)
    else throw new RuntimeException("There are less than 3 drivers")
  }

  def findBestDriversNames(bestDrivers: Array[String]): Unit = {
    val bestDriversNames = readFile(driversFileName).map((line: String) => {
      val words = line.split(", ")
      new Tuple2[String, String](words(0), words(1))
    }).filter((words: Tuple2[String, String]) => words._1.equalsIgnoreCase(bestDrivers(0)) || words._1.equalsIgnoreCase(bestDrivers(1)) || words._1.equalsIgnoreCase(bestDrivers(2))).collectAsMap
    System.out.println("Names of best drivers:")
    for (i <- 0 until 3) {
      System.out.println(bestDriversNames.get(bestDrivers(i)))
    }
  }
}
