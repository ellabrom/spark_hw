package com.epam.spark_hw.java.handler;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Component
public class TripsHandler implements  Serializable {
    transient JavaSparkContext sc;
    @Value("${input_file_drivers}")
    private String driversFileName;

    @Value("${input_file_trips}")
    private String tripsFileName;

    @Value("${city_name_to_check}")
    private String cityNameToCheck;

    @Value("${min_distance}")
    private String distance;

    public TripsHandler() {
        SparkConf sparkConf = new SparkConf().setAppName("homework").setMaster("local[*]");
        sc = new JavaSparkContext(sparkConf);
    }
    public JavaRDD<String> readFile(String fileName) {
        return sc.textFile(fileName);
    }

    public void countNumberOfLines(JavaRDD<String> lines) {
        System.out.println("Number of lines in files " + tripsFileName + " is " + lines.count());
    }

    public JavaPairRDD<String, String> filterOutCityToCheck(JavaRDD<String> lines) {
        return lines.mapToPair(line -> {
            String[] words = line.split(" ");
            return new Tuple2<>(words[1], words[2]);
        }).filter(words -> words._1.equalsIgnoreCase(cityNameToCheck)).persist(StorageLevel.MEMORY_AND_DISK());
    }

    public void calculateNumberOfTripsLongerThanXKm(JavaPairRDD<String, String> persist) {
        System.out.println("Number of trips to " + cityNameToCheck + " longer than " + distance + " km is: " +
                persist.filter(words -> Integer.parseInt(words._2) > Integer.parseInt(distance)).count());
    }

    public void calculateTotalTripsDistanceToSpecificCity(JavaPairRDD<String, String> persist) {
        System.out.println("Total killomitrage of trips to " + cityNameToCheck + " is: " +
                persist.mapToDouble(words -> Double.parseDouble(words._2)).sum());
    }

    public void findBestDrivers(JavaRDD<String> tripsFile) {
        List<String> bestDrivers = tripsFile.mapToPair(line -> {
            String[] words = line.split(" ");
            return new Tuple2<>(words[0], Integer.parseInt(words[2]));
        }).reduceByKey(Integer::sum).mapToPair(Tuple2::swap).sortByKey(false).map(values -> values._2).toArray();
        if (bestDrivers.size() >= 3) {
            findBestDriversNames(bestDrivers);
        } else {
            throw new RuntimeException("There are less than 3 drivers");

        }
    }

    public void findBestDriversNames(List<String> bestDrivers) {
        Map<String, String> bestDriversNames = readFile(driversFileName).mapToPair(line -> {
            String[] words = line.split(", ");
            return new Tuple2<>(words[0], words[1]);
        }).filter(words -> words._1.equalsIgnoreCase(bestDrivers.get(0)) || words._1.equalsIgnoreCase(bestDrivers.get(1)) || words._1.equalsIgnoreCase(bestDrivers.get(2)))
                .collectAsMap();

        System.out.println("Names of best drivers:");
        for (int i = 0; i < 3; i++) {
            System.out.println(bestDriversNames.get(bestDrivers.get(i)));
        }
    }
}
