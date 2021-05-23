package com.epam.spark_hw.java.flow_controller;

import com.epam.spark_hw.java.handler.TripsHandler;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class TripsFlowControllerImpl implements TripsFlowController {
    @Value("${input_file_trips}")
    private String tripsFileName;



    @Override
    public void handle( ConfigurableApplicationContext context) {
        TripsHandler tripsHandler = context.getBean(TripsHandler.class);
        JavaRDD<String> tripsFile = tripsHandler.readFile(tripsFileName);
        tripsHandler.countNumberOfLines(tripsFile);
        JavaPairRDD<String, String> persist = tripsHandler.filterOutCityToCheck(tripsFile);
        tripsHandler.calculateNumberOfTripsLongerThanXKm(persist);
        tripsHandler.calculateTotalTripsDistanceToSpecificCity(persist);
        tripsHandler.findBestDrivers(tripsFile);
    }


}



