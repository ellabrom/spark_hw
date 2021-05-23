package com.epam.spark_hw.java;

import com.epam.spark_hw.java.flow_controller.TripsFlowControllerImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;


@SpringBootApplication


public class SparkHWJava {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\hadoop-common-2.2.0-bin-master\\");
        ConfigurableApplicationContext context =
                SpringApplication.run(SparkHWJava.class, args);
        TripsFlowControllerImpl flowManager = context.getBean(TripsFlowControllerImpl.class);
        flowManager.handle(context);
    }
}
