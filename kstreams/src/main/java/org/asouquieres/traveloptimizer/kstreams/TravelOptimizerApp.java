package org.asouquieres.traveloptimizer.kstreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


// Comes with kstreamplify-spring-boot dependency
@SpringBootApplication
public class TravelOptimizerApp {
    public static void main(String[] args) {
        SpringApplication.run(TravelOptimizerApp.class, args);
    }
}
