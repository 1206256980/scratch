package com.binance.index;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class BinanceIndexApplication {

    public static void main(String[] args) {
        SpringApplication.run(BinanceIndexApplication.class, args);
    }
}
