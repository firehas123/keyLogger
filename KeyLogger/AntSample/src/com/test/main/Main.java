package com.test.main;

import com.test.main.conusmer.Consumer;


public class Main {

    public static void main(String[] args) {
        System.out.println("hi");
        try {
            new Consumer().consumerKafkaMessage();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("hi");
    }
}
