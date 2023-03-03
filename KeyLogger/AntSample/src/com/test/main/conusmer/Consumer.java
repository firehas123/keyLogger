package com.test.main.conusmer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.test.main.bean.Person;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;


public class Consumer {

    /*public void consumerKafkaMessage() throws Exception{
//        Logger logger = Logger.getLogger(Consumer.class);
//        logger.setLevel(Level.OFF);
        System.out.println("hi2");
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
//        System.out.println("hi3");
        //KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("TestHan"));
//        ObjectMapper mapper = new ObjectMapper();
        List<Person> personList = new ArrayList<>();
        while (true) {
            //ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            ConsumerRecords<String, byte[]> records = consumer.poll(100);
            for (ConsumerRecord<String, byte[]> record : records) {
                byte[] byteArray = record.value();
                try {
                    ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
                    ObjectInputStream ois = new ObjectInputStream(bis);
                    personList = (List<Person>) ois.readObject();
                    System.out.println("Received message: name = " + personList.get(0).getName() + " age = " + personList.get(0).getAge());
                } catch (Exception e) {
                    System.out.println("Error deserializing message: " + e.getMessage());
                }
            }
//            for (ConsumerRecord<String, String> record : records) {
//                Person person = mapper.readValue(record.value(), Person.class);
//                personList.add(person);
//                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
//                Thread.sleep(5000);
//                System.out.println("hi3");
//                System.out.println("Prinitng value "+ personList.get(0).getName());
//            }
        }
    }*/

//    public void consumerKafkaMessage() throws Exception{
//        System.out.println("hi2");
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        props.put("group.id", "test");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
//
//        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
//        consumer.subscribe(Arrays.asList("TestHan"));
//
//        List<Person> personList = new ArrayList<>();
//        while (true) {
//            ConsumerRecords<String, byte[]> records = consumer.poll(100);
//            for (ConsumerRecord<String, byte[]> record : records) {
//                byte[] byteArray = record.value();
//                try {
//                    ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
//                    ObjectInputStream ois = new ObjectInputStream(bis);
//                    personList = (List<Person>) ois.readObject();
//                    System.out.println("Received message: name = " + personList.get(0).getName() + " age = " + personList.get(0).getAge());
//                } catch (Exception e) {
//                    System.out.println("Error deserializing message: " + e.getMessage());
//                }
//            }
//        }
//    }

    public void consumerKafkaMessage() throws Exception{
        System.out.println("hi2");
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("TestHan"));
        List<Person> personList = null;
        ObjectMapper mapper = new ObjectMapper();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String json = record.value();
                try {
                    personList = mapper.readValue(json, new TypeReference<List<Person>>(){});
                    System.out.println("Received message: name = " + personList.get(0).getName() + " age = " + personList.get(0).getAge());
                } catch (Exception e) {
                    System.out.println("Error deserializing message: " + e.getMessage());
                }
                Thread.sleep(5000);
                display(personList);
            }
        }
    }

    private void display(List<Person> personList) {
        for(int i=0;i<personList.size();i++){
            System.out.println(personList.get(i).toString());
        }
    }

}


