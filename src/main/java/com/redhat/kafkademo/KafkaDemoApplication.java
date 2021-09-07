package com.redhat.kafkademo;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.camel.json.simple.JsonArray;
import org.apache.camel.json.simple.JsonObject;
import org.apache.camel.json.simple.Jsoner;
import org.apache.camel.json.simple.parser.JSONParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class KafkaDemoApplication implements CommandLineRunner {
	
	public static Logger logger = LoggerFactory.getLogger(KafkaDemoApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(KafkaDemoApplication.class, args);
	}
	
	@Autowired
    private KafkaTemplate<String, String> template;

    private final CountDownLatch latch = new CountDownLatch(10);

    @Override
    public void run(String... args) throws Exception {
    	for (int i = 0; i < 10; i++) {
    		this.template.send("test_topic", "java-app", "MENSAJE DE PRUEBA ULTIMO!");
    	}
        latch.await(60, TimeUnit.SECONDS);
        logger.info("All received");
    }

    @KafkaListener(topics = "log_topic_audit")
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        JsonObject json = (JsonObject) new JSONParser().parse(cr.value().toString());
        //json = Jsoner.deserialize("");
        logger.info("JSON: " + json.get("message"));
        //logger.info("RECIBI: value: " + cr.value().toString());
        latch.countDown();
    }

}
