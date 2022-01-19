package com.microservices.demo.twitter.to.kafka.service.listener;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.producer.config.service.KafkaProducer;
import com.microservices.demo.twitter.to.kafka.service.transformer.TwitterStatusToAvroTransformerClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterToKafkaStatusListener extends StatusAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterToKafkaStatusListener.class);

    private final KafkaConfigData kafkaConfigData;
    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;
    private final TwitterStatusToAvroTransformerClass twitterStatusToAvroTransformerClass;

    public TwitterToKafkaStatusListener(KafkaConfigData kafkaConfigData,
                                        KafkaProducer<Long, TwitterAvroModel> kafkaProducer,
                                        TwitterStatusToAvroTransformerClass twitterStatusToAvroTransformerClass) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaProducer = kafkaProducer;
        this.twitterStatusToAvroTransformerClass = twitterStatusToAvroTransformerClass;
    }

    @Override
    public void onStatus(Status status) {
        LOGGER.info("Twitter status text {} sending to kafka topic {}",
                status.getText(), kafkaConfigData.getTopicName());
        TwitterAvroModel twitterAvroModel = twitterStatusToAvroTransformerClass.getTwitterAvroModelFromStatus(status);
        kafkaProducer.send(kafkaConfigData.getTopicName(), twitterAvroModel.getUserId(), twitterAvroModel);
    }
}
