package com.github.q225zhan;

import com.google.cloud.pubsub.spi.v1.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;

import java.io.IOException;

/**
 * Created by q225zhan on 2017-03-31.
 */
public class PubSubSDK {

    private String project;

    PubSubSDK(String project) throws IOException {
        this.project = project;
    }

    void createTopic(String topic) throws Exception {
        try (PublisherClient publisherClient = PublisherClient.create()) {
            publisherClient.createTopic(TopicName.create(project, topic));
        }
    }

    void send(String topic, String message) throws IOException {
        TopicName topicName = TopicName.create(project, topic);
        Publisher publisher = Publisher.newBuilder(topicName).build();
        PubsubMessage pm = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(message)).build();
        publisher.publish(pm);
    }

    void createSubscriber(String topic, String subscriber) throws Exception {
        try (SubscriberClient subscriberClient = SubscriberClient.create()) {
            SubscriptionName subscriptionName = SubscriptionName.create(project, subscriber);
            subscriberClient.createSubscription(
                    subscriptionName,
                    TopicName.create(project, topic), null, 60);
            Subscriber.newBuilder(subscriptionName, (message, consumer) -> {
                System.out.println(message.toString());
                consumer.accept(AckReply.ACK, null);
            }).build().startAsync();
        }
    }

}
