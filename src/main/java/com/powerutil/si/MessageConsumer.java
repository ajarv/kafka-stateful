package com.powerutil.si;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.stereotype.Component;

import java.util.Calendar;

/**
 * A simple Camel route that triggers from a timer and calls a bean and prints to system out.
 * <p/>
 * Use <tt>@Component</tt> to make Camel auto detect this route when starting.
 */
@Component
public class MessageConsumer extends RouteBuilder {
    @Override
    public void configure() {

        from("kafka:{{kafka.streams.topic.output}}?brokers={{kafka.brokers}}&groupId={{kafka.consumer.groupid}}&autoOffsetReset=earliest&consumersCount=1")
                .process(new Processor() {
                    @Override
                    public void process(Exchange exchange)
                            throws Exception {
                        String messageKey = "";
                        if (exchange.getIn() != null) {
                            Message message = exchange.getIn();
                            Integer partitionId = (Integer) message
                                    .getHeader(KafkaConstants.PARTITION);
                            String topicName = (String) message
                                    .getHeader(KafkaConstants.TOPIC);
                            if (message.getHeader(KafkaConstants.KEY) != null)
                            {
                                Object _messageKey = message.getHeader(KafkaConstants.KEY);
                                messageKey = _messageKey instanceof  byte[] ? new String((byte[])_messageKey) : ""+_messageKey;
                            }
                            Object data = message.getBody();


                            System.out.println("topicName :: "
                                    + topicName + " partitionId :: "
                                    + partitionId + " messageKey :: "
                                    + messageKey + " message :: "
                                    + data + "\n");
                        }
                    }
                }).to("log:input");


        from("kafka:{{kafka.streams.topic.result}}?brokers={{kafka.brokers}}&groupId={{kafka.consumer.groupid}}&autoOffsetReset=earliest&consumersCount=1")
                .process(new Processor() {
                    @Override
                    public void process(Exchange exchange)
                            throws Exception {
                        String messageKey = "";
                        if (exchange.getIn() != null) {
                            Message message = exchange.getIn();
                            Integer partitionId = (Integer) message
                                    .getHeader(KafkaConstants.PARTITION);
                            String topicName = (String) message
                                    .getHeader(KafkaConstants.TOPIC);
                            if (message.getHeader(KafkaConstants.KEY) != null)
                            {
                                Object _messageKey = message.getHeader(KafkaConstants.KEY);
                                messageKey = _messageKey instanceof  byte[] ? new String((byte[])_messageKey) : ""+_messageKey;
                            }
                            Object data = message.getBody();


                            System.out.println("topicName :: "
                                    + topicName + " partitionId :: "
                                    + partitionId + " messageKey :: "
                                    + messageKey + " message :: "
                                    + data + "\n");
                        }
                    }
                }).to("log:input");

    }

}

