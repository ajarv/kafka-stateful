package com.powerutil.si;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class StreamProcess {

    @Value("${kafka.brokers}")
    private String kafkaBrokers;
    @Value("${kafka.streams.topic.input}")
    private String kafkaStreamsTopicInput;
    @Value("${kafka.streams.topic.output}")
    private String kafkaStreamsTopicOutput;
    @Value("${kafka.streams.topic.callback}")
    private String kafkaStreamsTopicCallback;
    @Value("${kafka.streams.topic.result}")
    private String kafkaStreamsTopicResult;
    @Value("${kafka.streams.appid}")
    private String kafkaStreamsAppId;

    @PostConstruct
    public void run(){


        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaStreamsAppId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // In the subsequent lines we define the processing topology of the Streams application.


//        final KTable<String, String> counts = source
//                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
//                .groupBy((key, value) -> {
//                    System.out.println(key+" , "+value);
//                    return value;
//                })
//                .reduce((aggvalue,newvalue) -> {
//                    System.out.println(aggvalue+" , "+newvalue);
//                    return newvalue;`
//                });
//        // need to override value serde to Long type
//        counts.toStream().to(kafkaStreamsTopicOutput, Produced.with(Serdes.String(), Serdes.String()));

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> inputPayloadStream = builder.stream(kafkaStreamsTopicInput);
        final KStream<String, String> callbackPayloadStream = builder.stream(kafkaStreamsTopicCallback);


        final KTable<String, String> inputPayloadTable = inputPayloadStream
                // Split each text line, by whitespace, into words.  The text lines are the record
                // values, i.e. we can ignore whatever data is in the record keys and thus invoke
                // `flatMapValues()` instead of the more generic `flatMap()`.

                //.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .map(
                        (key, value) -> KeyValue.pair(key, value))

                // Count the occurrences of each word (record key).
                //
                // This will change the stream type from `KStream<String, String>` to `KTable<String, Long>`
                // (word -> count).  In the `count` operation we must provide a name for the resulting KTable,
                // which will be used to name e.g. its associated state store and changelog topic.
                //
                // Note: no need to specify explicit serdes because the resulting key and value types match our default serde settings
                .groupBy((key, word) -> {
//                    System.out.println(" >> Group By:"+ key+ " -> "+word);

                    return key;
                })
                .reduce((o,n)->{
//                    System.out.println(" >> Reduce  :"+o+ " -> "+n);
                    return n;
                });



        // need to override value serde to Long type
        inputPayloadTable.toStream().to(kafkaStreamsTopicOutput, Produced.with(Serdes.String(), Serdes.String()));
//        inputPayloadTable.toStream().print(Printed.toSysOut());;


        callbackPayloadStream
            .leftJoin(inputPayloadTable,(callbackPayload,inputPayload) -> {
                Map<String, String> rmap= new HashMap<String,String>() {{
                    put("callbackPayload", callbackPayload);
                    put("inputPayload", inputPayload);
                }};
                return rmap.toString();
             }).to(kafkaStreamsTopicResult,Produced.with(Serdes.String(), Serdes.String()));



        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
