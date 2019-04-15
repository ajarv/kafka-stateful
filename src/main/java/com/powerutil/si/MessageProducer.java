package com.powerutil.si;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

/**
 * A simple Camel route that triggers from a timer and calls a bean and prints to system out.
 * <p/>
 * Use <tt>@Component</tt> to make Camel auto detect this route when starting.
 */
@Component
public class MessageProducer extends RouteBuilder {


    @Autowired
    @Qualifier("line_generator_bean")
   private Iterator<String> lineIterator;

    @Override
    public void configure() {
        // First Route is just for demp
//        from("timer:hello?period={{timer.period}}").routeId("hello")
//                .transform().method("greeter_bean", "saySomething")
//                .filter(simple("${body} contains 'foo'"))
//                .to("log:foo")
//                .end()
//                .to("stream:out");
//

        from("timer:namastae?period={{timer.period}}").routeId("namastae")
                .process(new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                String body = lineIterator.hasNext() ? lineIterator.next() : "Shukriya - "+ new Date();
                exchange.getIn().setBody(body,String.class);
                exchange.getIn().setHeader(KafkaConstants.KEY, ""+ Calendar.getInstance().get(Calendar.SECOND));
            }
        }).to("kafka:{{kafka.streams.topic.input}}?brokers={{kafka.brokers}}");


        from("timer:kneehow?period=4000").routeId("kneehow")
                .process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        String body = lineIterator.hasNext() ? lineIterator.next() : "Shea Shea - "+ new Date();
                        exchange.getIn().setBody(body,String.class);
                        exchange.getIn().setHeader(KafkaConstants.KEY, ""+ Calendar.getInstance().get(Calendar.SECOND));
                    }
                }).to("kafka:{{kafka.streams.topic.callback}}?brokers={{kafka.brokers}}");


    }

}

