package com.example.springkafkadocker.springkafkadockerint;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class KafkaConsumer {

    @KafkaListener(topics={"my_topic"}, groupId="my_group_id")
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        List<GenericRecord> records = (List<GenericRecord>) cr.value();
        records.forEach(r -> {
            System.out.println( r.toString() );
        } );
    }
}
