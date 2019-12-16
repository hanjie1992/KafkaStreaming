package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MyProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.71:9092");
        props.put("acks", "1");
        props.put("delivery.timeout.ms", 30000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 50; i++) {
           producer.send(new ProducerRecord<String, String>("wangtianxiong", Integer.toString(i), Integer.toString(i * 10)));
           //System.out.println(f.get());
           System.out.println("hello: " + i + "-> " + i * 10);
        }
        producer.close();


//        Properties props = new Properties();
//        props.put("bootstrap.servers", "192.168.1.71:9092");
//        props.put("zookeeper", "192.168.1.71:2181,192.168.1.72:2181,192.168.1.73:2181/kafka");
//        props.put("transactional.id", "my-transactional-id");
//        Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
//
//        producer.initTransactions();
//
//        try {
//            producer.beginTransaction();
//            for (int i = 0; i < 100; i++) {
//                System.out.println("hello ");
//                producer.send(new ProducerRecord<>("wangtianxiong", Integer.toString(i), Integer.toString(i)));
//                System.out.println(i + ":" + i);
//            }
//            producer.commitTransaction();
//        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
//            // We can't recover from these exceptions, so our only option is to close the producer and exit.
//            producer.close();
//        } catch (KafkaException e) {
//            // For all other exceptions, just abort the transaction and try again.
//            producer.abortTransaction();
//        }
//        producer.close();
    }
}
