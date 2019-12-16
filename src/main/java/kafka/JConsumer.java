package kafka;


import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;



public class JConsumer implements Runnable
{

    private KafkaConsumer<String, String> consumer;

    private JConsumer()
    {
        Properties props = new Properties();

        props.put("bootstrap.servers", ConfigureAPI.BROKER_LIST);
        props.put("group.id", ConfigureAPI.GROUP_ID);
        props.put("enable.auto.commit", true);
        props.put("auto.commit.interval.ms", 1000);
        props.put("session.timeout.ms", 30000);
        props.put("zookeeper.connect", "192.168.1.71:2181");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(ConfigureAPI.TOPIC)); // 多个topic逗号隔开

    }

    @Override
    public void run()
    {
        // TODO Auto-generated method stub
        while (true)
        {
//            System.out.println("poll Server message");
            ConsumerRecords<String, String> records = consumer.poll(ConfigureAPI.GET_MEG_INTERVAL);
            for (ConsumerRecord<String, String> record : records)
            {
                handleMeg(record.value());

            }
        }

    }

    private void handleMeg(String record)
    {

        System.out.println(record);

    }

    public static void main(String[] args)
    {
        ExecutorService threadPool = Executors.newCachedThreadPool();
        threadPool.execute(new JConsumer());
        threadPool.shutdown();
    }
}