package kafka;


import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * Hello world!
 */
public class JProducer implements Runnable
{
    private Producer<String, String> producer;

    public JProducer()
    {
        Properties props = new Properties();

        props.put("bootstrap.servers", ConfigureAPI.BROKER_LIST);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("zookeeper.connect", "192.168.1.71:2181");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // request.required.acks
        // 0：这意味着生产者producer不等待来自broker同步完成的确认继续发送下一条（批）消息。此选项提供最低的延迟但最弱的耐久性保证（当服务器发生故障时某些数据会丢失，如leader已死，但producer并不知情，发出去的信息broker就收不到）。
        // 1：这意味着producer在leader已成功收到的数据并得到确认后发送下一条message。此选项提供了更好的耐久性为客户等待服务器确认请求成功（被写入死亡leader但尚未复制将失去了唯一的消息）。
        // -1：这意味着producer在follower副本确认接收到数据后才算一次发送完成。 此选项提供最好的耐久性，我们保证没有信息将丢失，只要至少一个同步副本保持存活。
        // 三种机制，性能依次递减 (producer吞吐量降低)，数据健壮性则依次递增。
        props.put("request.required.acks", "-1");

        producer = new KafkaProducer<String, String>(props);

    }

    @Override
    public void run()
    {
        // TODO Auto-generated method stub
        try
        {
            int a = 0;
            while (true){
                Random random = new Random();
                int number = random.nextInt(1000-1+1)+1;
                String data = "hello "+number;
                producer.send(new ProducerRecord<String, String>(ConfigureAPI.TOPIC, data));
                System.out.println(data);
                Thread.sleep(1000);
                a++;
                if (a>6){
                    break;
                }
            }
        }
        catch (Exception e)
        {
            // TODO: handle exception
            e.getStackTrace();
        }
        finally
        {
            producer.close();
        }

    }

    public static void main(String[] args)
    {
        ExecutorService threadPool = Executors.newCachedThreadPool();
        threadPool.execute(new JProducer());
        threadPool.shutdown();
    }

}