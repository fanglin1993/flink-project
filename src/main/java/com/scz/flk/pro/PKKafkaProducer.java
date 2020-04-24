package com.scz.flk.pro;

import com.scz.flk.util.DateUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Created by shen on 2019/12/24.
 */
public class PKKafkaProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        String topic = "test";

        Random random = new Random();
        int i = 0;
        // 通过死循环一直不停往Kafka的Broker里面生产数据
        while (i++ < 2000) {
            try {
                String msg = getMsg(random);
                System.out.println(msg);
                producer.send(new ProducerRecord<String, String>(topic, msg));
                Thread.sleep(1000);
            } catch (Exception e) {
                break;
            }
        }
    }

    private static String getMsg(Random random) {
        StringBuilder builder = new StringBuilder();
        String[] domains = new String[]{
                "v1.go2yd.com",
                "v2.go2yd.com",
                "v3.go2yd.com",
                "v4.go2yd.com",
                "vmi.go2yd.com"
        };
        String[] ips = new String[]{
                "223.104.18.110",
                "113.101.75.194",
                "27.17.127.135",
                "183.225.139.16",
                "112.1.66.34",
                "175.148.211.190",
                "183.227.58.21",
                "59.83.198.84",
                "117.28.38.28",
                "117.59.39.169"
        };
        String[] levels = new String[]{"M","E"};
        int traffic = random.nextInt(10000);
        builder.append("imooc").append("\t")
                .append("CN").append("\t")
                .append(levels[random.nextInt(levels.length)]).append("\t")
                .append(DateUtils.format(new Date())).append("\t")
                .append(ips[random.nextInt(ips.length)]).append("\t")
                .append(domains[random.nextInt(domains.length)]).append("\t")
                .append(traffic);
        return builder.toString();
    }

}
