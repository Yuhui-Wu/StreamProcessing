import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class DataProducer {
    private static File file;
    private static BufferedInputStream fis;
    private static BufferedReader reader;

    public static void main(String[] args) throws IOException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "172.31.45.68:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,  String> producer = new KafkaProducer<>(props);

        file = new File("tracefile");
        fis = new BufferedInputStream(new FileInputStream(file));
        reader = new BufferedReader(new InputStreamReader(fis, "utf-8"));

        String line;
        while ((line = reader.readLine()) != null) {
            JSONObject value = new JSONObject(line);
            String type = value.getString("type");
            int blockId = value.getInt("blockId");
            if (type.equals("DRIVER_LOCATION")) {
//                System.out.println("driver-locations " + Integer.toString(blockId % 5) + " " + line);
                producer.send(new ProducerRecord<>("driver-locations", blockId % 5, Integer.toString(blockId), line));
            } else {
//                System.out.println("events " + Integer.toString(blockId % 5) + " " + line);
                producer.send(new ProducerRecord<>("events", blockId % 5, Integer.toString(blockId), line));
            }
        }
        producer.close();
    }
}
