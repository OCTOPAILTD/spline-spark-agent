package src.main.java.za.co.absa.spline.example.batch;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ListKafkaTopics {
    public static void main(String[] args) {
        String bootstrapServers = "192.168.1.102:9093";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(props)) {
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(true); // include internal topics like __consumer_offsets

            ListTopicsResult topics = adminClient.listTopics(options);
            Set<String> names = topics.names().get();

            System.out.println("📜 List of topics:");
            for (String name : names) {
                System.out.println(" - " + name);
            }
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("❌ Failed to list topics: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
