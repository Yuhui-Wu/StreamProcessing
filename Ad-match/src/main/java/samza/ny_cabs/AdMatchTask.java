package samza.ny_cabs;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import org.json.*;
import java.util.*;
import java.io.InputStream;


/**
 * Consumes the stream of events.
 * Outputs a stream which handles static file and one stream 
 * and gives a stream of advertisement matches.
 */
public class AdMatchTask implements StreamTask, InitableTask {
    
    private KeyValueStore<Integer, Map<String, Object>> userInfo;

    private KeyValueStore<String, Map<String, Object>> yelpInfo;

    private static double distance(double lat1, double lon1, double lat2, double lon2) {
        if ((lat1 == lat2) && (lon1 == lon2)) {
            return 0;
        }
        else {
            double theta = lon1 - lon2;
            double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2)) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
            dist = Math.acos(dist);
            dist = Math.toDegrees(dist);
            dist = dist * 60 * 1.1515;
            return (dist);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) throws Exception {
        // Initialize kv store

        userInfo = (KeyValueStore<Integer, Map<String, Object>>) context.getStore("user-info");
        yelpInfo = (KeyValueStore<String, Map<String, Object>>) context.getStore("yelp-info");

        //Initialize static data and save them in kv store
        initialize();
    }

    /**
     * Read the static data from resources folder 
     * and save data in KV store.
     */
    
     public void initialize(){
        ClassLoader classLoader = this.getClass().getClassLoader();

        // Read "UserInfoData.json" from resources folder
        InputStream s = classLoader.getResourceAsStream("UserInfoData.json");
        Scanner scanner = new Scanner(s);
        
        while(scanner.hasNextLine()){
            // read data and put them in KV store
            Map<String, Object> map = new HashMap<>();
            String line = scanner.nextLine();
            JSONObject obj = new JSONObject(line);
            int userId = obj.getInt("userId");
            for(String k:obj.keySet()){
                map.put(k,obj.get(k));
            }
            userInfo.put(userId,map);
        }

        s = classLoader.getResourceAsStream("NYCstore.json");
        scanner = new Scanner(s);
        
        while(scanner.hasNextLine()){
            // read data and put them in KV store
            String line = scanner.nextLine();
            Map<String, Object> map = new HashMap<>();
            JSONObject obj = new JSONObject(line);
            String storeId = obj.getString("storeId");
            for(String k:obj.keySet()){
                map.put(k,obj.get(k));
            }

            yelpInfo.put(storeId, map);
        }
        scanner.close();
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        /*
        All the messsages are partitioned by blockId
        */
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        if (incomingStream.equals(AdMatchConfig.EVENT_STREAM.getStream())) {
	    // Handle Event messages
            try {
                Map<String, Object> event = (Map<String, Object>) envelope.getMessage();
                String type = (String) event.get("type");
                switch (type) {
                    case "RIDER_INTEREST": {
                        Integer duration = (Integer) event.get("duration");
                        if (duration != null && duration > 300000) {

                            Integer userId = (Integer) event.get("userId");
                            Map<String, Object> info = userInfo.get(userId);

                            if (event.containsKey("interest")) {
                                info.put("interest", event.get("interest"));
                            }
                        }
                    }
                    case "RIDE_REQUEST": {
                        Integer clientId = (Integer) event.get("clientId");
                        Double lat1 = (Double) event.get("latitude");
                        Double lon1 = (Double) event.get("longitude");

                        if (clientId != null) {
                            Map<String, Object> info = userInfo.get(clientId);
                            if (info == null) {
                                return;
                            }
                            Integer age = (Integer) info.get("age");
                            String interest = (String) info.get("interest");
                            Integer travelCount = (Integer) info.get("integer");
                            String device = (String) info.get("device");

                            int deviceValue = 0;
                            switch (device) {
                                case "iPhone XS": {
                                    deviceValue = 3;
                                    break;
                                }
                                case "iPhone 7": {
                                    deviceValue = 2;
                                    break;
                                }
                                case "iPhone 5": {
                                    deviceValue = 1;
                                    break;
                                }
                            }

                            KeyValueIterator<String, Map<String, Object>> iterator = yelpInfo.all();

                            double maxScore = 0;
                            String storeId = "";
                            String name = "";
                            while (iterator.hasNext()) {
                                Map<String, Object> store = iterator.next().getValue();
                                String categories = (String) store.get("categories");
                                if (!interest.equals(categories)) {
                                    continue;
                                }

                                double score = 0;

                                // initial score
                                Integer reviewCount = (Integer) store.get("review_count");
                                Double rating = (Double) store.get("rating");
                                score = reviewCount * rating;

                                // price match
                                String price = (String) store.get("price");
                                int priceValue = 0;
                                if (price.equals("$$$$") || price.equals("$$$")) {
                                    priceValue = 3;
                                } else if (price.equals("$$")) {
                                    priceValue = 2;
                                } else if (price.equals("$")) {
                                    priceValue = 1;
                                }

                                score = score * (1 - Math.abs(priceValue - deviceValue) * 0.1);

                                // match the score of age, travel and distance
                                Double lat2 = (Double) store.get("latitude");
                                Double lon2 = (Double) store.get("longitude");

                                double dist = distance(lat1, lon1, lat2, lon2);

                                if (travelCount != null && age != null) {
                                    if (travelCount > 50 || age == 20) {
                                        if (dist > 10) {
                                            score = score * 0.1;
                                        }
                                    } else {
                                        if (dist > 5) {
                                            score = score * 0.1;
                                        }
                                    }
                                }

                                if (score >= maxScore) {
                                    maxScore = score;
                                    storeId = (String) store.get("storeId");
                                    name = (String) store.get("name");
                                }

                            }

                            Map<String, Object> message = new HashMap<>();
                            message.put("userId", clientId);
                            message.put("storeId", storeId);
                            message.put("name", name);
                            collector.send(new OutgoingMessageEnvelope(AdMatchConfig.AD_STREAM, message));
//                            System.out.println(clientId + ": " + storeId);
                        }
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }
}
