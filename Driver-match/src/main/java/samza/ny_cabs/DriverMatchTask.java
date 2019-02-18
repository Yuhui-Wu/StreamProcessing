package samza.ny_cabs;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import java.util.HashMap;
import java.util.Map;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask {

    private KeyValueStore<String, Map<String, Map<String, Object>>> driverLoc;

    private double MAX_MONEY = 100.0;

    private Double distance(Double lat1, Double lon1, Double lat2, Double lon2) {
        Double deltaLat = Math.abs(lat1 - lat2);
        Double deltaLon = Math.abs(lon1 - lon2);
        return Math.sqrt(deltaLat * deltaLat + deltaLon * deltaLon);
    }


    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) throws Exception {
        // Initialize
        driverLoc = (KeyValueStore<String, Map<String, Map<String, Object>>>) context
                .getStore("driver-loc");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope,
                        MessageCollector collector,
                        TaskCoordinator coordinator) {

        //All the messsages are partitioned by blockId
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
	    // Handle Driver Location messages
            try {
                Map<String, Object> driverLocations = (Map<String, Object>) envelope.getMessage();

                // get the blockId from map and find the driver list of this block
                Integer blockId = (Integer) driverLocations.get("blockId");
                Map<String, Map<String, Object>> driverList= driverLoc.get(blockId.toString());

                Integer driverId = (Integer) driverLocations.get("driverId");

                if (driverList != null && driverId != null) {
                    if (driverList.containsKey(driverId.toString())) {
                        Map<String, Object> driverInfo = driverList.get(driverId.toString());

                        Double latitude = (Double) driverLocations.get("latitude");
                        Double longitude = (Double) driverLocations.get("longitude");

                        if (driverLocations.containsKey("latitude")) {
                            driverInfo.put("latitude", latitude);
                        }

                        if (driverLocations.containsKey("longitude")) {
                            driverInfo.put("longitude", longitude);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }


//            try {
//
//                // get the blockId from json and find the driver list of this block
//                Map<Integer, JSONObject> driverList= driverLoc.get(blockId);
//
//                // get the driverId from json and find the driver info from the map
//                int driverId = driverLocations.getInt("driverId");
//                if (driverList != null && driverList.containsKey(driverId)) {
//                    JSONObject driverInfo = driverList.get(driverId);
//
//                    // update current location
//                    double latitude = driverLocations.getDouble("latitude");
//                    double longitude = driverLocations.getDouble("longitude");
//                    driverInfo.put("latitude", latitude);
//                    driverInfo.put("longitude", longitude);
//
//                    driverLoc.put(blockId, driverList);
//                }
//            } catch (Exception e) {
//                collector.send(new OutgoingMessageEnvelope(
//                        DriverMatchConfig.LOG_STREAM, e.getMessage()));
//            }


        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
	    // Handle Event messages
            try {
                Map<String, Object> events = (Map<String, Object>) envelope.getMessage();
                Integer blockId = (Integer) events.get("blockId");
                Map<String, Map<String, Object>> driverList= driverLoc.get(blockId.toString());

                String type = (String) events.get("type");

                Double latitude = (Double) events.get("latitude");
                Double longitude = (Double) events.get("longitude");

                switch (type) {
                    case "RIDE_REQUEST": {
                        Integer clientId = (Integer) events.get("clientId");
                        if (clientId != null) {
                            String genderPreference = (String) events.get("gender_preference");
                            Double maxScore = 0.0;
                            String driverId = "";
                            for (Map.Entry<String, Map<String, Object>> kv : driverList.entrySet()) {
                                Map<String, Object> info = kv.getValue();
                                Double distanceScore = 0.0;
                                Double genderScore = 0.0;
                                Double ratingScore = 0.0;
                                Double salaryScore = 0.0;
                                if (driverList.size() > 1 && (info.get("status")).equals("AVAILABLE")) {
                                    if (info.containsKey("latitude") && info.containsKey("longitude")) {
                                        Double dist = distance(
                                                (Double)info.get("latitude"),
                                                (Double)info.get("longitude"),
                                                latitude,
                                                longitude
                                                );
                                        distanceScore = 1 * Math.exp(-1 * dist);
                                    }

                                    if (info.containsKey("gender")) {
                                        String gender = (String) info.get("gender");
                                        genderScore = gender.equals(genderPreference) || gender.equals("N") ? 1.0 : 0.0;
                                    }

                                    if (info.containsKey("rating")) {
                                        Double rating = (Double) info.get("rating");
                                        ratingScore = rating / 5.0;
                                    }

                                    if (info.containsKey("salary")) {
                                        Integer salary = (Integer) info.get("salary");
                                        salaryScore = 1 - salary / 100.0;
                                    }

                                    double matchScore = distanceScore * 0.4 +
                                        genderScore * 0.1 + ratingScore * 0.3 + salaryScore * 0.2;

                                    if (matchScore >= maxScore) {
                                        maxScore = matchScore;
                                        driverId = kv.getKey();
                                    }
                                } else if (driverList.size() <= 1 && (info.get("status")).equals("AVAILABLE")) {
                                    driverId = kv.getKey();
                                }
                            }

                            Map<String, Object> message = new HashMap<>();
                            message.put("clientId", clientId);
                            message.put("driverId", driverId);
                            collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, message));
                            System.out.println(clientId + ": " + driverId);
                            driverList.remove(driverId);
                            break;

                        }
                    }

                    case "LEAVING_BLOCK": {
                        Integer driverId = (Integer) events.get("driverId");
                        if (driverId != null) {
                            driverList.remove(driverId.toString());
                        }
                        break;
                    }

                    case "ENTERING_BLOCK": {
                        if (driverList == null) {
                            driverList = new HashMap<>();
                        }
                        Integer driverId = (Integer) events.get("driverId");
                        if (driverId != null) {
                            if (!driverList.containsKey(driverId.toString())) {
                                driverList.put(driverId.toString(), new HashMap<>());
                            }

                            Map<String, Object> driverInfo = driverList.get(driverId.toString());

                            if (events.containsKey("latitude")) {
                                driverInfo.put("latitude", latitude);
                            }

                            if (events.containsKey("longitude")) {
                                driverInfo.put("longitude", longitude);
                            }

                            if (events.containsKey("gender")) {
                                driverInfo.put("gender", events.get("gender"));
                            }

                            if (events.containsKey("rating")) {
                                driverInfo.put("rating", events.get("rating"));
                            }

                            if (events.containsKey("salary")) {
                                driverInfo.put("salary", events.get("salary"));
                            }

                            if (events.containsKey("status")) {
                                driverInfo.put("status", events.get("status"));

                            }

                            driverLoc.put(blockId.toString(), driverList);

                        }

                        break;

                    }
                    case "RIDE_COMPLETE": {
                        Integer driverId = (Integer) events.get("driverId");
                        if (driverId != null) {
                            Map<String, Object> driverInfo = driverList.get(driverId.toString());

                            if (events.containsKey("latitude")) {
                                driverInfo.put("latitude", latitude);
                            }

                            if (events.containsKey("longitude")) {
                                driverInfo.put("longitude", longitude);
                            }

                            Double rating = (Double) events.get("rating");
                            Double userRating = (Double) events.get("user_rating");

                            if (rating != null && userRating != null) {
                                driverInfo.put("rating", (rating + userRating) / 2);
                            } else if (rating == null && userRating != null) {
                                driverInfo.put("rating", userRating);
                            } else if (rating != null) {
                                driverInfo.put("rating", rating);
                            }

                            if (events.containsKey("gender")) {
                                driverInfo.put("gender", events.get("gender"));
                            }

                            if (events.containsKey("salary")) {
                                driverInfo.put("salary", events.get("salary"));

                            }

                            driverInfo.put("status", "AVAILABLE");
                            driverLoc.put(blockId.toString(), driverList);
                            break;

                        }

                    }



                }
            } catch (Exception e) {
                e.printStackTrace();
                collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.LOG_STREAM, e.getMessage()));
            }

        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }


}
