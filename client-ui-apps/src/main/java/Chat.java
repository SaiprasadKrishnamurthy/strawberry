import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.eclipse.jetty.websocket.api.Session;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.web.client.RestTemplate;
import spark.utils.IOUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static j2html.TagCreator.*;
import static spark.Spark.*;

public class Chat {

    // this map is shared between sessions and threads, so it needs to be thread-safe (http://stackoverflow.com/a/2688817)
    static Map<Session, String> userNameToTopicMap = new ConcurrentHashMap<>();
    static Map<String, List<Session>> topicToSessionMap = new ConcurrentHashMap<>();
    static Map<Session, List<String>> sessionsToTopicsMap = new ConcurrentHashMap<>();
    public static HazelcastInstance hazelcastInstance;

    private static void initHazelcast() {
        String[] memoryGridHosts = {"localhost"};
        Config config = new Config();
        config.getNetworkConfig().setPort(Integer.parseInt("5900"));
        config.getNetworkConfig().setPortAutoIncrement(true);
        NetworkConfig network = config.getNetworkConfig();
        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        Stream.of(memoryGridHosts).forEach(host -> join.getTcpIpConfig().addMember(host).setEnabled(true));
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    }

    public static void main(String[] args) {
        initHazelcast();

        // Get all the available channels to listen to.
        String url = "http://localhost:9090/configs";
        RestTemplate rt = new RestTemplate();
        List<Map> response = rt.getForObject(url, List.class);
        for (Map r : response) {
            Map watches = (Map) r.get("watchQueries");
            watches.forEach((k, v) ->
                    Chat.hazelcastInstance.getTopic(k.toString())
                            .addMessageListener(message -> Chat.publish(k.toString(), message.getMessageObject().toString()))
            );
        }
        staticFiles.location("/public"); //index.html is served at localhost:4567 (default port)
        staticFiles.expireTime(600);
        webSocket("/chat", ChatWebSocketHandler.class);
        final SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 6);
        exception(Exception.class, (ex, rq, rs) -> ex.printStackTrace());
        post("/txn", (rq, rs) -> {
            System.out.println(rq.body());
            String[] tokens = rq.body().split("&");
            String card = tokens[0].split("=")[1];
            String amount = tokens[1].split("=")[1];
            String billingAddress = tokens[2].split("=")[1];
            String txnLoc = tokens[3].split("=")[1];
            String bank = tokens[4].split("=")[1];
            String txnId = UUID.randomUUID().toString();
            String userLocLong = billingAddress.split("%2C")[1];
            String userLocLat = billingAddress.split("%2C")[0];

            String txnLocLong = txnLoc.split("%2C")[1];
            String txnLocLat = txnLoc.split("%2C")[0];

            String txnTemplate = IOUtils.toString(Chat.class.getClassLoader().getResourceAsStream("transaction_template.json"));
            txnTemplate = txnTemplate.replace("$$txnId", txnId).replace("$$cardNo", card).replace("$$userLat", userLocLat)
                    .replace("$$userLong", userLocLong)
                    .replace("$$txnLat", txnLocLat)
                    .replace("$$txnLong", txnLocLong)
                    .replace("$$ts", f.format(cal.getTime()))
                    .replace("$$bank", bank)
                    .replace("$$amount", amount);
            System.out.println(txnTemplate);

            rt.postForObject("http://localhost:9090/eventstream/card-txns", new ObjectMapper().readValue(txnTemplate, Map.class), Map.class);
            rs.redirect("test.html");
            return rs;
        });
        init();
    }

    //Sends a message from one user to all users, along with a list of current usernames
    public static void broadcastMessage(String sender, String message) {
        System.out.println("Hello: " + sender + ", " + message);
        userNameToTopicMap.keySet().stream().filter(Session::isOpen).forEach(session -> {
            try {
                session.getRemote().sendString(String.valueOf(new JSONObject()
                        .put("userMessage", createHtmlMessageFromSender(sender, message))
                        .put("userlist", userNameToTopicMap.values())
                ));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public static void publish(String topic, String message) {
        System.out.println("Publish called: " + topic);
        sessionsToTopicsMap.entrySet()
                .stream()
                .filter(kv -> kv.getKey().isOpen())
                .forEach(entry -> {
                    List<String> topics = entry.getValue();
                    if (topics.contains(topic)) {
                        try {
                            entry.getKey().getRemote().sendString(String.valueOf(new JSONObject()
                                    .put("userMessage", createHtmlMessageFromSender("[" + topic + "]  ", message))
                                    .put("userlist", topics)
                            ));
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                    }
                });
    }

    //Builds a HTML element with a sender-name, a message, and a timestamp,
    private static String createHtmlMessageFromSender(String sender, String message) {
        return article().with(
                b(sender),
                p(message),
                span().withClass("timestamp").withText(new SimpleDateFormat("HH:mm:ss").format(new Date()))
        ).render();
    }

}
