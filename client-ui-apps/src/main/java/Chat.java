import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.eclipse.jetty.websocket.api.Session;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
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
        System.out.println("Publish called: "+topic);
        sessionsToTopicsMap.entrySet()
                .stream()
                .filter(kv -> kv.getKey().isOpen())
                .forEach(entry -> {
                    List<String> topics = entry.getValue();
                    if (topics.contains(topic)) {
                        try {
                            entry.getKey().getRemote().sendString(String.valueOf(new JSONObject()
                                    .put("userMessage", createHtmlMessageFromSender("["+ topic + "]  ", message))
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
