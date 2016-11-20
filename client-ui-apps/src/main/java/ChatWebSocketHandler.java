import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

@WebSocket
public class ChatWebSocketHandler {

    private Set<String> subscribed = new HashSet<>();

    @OnWebSocketConnect
    public void onConnect(Session user) throws Exception {
        System.out.println("On connect: " + user);
    }

    @OnWebSocketClose
    public void onClose(Session user, int statusCode, String reason) {
        Chat.userNameToTopicMap.remove(user);
    }

    @OnWebSocketMessage
    public void onMessage(final Session userSession, String topic) {
        Chat.sessionsToTopicsMap.computeIfPresent(userSession, (key, topics) -> {
            if (!topics.contains(topic)) {
                topics.add(topic);
            }
            return topics;
        });

        Chat.sessionsToTopicsMap.computeIfAbsent(userSession, session -> {
            ArrayList<String> _topics = new ArrayList<>();
            _topics.add(topic);
            return _topics;
        });
    }

}
