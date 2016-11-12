import com.strawberry.engine.config.StrawberryConfigHolder;

public class HazelConsumer {
    public static void main(String[] args) throws Exception {
        StrawberryConfigHolder.init("/Users/saipkri/learning/strawberry/engine/src/main/resources/application.properties");
        StrawberryConfigHolder.hazelcastInstance().getTopic("vehicle-camera-sensor-stream-nh45").addMessageListener(message -> System.out.println(message.getMessageObject()));
    }
}

