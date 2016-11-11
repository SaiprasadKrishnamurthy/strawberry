import com.strawberry.engine.config.StrawberryConfigHolder;

public class HAzelExample {
    public static void main(String[] args) throws Exception {
        StrawberryConfigHolder.init("/Users/saipkri/learning/strawberry/engine/src/main/resources/application.properties");
        StrawberryConfigHolder.initMongo();
        StrawberryConfigHolder.hazelcastInstance().getTopic("vehicle-camera-sensor-events").addMessageListener(message -> System.out.println(message.getMessageObject()));
    }
}

