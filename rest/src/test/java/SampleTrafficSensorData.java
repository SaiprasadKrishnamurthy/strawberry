import com.fasterxml.jackson.databind.ObjectMapper;
import com.thedeanda.lorem.Lorem;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.web.client.RestTemplate;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;

/**
 * Created by saipkri on 27/10/16.
 */
public class SampleTrafficSensorData {

    public static void main(String[] args) throws Exception {

        String dataTemplate = IOUtils.toString(SampleTrafficSensorData.class.getClassLoader().getResourceAsStream("traffic_sensor_data_feed_template.json"));

        int size = 1000;
        DateTime dateTime = new DateTime();
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
        Random r = new Random();
        RestTemplate restTemplate = new RestTemplate();
        ObjectMapper mapper = new ObjectMapper();

        int start = r.nextInt(100000);

        for (int i = start; i < start + size + 1; i++) {
            String timestamp = f.format(dateTime.withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneId.of("Europe/London")))).minusHours(r.nextInt(3)).minusMinutes(r.nextInt(30)).toDate());
            System.out.println(timestamp);
            int noOfVehiclesEntered = r.nextInt(20);
            int noOfVehiclesExited = r.nextInt(10);
            String reg1 = "TN-" + r.nextInt(100000);
            String reg2 = "KA-" + r.nextInt(100000);
            String reg3 = "KL-" + r.nextInt(100000);
            int speed1 = r.nextInt(80);
            int speed2 = r.nextInt(100);
            int speed3 = r.nextInt(65);

            String template = dataTemplate.replace("$noOfVehiclesEntered", noOfVehiclesEntered + "")
                    .replace("$noOfVehiclesExited", noOfVehiclesExited + "")
                    .replace("$timestamp", timestamp)
                    .replace("$reg1", reg1)
                    .replace("$speed1", speed1 + "")
                    .replace("$reg2", reg2)
                    .replace("$speed2", speed2 + "")
                    .replace("$reg3", reg3)
                    .replace("$speed3", speed3 + "")
                    .replace("$slowestVehicleSpeed", r.nextInt(40) + "")
                    .replace("$fastestVehicleSpeed", r.nextInt(100) + "");
            Map doc = mapper.readValue(template, Map.class);
            System.out.println(doc);
            System.out.println(restTemplate.postForObject("http://localhost:9090/streamingsearch/traffic-sensor-stream/" + i + "?groupedFieldName=chennai", doc, Object.class));
        }
    }
}
