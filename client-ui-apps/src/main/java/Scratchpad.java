import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by saipkri on 20/11/16.
 */
public class Scratchpad {
    public static void main(String[] args) throws Exception{
        Files.lines(Paths.get("world_cities.csv"))
                .sorted()
                .forEach(line -> {
                    String[] t = line.split(",");
                    System.out.println("<option value=\""+t[3]+","+t[2]+"\">"+t[0]+"</option>");
                });
    }
}
