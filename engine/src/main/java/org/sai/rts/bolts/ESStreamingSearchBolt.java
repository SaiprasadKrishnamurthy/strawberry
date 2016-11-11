package org.sai.rts.bolts;

import org.apache.storm.shade.org.apache.commons.lang.RandomStringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * @author Sai Kris.
 */
public class ESStreamingSearchBolt extends BaseRichSpout {
    private String movieInputFileName;
    private SpoutOutputCollector _collector;
    private static final Random RANDOM = new Random();

    public ESStreamingSearchBolt(final String movieName) {
        this.movieInputFileName = movieName;
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("fullName", "gender", "dateOfBirth", "nationality", "placeOfBirth", "passportNumber", "movieName"));
    }

    @Override
    public void open(final Map map, final TopologyContext topologyContext, final SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        try {
            List<String> movieNames = Files.lines(Paths.get(movieInputFileName)).collect(Collectors.toList());
            for (String movieName : movieNames) {
                Utils.sleep(100);
                boolean isMale = System.currentTimeMillis() % 2 == 0;
                String gender = isMale ? "M" : "F";
                int yearsSubtract = RANDOM.nextInt(50);
                long dob = System.currentTimeMillis() - (yearsSubtract * 365 * 24 * 60 * 60 * 1000) - (RANDOM.nextInt(365) * 365 * 24 * 60 * 60 * 1000);
                String nationality = Locale.getISOCountries()[RANDOM.nextInt(Locale.getISOCountries().length - 1)];
                String passportNumber = RandomStringUtils.randomAlphanumeric(20);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
