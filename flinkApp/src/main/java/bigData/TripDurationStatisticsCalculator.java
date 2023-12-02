package bigData;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TripDurationStatisticsCalculator extends ProcessAllWindowFunction<OsloRide, TripDurationStatistics, TimeWindow> {

    @Override
    public void process(ProcessAllWindowFunction<OsloRide, TripDurationStatistics, TimeWindow>.Context context,
                        Iterable<OsloRide> elements, Collector<TripDurationStatistics> out) throws Exception {
        float sum = 0;
        float max = 0;
        float min = Float.MAX_VALUE;
        float avg;
        String station1 = "";
        int numRides1 = 0;
        String station2 = "";
        int numRides2 = 0;
        String station3 = "";
        int numRides3 = 0;
        float count = 0;

        Map<String, Integer> popular = new HashMap<>();

        for (OsloRide msg : elements) {
            count++;
            float duration = msg.duration;
            sum += duration;

            if (duration > max) {
                max = duration;
            }
            if (duration < min) {
                min = duration;
            }

            popular.put(msg.end_station_name, popular.getOrDefault(msg.end_station_name, 0) + 1);
        }

        avg = sum / count;

        int index = 0;
        for (String station : popular.keySet()) {
            if (index == 0) {
                station1 = station;
                numRides1 = popular.get(station);
            } else if (index == 1) {
                station2 = station;
                numRides2 = popular.get(station);
            } else if (index == 2) {
                station3 = station;
                numRides3 = popular.get(station);
            }
            index++;
            if (index >= 3) {
                break;
            }
        }

        Date date = new Date();
        TripDurationStatistics result = new TripDurationStatistics(date, max, min, avg, station1, numRides1, station2, numRides2, station3, numRides3);
        
        out.collect(result);
    }
}
