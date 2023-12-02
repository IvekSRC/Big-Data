package bigData;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

import java.util.Date;

@Table(keyspace = "flink", name = "trip_duration_statistics")
public class TripDurationStatistics {
    @Column(name = "record_time")
    public Date recordTime;
    @Column(name = "max_duration")
    public Float maxDuration;
    @Column(name = "min_duration")
    public Float minDuration;
    @Column(name = "avg_duration")
    public Float avgDuration;
    @Column(name = "station_1")
    public String station1;
    @Column(name = "num_rides_1")
    public Integer numRides1;
    @Column(name = "station_2")
    public String station2;
    @Column(name = "num_rides_2")
    public Integer numRides2;
    @Column(name = "station_3")
    public String station3;
    @Column(name = "num_rides_3")
    public Integer numRides3;

    public TripDurationStatistics() {}

    public TripDurationStatistics(Date recordTime, Float maxDuration, Float minDuration, Float avgDuration, String station1, Integer numRides1, String station2, Integer numRides2, String station3, Integer numRides3) {
        this.recordTime = recordTime;
        this.maxDuration = maxDuration;
        this.minDuration = minDuration;
        this.avgDuration = avgDuration;
        this.station1 = station1;
        this.numRides1 = numRides1;
        this.station2 = station2;
        this.numRides2 = numRides2;
        this.station3 = station3;
        this.numRides3 = numRides3;
    }

    public Date getRecordTime() {
        return recordTime;
    }

    public void setRecordTime(Date recordTime) {
        this.recordTime = recordTime;
    }

    public Float getMaxDuration() {
        return maxDuration;
    }

    public void setMaxDuration(Float maxDuration) {
        this.maxDuration = maxDuration;
    }

    public Float getMinDuration() {
        return minDuration;
    }

    public void setMinDuration(Float minDuration) {
        this.minDuration = minDuration;
    }

    public Float getAvgDuration() {
        return avgDuration;
    }

    public void setAvgDuration(Float avgDuration) {
        this.avgDuration = avgDuration;
    }

    public String getStation1() {
        return station1;
    }

    public void setStation1(String station1) {
        this.station1 = station1;
    }

    public Integer getNumRides1() {
        return numRides1;
    }

    public void setNumRides1(Integer numRides1) {
        this.numRides1 = numRides1;
    }

    public String getStation2() {
        return station2;
    }

    public void setStation2(String station2) {
        this.station2 = station2;
    }

    public Integer getNumRides2() {
        return numRides2;
    }

    public void setNumRides2(Integer numRides2) {
        this.numRides2 = numRides2;
    }

    public String getStation3() {
        return station3;
    }

    public void setStation3(String station3) {
        this.station3 = station3;
    }

    public Integer getNumRides3() {
        return numRides3;
    }

    public void setNumRides3(Integer numRides3) {
        this.numRides3 = numRides3;
    }
}
