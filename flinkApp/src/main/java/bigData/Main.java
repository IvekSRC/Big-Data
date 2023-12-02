package bigData;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.mapping.Mapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

public class Main {
        private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:29092";
        private static final String KAFKA_TOPIC = "flink";
        private static final String CASSANDRA_CONTACT_POINT = "cassandra-node";
        private static final int CASSANDRA_PORT = 9042;
        private static final String START_STATION_ID_FILTER = "480";
        private static final int PARALLELISM = 2;

        public static void main(String[] args) throws Exception {
                final DeserializationSchema<OsloRide> schema = new OsloRideDeserializationSchema();
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

                KafkaSource<OsloRide> source = KafkaSource.<OsloRide>builder()
                        .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                        .setTopics(KAFKA_TOPIC)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(schema))
                        .build();

                DataStream<OsloRide> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").
                        filter((FilterFunction<OsloRide>) value -> (value.start_station_id.equals(START_STATION_ID_FILTER)));

                DataStream<TripDurationStatistics> res = ds.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                        .process(new TripDurationStatisticsCalculator());
                
                CassandraSink.addSink(res)
                        .setMapperOptions(() -> new Mapper.Option[] {
                                Mapper.Option.saveNullFields(true)
                        })
                        .setClusterBuilder(new ClusterBuilder() {
                                private static final long serialVersionUID = 1L;

                                @Override
                                protected Cluster buildCluster(Cluster.Builder builder) {
                                        return builder.addContactPoints(CASSANDRA_CONTACT_POINT).withPort(CASSANDRA_PORT).build();
                                }
                        })
                        .build();

                env.setParallelism(2);
                env.execute("Big Data 2 - Flink");
    }
}
