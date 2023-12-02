package bigData;

import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class OsloRideDeserializationSchema implements DeserializationSchema<OsloRide> {
    private static final Gson gson = new Gson();

    @Override
    public TypeInformation<OsloRide> getProducedType() {
        return TypeInformation.of(OsloRide.class);
    }

    @Override
    public OsloRide deserialize(byte[] bytes) throws IOException {
        OsloRide response = gson.fromJson(new String(bytes), OsloRide.class);
        return response;
    }

    @Override
    public boolean isEndOfStream(OsloRide osloRide) {
        return false;
    }

}
