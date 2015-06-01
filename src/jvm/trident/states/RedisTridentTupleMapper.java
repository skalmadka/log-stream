package trident.states;

import backtype.storm.tuple.ITuple;
import com.github.fhuss.storm.elasticsearch.Document;
import org.apache.storm.redis.common.mapper.TupleMapper;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Sunil Kalmadka on 5/27/15.
 */
public class RedisTridentTupleMapper implements TupleMapper {

    @Override
    public String getKeyFromTuple(ITuple iTuple) {
        return iTuple.getString(0);
    }

    @Override
    public String getValueFromTuple(ITuple iTuple) {
        return iTuple.getInteger(1).toString();
    }
}