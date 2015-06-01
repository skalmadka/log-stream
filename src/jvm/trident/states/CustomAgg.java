package trident.states;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Sunil Kalmadka
 */
public class CustomAgg implements CombinerAggregator<Long> {
    public CustomAgg() {
    }

    public Long init(TridentTuple tuple) {
        return Long.valueOf(1L);
    }

    public Long combine(Long val1, Long val2) {
        return Long.valueOf(val1.longValue() + val2.longValue());
    }

    public Long zero() {
        return Long.valueOf(0L);
    }
}
