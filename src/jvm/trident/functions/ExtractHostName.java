package trident.functions;

import backtype.storm.tuple.Values;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONObject;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import trident.ConfigConstants;

import java.util.Map;
import java.util.UUID;

public class ExtractHostName extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        final JSONObject logJson = (JSONObject)tridentTuple.get(0);
        final String hostname = (String)logJson.get("@host");

        tridentCollector.emit(new Values(hostname));
    }
}