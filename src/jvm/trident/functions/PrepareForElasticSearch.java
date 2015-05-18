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

import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class PrepareForElasticSearch extends BaseFunction {

    private String esIndexPrefix;
     DateTimeFormatter datePattern;

    public PrepareForElasticSearch(){

    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.esIndexPrefix = conf.get(ConfigConstants.ELASTICSEARCH_INDEX_NAME).toString();

        final String datePatternString = new String("yyyy.MM.dd");
        this.datePattern = DateTimeFormat.forPattern(datePatternString);
    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        final JSONObject logJson = (JSONObject)tridentTuple.get(0);

        final DateTime logDateTime = DateTime.parse((String)logJson.get("@timestamp"));
        //ES Index
        String date = datePattern.print(logDateTime);
        String esIndexName =esIndexPrefix + "_" + date;

        //ES Type
        String esType = "logs";

        //ES Document Id
        String esDocId = UUID.randomUUID().toString();

        //ES Document Source
        String esSource = logJson.toJSONString();
        //System.out.println("----- ES SOURCE:" + esSource);

        tridentCollector.emit(new Values(esIndexName, esType, esDocId, esSource));
    }
}