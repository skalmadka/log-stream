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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExtractData  extends BaseFunction {

     Pattern apacheAccessLogPattern;
     DateTimeFormatter apacheLogDateTimePattern;

    public ExtractData(){

    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        final String apacheAccessLogPatternString = "^([\\d.a-zA-Z_-]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\-|\\d+)(.*)";
        this.apacheAccessLogPattern = Pattern.compile(apacheAccessLogPatternString);

        final String apacheLogDateTimePatternString = new String("dd/MMM/yyyy:HH:mm:ss Z");
        this.apacheLogDateTimePattern = DateTimeFormat.forPattern(apacheLogDateTimePatternString);
    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String logEntryLine = tridentTuple.getString(0);
        Matcher matcher = this.apacheAccessLogPattern.matcher(logEntryLine);

        if (!matcher.matches() || matcher.groupCount() < 7){
            System.err.println("ERROR no match:" + logEntryLine);
            return;
        }

        //Extract Datetime
        DateTime logDateTime = this.apacheLogDateTimePattern.parseDateTime(matcher.group(4));

        //Prepare Log Object
        JSONObject logJson = new JSONObject();
        logJson.put("@host" , matcher.group(1));
        logJson.put("@timestamp" , logDateTime.toString());
        logJson.put("request" , matcher.group(5));
        logJson.put("response" , matcher.group(6));
        logJson.put("bytes" , matcher.group(7));

        tridentCollector.emit(new Values(logJson));
    }
}