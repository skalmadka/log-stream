package trident.filters;

import backtype.storm.tuple.Values;
import jdk.nashorn.internal.runtime.regexp.joni.Config;
import org.joda.time.DateTime;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import trident.ConfigConstants;

import java.util.Map;

public class RedisCounter extends BaseFilter {
    Jedis jedis;
    final String success_key = "success-request-map";
    final String fail_key = "fail-request-map";

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        //Connect Redis
        jedis = new Jedis(conf.get(ConfigConstants.REDIS_HOST_NAME).toString(),  Short.parseShort(conf.get(ConfigConstants.REDIS_HOST_PORT).toString()));
    }


    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        final JSONObject logJson = (JSONObject)tridentTuple.get(0);
        final String request = (String)logJson.get("request");
        final String response = (String)logJson.get("response");

        String dir = "";
        try {//Extract the root directory name of the requested page
            String[] tokens = request.split("/");
            dir = tokens[1];
        } catch(Exception e){//Ignore errors
            return true;
        }

        if("200".equals(response))
            jedis.hincrBy(success_key, dir, new Long(1));
        else
            jedis.hincrBy(fail_key, dir, new Long(1));

        return true;
    }
}
