package trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.state.ESIndexState;
import com.github.fhuss.storm.elasticsearch.state.ESIndexUpdater;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.json.simple.JSONObject;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;
import trident.common.ConfigReader;
import trident.filters.PrintFilter;
import trident.filters.RedisCounter;
import trident.functions.ExtractData;
import trident.functions.PrepareForElasticSearch;
import trident.functions.SplitFunction;
import trident.states.ESIndexStateCustom;
import trident.states.ESTridentTupleMapper;

/**
 * Created by Sunil Kalmadka
 */
public class LogStreamTopology {
    public static StormTopology buildTopology(Config conf) {
        TridentTopology topology = new TridentTopology();

        //Kafka Spout
        BrokerHosts zk = new ZkHosts(conf.get(ConfigConstants.KAFKA_CONSUMER_HOST_NAME) + ":" +conf.get(ConfigConstants.KAFKA_CONSUMER_HOST_PORT));
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(zk, (String) conf.get(ConfigConstants.KAFKA_TOPIC_NAME));
        kafkaConfig.fetchSizeBytes  = 4000000;
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        //kafkaConfig.ignoreZkOffsets=true;
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(kafkaConfig);

        //ElasticSearch Persistent State
        Settings esSettings = ImmutableSettings.settingsBuilder()
                .put("storm.elasticsearch.cluster.name", conf.get(ConfigConstants.ELASTICSEARCH_CLUSTER_NAME))
                .put("storm.elasticsearch.hosts", conf.get(ConfigConstants.ELASTICSEARCH_HOST_NAME) + ":" + conf.get(ConfigConstants.ELASTICSEARCH_HOST_PORT))
                .build();
        StateFactory esStateFactory = new ESIndexState.Factory<JSONObject>(new ClientFactory.NodeClient(esSettings.getAsMap()), JSONObject.class);
        TridentState esStaticState = topology.newStaticState(esStateFactory);

        //Topology
        topology.newStream("commonLogKafkaSpout", spout).parallelismHint(3).name("commonLogKafkaSpout")
                 //A Complex Reg-Ex
                .each(new Fields("str"), new ExtractData(), new Fields("logJson")).parallelismHint(2).name("ExtractData")
                //Some Redis Operation
                //.each(new Fields("logJson"), new RedisCounter()).parallelismHint(2).name("RedisCounter")
                // Load to Elasticsearch
                .each(new Fields("logJson"), new PrepareForElasticSearch(), new Fields("index", "type", "id", "source")).parallelismHint(2).name("PrepareForElasticSearch")
                .partitionPersist(esStateFactory, new Fields("index", "type", "id", "source"), new ESIndexUpdater<String>(new ESTridentTupleMapper())).parallelismHint(2)
                ;

        return topology.build();
    }

    public static void main(String[] args) throws Exception {

        if(args.length < 1){
            System.err.println("[ERROR] Configuration File Required");
        }
        Config conf = new Config();

        // Store all the configuration in the Storm conf object
        conf.putAll(ConfigReader.readConfigFile(args[0]));

        //Second arg should be local in order to run locally
        if(args.length  < 2 || (args.length  == 2 && !args[1].equals("local"))) {
            StormSubmitter.submitTopologyWithProgressBar("log_stream", conf, buildTopology(conf));
        } else {
            LocalCluster localcluster = new LocalCluster();
            localcluster.submitTopology("log_stream", conf, buildTopology(conf));
        }
    }

}
