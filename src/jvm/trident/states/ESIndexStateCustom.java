package trident.states;

import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.FailedException;
import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.handler.BulkResponseHandler;
import com.github.fhuss.storm.elasticsearch.mapper.TridentTupleMapper;
import com.github.fhuss.storm.elasticsearch.state.ValueSerializer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.*;

/**
 * Created by Sunil Kalmadka on 5/5/15.
 * Reference: com.github.fhuss.storm.elasticsearch.state.ESIndexState
 *
 * Modified com.github.fhuss.storm.elasticsearch.state.ESIndexState to support rich search features and
 * bring in more control over Elasticsearch.
 */
public class ESIndexStateCustom<T> implements State {
    public static final Logger LOGGER = LoggerFactory.getLogger(ESIndexStateCustom.class);
    private Client client;
    private ValueSerializer<T> serializer;

    public ESIndexStateCustom(Client client, ValueSerializer<T> serializer) {
        this.client = client;
        this.serializer = serializer;
    }

    public void beginCommit(Long aLong) {
    }

    public void commit(Long aLong) {
    }

    public void bulkUpdateIndices(List<TridentTuple> inputs, TridentTupleMapper<Document<T>> mapper, BulkResponseHandler handler) {
        BulkRequestBuilder bulkRequest = this.client.prepareBulk();

        IndexRequestBuilder request;
        for(Iterator e = inputs.iterator(); e.hasNext(); bulkRequest.add(request)) {
            TridentTuple input = (TridentTuple)e.next();
            Document doc = (Document)mapper.map(input);
            request = this.client.prepareIndex(doc.getName(), doc.getType(), doc.getId()).setSource((String)doc.getSource());
            if(doc.getParentId() != null) {
                request.setParent(doc.getParentId());
            }
        }

        if(bulkRequest.numberOfActions() > 0) {
            try {
                handler.handle((BulkResponse)bulkRequest.execute().actionGet());
            } catch (ElasticsearchException var9) {
                LOGGER.error("error while executing bulk request to elasticsearch");
                throw new FailedException("Failed to store data into elasticsearch", var9);
            }
        }

    }

    public Collection<T> searchQuery(String query, List<String> indices, List<String> types) {
        SearchResponse response = (SearchResponse)this.client.prepareSearch(new String[0]).setIndices((String[])indices.toArray(new String[indices.size()])).setTypes((String[])types.toArray(new String[types.size()])).setQuery(query).execute().actionGet();
        LinkedList result = new LinkedList();
        Iterator i$ = response.getHits().iterator();

        while(i$.hasNext()) {
            SearchHit hit = (SearchHit)i$.next();

            try {
                result.add(this.serializer.deserialize(hit.source()));
            } catch (IOException var9) {
                LOGGER.error("Error while trying to deserialize data from json source");
            }
        }

        return result;
    }

    public static class Factory<T> implements StateFactory {
        private ClientFactory clientFactory;
        private ValueSerializer<T> serializer;

        public Factory(ClientFactory clientFactory, Class<T> clazz) {
            this.clientFactory = clientFactory;
            this.serializer = new ValueSerializer.NonTransactionalValueSerializer(clazz);
        }

        public State makeState(Map map, IMetricsContext iMetricsContext, int i, int i2) {
            return new ESIndexStateCustom(this.makeClient(map), this.serializer);
        }

        protected Client makeClient(Map map) {
            return this.clientFactory.makeClient(map);
        }
    }


    public Collection<T> customSearchQuery(String query, List<String> indices, List<String> types) {
        SearchResponse response = null;

        if(types.size() == 1 && types.get(0).equalsIgnoreCase("_all")){
            response = this.client.prepareSearch(new String[0])
                    .setIndices((String[]) indices.toArray(new String[indices.size()]))
                    .setQuery(query)
                    .execute()
                    .actionGet();
        } else {
            response = this.client.prepareSearch(new String[0])
                    .setIndices((String[])indices.toArray(new String[indices.size()]))
                    .setTypes((String[])types.toArray(new String[types.size()]))
                    .setQuery(query)
                    .execute()
                    .actionGet();
        }
        LinkedList result = new LinkedList();
        Iterator i$ = response.getHits().iterator();

        while(i$.hasNext()) {
            SearchHit hit = (SearchHit)i$.next();

            try {
                result.add(this.serializer.deserialize(hit.source()));
            } catch (IOException var9) {
                LOGGER.error("Error while trying to deserialize data from json source");
            }
        }

        return result;
    }

    public Collection<T> significantTermQuery(String query, List<String> indices, List<String> types, String field) {
        SearchResponse response = null;

        if(types.size() == 1 && types.get(0).equalsIgnoreCase("_all")){
            response = this.client.prepareSearch(new String[0])
                    .setIndices((String[]) indices.toArray(new String[indices.size()]))
                    .setQuery(query)
                    .addAggregation( AggregationBuilders
                            .significantTerms("significantTerms")
                            .field(field))
                    .execute()
                    .actionGet();
        } else {
            response = this.client.prepareSearch(new String[0])
                    .setIndices((String[]) indices.toArray(new String[indices.size()]))
                    .setTypes((String[]) types.toArray(new String[types.size()]))
                    .setQuery(query)
                    .addAggregation(AggregationBuilders
                            .significantTerms("significantTerms")
                            .field(field))
                    .execute()
                    .actionGet();
        }
        LinkedList result = new LinkedList();
        SignificantTerms agg = response.getAggregations().get("significantTerms");

        // For each entry
        for (SignificantTerms.Bucket entry : agg.getBuckets()) {
            try{
                result.add(this.serializer.deserialize(entry.toString().getBytes()));
            } catch (IOException var9) {
                LOGGER.error("Error while trying to deserialize data from json source");
            }
        }

        return result;
    }
}
