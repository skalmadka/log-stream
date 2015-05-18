package trident.states;

import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.mapper.TridentTupleMapper;
import storm.trident.tuple.TridentTuple;

public class ESTridentTupleMapper implements TridentTupleMapper<Document<String>> {
    @Override
    public Document<String> map(TridentTuple tridentTuple) {
        String index = tridentTuple.getString(0);
        String type = tridentTuple.getString(1);
        String id = tridentTuple.getString(2);
        String source = tridentTuple.getString(3);
        Document<String> esDocument = new Document<String>(index, type, source, id);
        return esDocument;
    }
}
