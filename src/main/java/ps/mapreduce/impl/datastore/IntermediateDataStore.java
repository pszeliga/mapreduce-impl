package ps.mapreduce.impl.datastore;

import java.util.List;

public interface IntermediateDataStore<K, V> {

    K write(IntermediateResult<K, V> intermediateResult);

    List<IntermediateResult<K, V>> read(String location);

}
