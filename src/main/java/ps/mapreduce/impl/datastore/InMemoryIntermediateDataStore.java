package ps.mapreduce.impl.datastore;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryIntermediateDataStore<K, V> implements IntermediateDataStore<K, V> {

    private final Map<K, List<IntermediateResult<K, V>>> memory = new ConcurrentHashMap<>();


    @Override
    public K write(IntermediateResult<K, V> intermediateResult) {
        System.out.println("writing: " + intermediateResult.toString());
            memory.computeIfAbsent(intermediateResult.getKey(),
                    key -> Collections.synchronizedList(new ArrayList<>())).add(intermediateResult);

            return intermediateResult.getKey();
    }

    @Override
    public List<IntermediateResult<K, V>> read(String location) {
        return memory.get(location);
    }
}
