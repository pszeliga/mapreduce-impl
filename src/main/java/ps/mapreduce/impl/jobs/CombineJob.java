package ps.mapreduce.impl.jobs;

import ps.mapreduce.impl.ReduceJob;

import java.util.List;

public interface CombineJob<K, V> {

    V reduce(K key, List<V> values);
}
