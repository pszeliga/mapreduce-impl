package ps.mapreduce.impl;

import ps.mapreduce.impl.jobs.CombineJob;

import java.util.List;

public interface ReduceJob<K, V> extends UserJob, CombineJob<K, V> {

}
