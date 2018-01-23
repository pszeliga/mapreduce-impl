package ps.mapreduce.impl.jobs;

import ps.mapreduce.impl.ReduceJob;

import java.util.List;

public interface CombineJob<MK, RV> {

    RV reduce(MK key, List<RV> values);
}
