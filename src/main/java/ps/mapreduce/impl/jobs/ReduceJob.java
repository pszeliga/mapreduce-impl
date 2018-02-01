package ps.mapreduce.impl.jobs;

import java.util.List;

public interface ReduceJob<MK, RV> {

    RV reduce(MK key, List<RV> values);
}
