package ps.mapreduce.impl.jobs;

import ps.mapreduce.impl.ReduceJob;

public class ReduceJobWrapper implements Job {

    private final String key;

    private final ReduceJob reduceJob;

    public ReduceJobWrapper(String key, ReduceJob reduceJob) {
        this.key = key;
        this.reduceJob = reduceJob;
    }

    public String getKey() {
        return key;
    }

    public ReduceJob getReduceJob() {
        return reduceJob;
    }
}
