package ps.mapreduce.impl.jobs;

import ps.mapreduce.impl.MapJob;
import ps.mapreduce.impl.ReduceJob;

public class MapJobWrapper implements Job {

    private final String fileName;

    private final MapJob mapJob;

    private CombineJob combineJob;

    public MapJobWrapper(String fileName, MapJob mapJob, CombineJob combineJob) {
        this.fileName = fileName;
        this.mapJob = mapJob;
        this.combineJob = combineJob;
    }

    public String getFileName() {
        return fileName;
    }

    public MapJob getMapJob() {
        return mapJob;
    }

    public CombineJob getCombineJob() {
        return combineJob;
    }
}
