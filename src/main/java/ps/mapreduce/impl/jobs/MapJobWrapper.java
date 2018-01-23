package ps.mapreduce.impl.jobs;

import ps.mapreduce.impl.MapJob;
import ps.mapreduce.impl.ReduceJob;

public class MapJobWrapper<MK, RV> implements Job {

    private final String fileName;

    private final MapJob<MK, RV> mapJob;

    private CombineJob<MK, RV> combineJob;

    public MapJobWrapper(String fileName, MapJob<MK, RV> mapJob, CombineJob<MK, RV> combineJob) {
        this.fileName = fileName;
        this.mapJob = mapJob;
        this.combineJob = combineJob;
    }

    public String getFileName() {
        return fileName;
    }

    public MapJob<MK, RV> getMapJob() {
        return mapJob;
    }

    public CombineJob<MK, RV> getCombineJob() {
        return combineJob;
    }
}
