package ps.mapreduce.impl.jobs;

import ps.mapreduce.impl.storage.IntermediateResult;

import java.util.List;

public interface MapJob<MK, MV> {

    List<IntermediateResult<MK, MV>> map(String key, String value);

}
