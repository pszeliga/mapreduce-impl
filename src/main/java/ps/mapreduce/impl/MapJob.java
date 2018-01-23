package ps.mapreduce.impl;

import ps.mapreduce.impl.datastore.IntermediateResult;

import java.util.List;

public interface MapJob<MK, MV> extends UserJob {

    List<IntermediateResult<MK, MV>> map(String key, String value);

}
