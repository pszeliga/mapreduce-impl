package ps.mapreduce.impl;

import ps.mapreduce.impl.datastore.IntermediateResult;

import java.util.List;

public interface MapJob<K2, V2> extends UserJob {

    List<IntermediateResult<K2, V2>> map(String key, String value);

}
