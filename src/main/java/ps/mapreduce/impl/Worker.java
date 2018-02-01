package ps.mapreduce.impl;

import ps.mapreduce.impl.jobs.ReduceJob;
import ps.mapreduce.impl.storage.*;
import ps.mapreduce.impl.jobs.MapJob;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class Worker<MK, RV> {

    private final DataStore<MK, RV> dataStore;

    public Worker(DataStore<MK, RV> dataStore) {
        this.dataStore = dataStore;
    }

    public List<MK> mapTask(MapJob<MK, RV> mapJob, String key, ReduceJob<MK, RV> combineJob) {
        System.out.println("New map task for key: " + key);
        return dataStore.readFile(key)
                .flatMap(line -> mapJob.map(key, line).stream())
                .collect(Collectors.groupingBy(IntermediateResult::getKey))
                .entrySet()
                .stream()
                .map(entry -> new IntermediateResult<>(entry.getKey(), combine(combineJob, entry)))
                .map(dataStore::write)
                .collect(Collectors.toList());
    }

    private RV combine(ReduceJob<MK, RV> combineJob, Map.Entry<MK, List<IntermediateResult<MK, RV>>> entry) {
        System.out.println("Combine task for: " + entry.getKey().toString());

        List<RV> valuesToCombine = entry.getValue().stream().map(IntermediateResult::getValue).collect(toList());
        return combineJob.reduce(entry.getKey(), valuesToCombine);
    }

    public FinalResult<MK, RV> reduceTask(ReduceJob<MK, RV> reduceJob, MK location) {
        System.out.println("New reduce task for: " + location.toString());

        List<IntermediateResult<MK, RV>> data = dataStore.read(location.toString());
        List<RV> valuesToReduce = data.stream().map(IntermediateResult::getValue).collect(toList());
        return new FinalResult<>(location, reduceJob.reduce(location, valuesToReduce));
    }
}
