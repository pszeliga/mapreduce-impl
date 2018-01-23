package ps.mapreduce.impl;

import ps.mapreduce.impl.datastore.*;
import ps.mapreduce.impl.jobs.CombineJob;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class Worker<MK, RV> {


    private InputDataStore inputDataStore = new InputDataStore() {
        @Override
        public Stream<String> read(String fileName) {
            if (fileName.equals("1")) return Stream.of("asdfasdf", "sdfgdsf", "gsdfgdsf");
            if (fileName.equals("2")) return Stream.of("gfd", "sdf", "gsdfgdsf", "sdf");
            if (fileName.equals("3")) return Stream.of("234gdf", "dfg", "gsdfgg344dsf", "234gdf", "234gdf");
            return null;
        }

        @Override
        public Stream<String> fileNames(String location) {
            return Stream.of("1", "2", "3");
        }
    };
    IntermediateDataStore<MK, RV> intermediateDataStore = new InMemoryIntermediateDataStore();

    /*
        A worker who is assigned a map task reads the
        contents of the corresponding input split. It parses
        key/value pairs out of the input data and passes each
        pair to the user-defined Map function. The intermediate
        key/value pairs produced by the Map function
        are buffered in memory
    */

    public List<MK> mapTask(MapJob<MK, RV> mapJob, String key, CombineJob<MK, RV> combineJob) {
        System.out.println("New map task for key: " + key);
        return inputDataStore.read(key)
                .flatMap(line -> mapJob.map(key, line).stream())
                .collect(Collectors.groupingBy(IntermediateResult::getKey)).entrySet()
                .stream()
                .map(entry -> new IntermediateResult<>(entry.getKey(),
                        combineJob.reduce(entry.getKey(), entry.getValue().stream().map(IntermediateResult::getValue).collect(toList()))))
                .map(e -> intermediateDataStore.write(e))
                .collect(Collectors.toList());

    }

    public FinalResult<MK, RV> reduceTask(ReduceJob<MK, RV> reduceJob, MK location) {
        List<IntermediateResult<MK, RV>> data = intermediateDataStore.read(location.toString());
        List<RV> value = data.stream().map(IntermediateResult::getValue).collect(toList());

        return new FinalResult<>(location, reduceJob.reduce(location, value));
    }
}
