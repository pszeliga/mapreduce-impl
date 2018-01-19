package ps.mapreduce.impl;

import ps.mapreduce.impl.datastore.*;
import ps.mapreduce.impl.jobs.CombineJob;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class Worker {


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
    IntermediateDataStore intermediateDataStore = new InMemoryIntermediateDataStore();

    /*
        A worker who is assigned a map task reads the
        contents of the corresponding input split. It parses
        key/value pairs out of the input data and passes each
        pair to the user-defined Map function. The intermediate
        key/value pairs produced by the Map function
        are buffered in memory
    */

    public List<Object> mapTask(MapJob<String, Integer> mapJob, String key, CombineJob<String, Integer> combineJob) {
        System.out.println("New map task for key: " + key);
        Map<String, List<IntermediateResult<String, Integer>>> collect = inputDataStore.read(key)
                .flatMap(line -> mapJob.map(key, line).stream())
                .collect(Collectors.groupingBy(IntermediateResult::getKey));
        collect.replaceAll((k, v) -> Collections.singletonList(new IntermediateResult<>(k,
                        combineJob.reduce(k, v.stream().map(IntermediateResult::getValue).collect(Collectors.toList())))));
        return collect.values().stream()
                .flatMap(e -> e.stream())
                .map(e -> intermediateDataStore.write(e))
                .collect(Collectors.toList());
    }

    public Object reduceTask(ReduceJob reduceJob, String location) {
        List<IntermediateResult> data = (List<IntermediateResult>) intermediateDataStore.read(location);
        return  new FinalResult<>(location,
                reduceJob.reduce(location, data.stream().map(ir -> ir.getValue()).collect(toList())));
    }
}
