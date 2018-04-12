package ps.mapreduce.impl.example;

import ps.mapreduce.impl.Master;
import ps.mapreduce.impl.jobs.MapJob;
import ps.mapreduce.impl.jobs.ReduceJob;
import ps.mapreduce.impl.storage.IntermediateResult;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/*
    Sorts words into multiple files (each file contains sorted words which starts from the same letter)
 */
public class SortWords {

    public static void main(String[] args) {
        int numberOfWorkers = Runtime.getRuntime().availableProcessors();
        Master<String, List<String>> master = new Master<>(numberOfWorkers,
                new SortWordsMap(),
                new SortWordsReduce(),
                new SortWordsCombine(),
                System.out);
        master.run();
    }

    public static class SortWordsMap implements MapJob<String, List<String>> {

        @Override
        public List<IntermediateResult<String, List<String>>> map(String key, String value) {
            return Arrays.stream(value.split(" "))
                    .map(word -> new IntermediateResult<>(word.substring(0,1), Collections.singletonList(word)))
                    .collect(toList());
        }
    }

    public static class SortWordsReduce implements ReduceJob<String, List<String>> {

        @Override
        public List<String> reduce(String reduceId, List<List<String>> values) {
            return values.stream()
                    .flatMap(Collection::stream)
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    public static class SortWordsCombine implements ReduceJob<String, List<String>> {

        @Override
        public List<String> reduce(String key, List<List<String>> values) {
            return values.stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }
    }

}
