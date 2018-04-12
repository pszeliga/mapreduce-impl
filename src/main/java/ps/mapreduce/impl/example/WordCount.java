package ps.mapreduce.impl.example;

import ps.mapreduce.impl.jobs.MapJob;
import ps.mapreduce.impl.Master;
import ps.mapreduce.impl.jobs.ReduceJob;
import ps.mapreduce.impl.storage.IntermediateResult;

import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

/*
    Counts the occurrences of each word in all files
 */
public class WordCount {

    public static void main(String[] args) {
        int numberOfWorkers = Runtime.getRuntime().availableProcessors();
        Master<String, Integer> master = new Master<>(numberOfWorkers,
                new CountMap(),
                new CountReduce(),
                new CountReduce(),
                System.out);
        master.run();
    }

    public static class CountMap implements MapJob<String, Integer> {

        @Override
        public List<IntermediateResult<String, Integer>> map(String key, String value) {
            return Arrays.stream(value.split(" "))
                    .map(word -> new IntermediateResult<>(word, 1))
                    .collect(toList());
        }
    }

    public static class CountReduce implements ReduceJob<String, Integer> {

        @Override
        public Integer reduce(String key, List<Integer> values) {
            return values.stream().reduce((a, b) -> a + b).orElse(0);
        }
    }

}
