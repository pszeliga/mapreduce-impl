package ps.mapreduce.impl.example;

import ps.mapreduce.impl.jobs.MapJob;
import ps.mapreduce.impl.Master;
import ps.mapreduce.impl.jobs.ReduceJob;
import ps.mapreduce.impl.storage.IntermediateResult;

import java.util.Collections;
import java.util.List;

/*
    Finds lines which contain provided pattern
 */
public class DistributedGrep {

    public static void main(String[] args) {
        int numberOfWorkers = Runtime.getRuntime().availableProcessors();
        Master<String, String> master = new Master<>(numberOfWorkers,
                new GrepMap("And"),
                new GrepReduce(),
                new GrepReduce(),
                System.out);
        master.run();
    }

    public static class GrepMap implements MapJob<String, String> {

        private String phrase;

        public GrepMap(String phrase) {
            this.phrase = phrase;
        }

        @Override
        public List<IntermediateResult<String, String>> map(String key, String value) {
            return value.contains(phrase) ?
                    Collections.singletonList(new IntermediateResult<>(key, value)) :
                    Collections.emptyList();
        }
    }

    public static class GrepReduce implements ReduceJob<String, String> {

        @Override
        public String reduce(String key, List<String> values) {
            return values.get(0);
        }
    }

}
