package ps.mapreduce.impl;

import ps.mapreduce.impl.datastore.IntermediateResult;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import static java.util.stream.Collectors.toList;

public class UserProgram {

    /*
        The MapReduce library in the user program first
    splits the input files into M pieces of typically 16
    megabytes to 64 megabytes (MB) per piece (controllable
    by the user via an optional parameter). It
    then starts up many copies of the program on a cluster
    of machines.
     */

    public static void main(String[] args) {
        int numberOfSplits = 20;
        int numberOfWorkers = 10;
        Class<String> outputKeyClass = String.class;
        Class<Integer> outputValueClass = Integer.class;


        new Master(numberOfWorkers, numberOfSplits, new CountMap(), new CountReduce(), new CountReduce()).run();


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
