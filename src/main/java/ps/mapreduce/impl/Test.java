package ps.mapreduce.impl;

import ps.mapreduce.impl.datastore.IntermediateResult;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class Test {

    public static void main(String[] args) throws Exception {
        String file1 = "abc.txt";
        String content1 = "is this the real life, is this just fantasy";
        String file2 = "bcd.txt";
        String content2 = "this is the time when it is going to work";

        Class<?> aClass = Class.forName("ps.mapreduce.impl.Test$CountMap");
        Object instance = aClass.newInstance();
        Method mapMethod = aClass.getMethod("map", Serializable.class, Serializable.class);
        Object result = mapMethod.invoke(instance, file1, content1);

        System.out.println();
//        CountMap countMap = new CountMap();
//        CountReduce countReduce = new CountReduce();
//
//        List<IntermediateResult<String, Integer>> i1 = countMap.map(file1, content1);
//        List<IntermediateResult<String, Integer>> i2 = countMap.map(file2, content2);
//
//        Map<String, List<IntermediateResult<String, Integer>>> mapped = Stream.concat(i1.stream(), i2.stream())
//                .collect(groupingBy(IntermediateResult::getKey));
//        List<IntermediateResult<String, List<Integer>>> preReduced = mapped.keySet().stream()
//                .map(key -> new IntermediateResult<>(key, mapped.get(key)
//                        .stream().map(IntermediateResult::getValue).collect(toList())))
//                .collect(toList());
//        preReduced.stream()
//                .map(i -> new IntermediateResult<>(i.getKey(), countReduce.reduce(i.getKey(), i.getValue())))
//                .forEach(i -> System.out.println("key: " + i.getKey() + " value: " + i.getValue()));


//                .map(i -> countReduce.reduce(i.getKey(), i.getValue()))

//TODO list serializable
//TODO final result + key
    }


//    public static class CountMap implements MapJob<String, Integer> {
//
//        @Override
//        public List<IntermediateResult<String, Integer>> map(String key, String value) {
//            return Arrays.stream(value.split(" "))
//                    .map(word -> new IntermediateResult<>(word, 1))
//                    .collect(toList());
//        }
//    }
//
//    public static class CountReduce implements ReduceJob<String, Integer> {
//
//        @Override
//        public Integer reduce(String key, List<Integer> values) {
//            return values.stream().reduce((a, b) -> a + b).orElse(0);
//        }
//
//    }


}
