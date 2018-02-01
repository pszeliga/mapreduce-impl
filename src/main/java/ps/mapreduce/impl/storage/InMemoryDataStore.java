package ps.mapreduce.impl.storage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class InMemoryDataStore<K, V> implements DataStore<K, V> {

    private final Map<String, String> files = new ConcurrentHashMap<>();

    private final Map<K, List<IntermediateResult<K, V>>> intermediateResults = new ConcurrentHashMap<>();

    public InMemoryDataStore() {
        files.put("file1.txt", "This is the first file \ncontent. ");
        files.put("file2.txt", "And this is the second file content. ");
        files.put("file3.txt", "More text in \nthird file");
        files.put("file4.txt", "And some random text here");
        files.put("file5.txt", "Why not \none more");
        files.put("file6.txt", "Lululu tengo manzana");
    }

    @Override
    public K write(IntermediateResult<K, V> intermediateResult) {
        System.out.println("Writing: " + intermediateResult.toString());
        intermediateResults.computeIfAbsent(intermediateResult.getKey(),
                key -> Collections.synchronizedList(new ArrayList<>())
        ).add(intermediateResult);

        return intermediateResult.getKey();
    }

    @Override
    public List<IntermediateResult<K, V>> read(String location) {
        return intermediateResults.get(location);
    }

    @Override
    public Stream<String> readFile(String fileName) {
        return Arrays.stream(files.get(fileName).split("\n"));
    }

    @Override
    public Stream<String> fileNames() {
        return files.keySet().stream();
    }
}
