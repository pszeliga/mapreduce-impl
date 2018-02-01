package ps.mapreduce.impl.storage;

import java.util.List;
import java.util.stream.Stream;

public interface DataStore<K, V> {

    K write(IntermediateResult<K, V> intermediateResult);

    List<IntermediateResult<K, V>> read(String location);

    Stream<String> readFile(String fileName);

    Stream<String> fileNames();

}
