package ps.mapreduce.impl.datastore;

import java.util.stream.Stream;

public interface InputDataStore {

    Stream<String> read(String fileName);//, int offset, int chunkSize);

    Stream<String> fileNames(String location);

}
