package ps.mapreduce.impl.datastore;

public class FinalResult<K, V> {

    private final K key;

    private final V value;

    public FinalResult(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "FinalResult{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
