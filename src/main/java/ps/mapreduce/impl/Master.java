package ps.mapreduce.impl;

import ps.mapreduce.impl.storage.FinalResult;
import ps.mapreduce.impl.storage.DataStore;
import ps.mapreduce.impl.storage.InMemoryDataStore;
import ps.mapreduce.impl.jobs.ReduceJob;
import ps.mapreduce.impl.jobs.MapJob;

import java.io.PrintStream;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Master<MK, RV> {

    private static final int MAX_QUEUE_SIZE = 100;

    private final DataStore<MK, RV> dataStore;
    private final ExecutorService executor;
    private final MapJob<MK, RV> mapJob;
    private final ReduceJob<MK, RV> reduceJob;
    private final ReduceJob<MK, RV> combineJob;
    private final Worker<MK, RV> worker;
    private final PrintStream output;

    public Master(int workersCount, MapJob<MK, RV> mapJob, ReduceJob<MK, RV> reduceJob,
                  ReduceJob<MK, RV> combinerJob, PrintStream output) {
        this.mapJob = mapJob;
        this.reduceJob = reduceJob;
        this.combineJob = combinerJob;
        this.executor = new ThreadPoolExecutor(workersCount, workersCount, 0L, TimeUnit.MILLISECONDS,
                new LimitedQueue<>(MAX_QUEUE_SIZE));
        this.dataStore = new InMemoryDataStore<>();
        this.worker = new Worker<>(dataStore);
        this.output = output;
    }

    public void run() {
        List<CompletableFuture<List<MK>>> futureMapResultKeys = dataStore.fileNames()
                .map(fileName -> CompletableFuture.supplyAsync(() -> invokeMapTask(fileName, worker), executor))
                .collect(Collectors.toList());

        joinAll(futureMapResultKeys)
                .thenApply(this::prepareKeys)
                .thenApply(sorted -> sorted.stream()
                        .map(key -> CompletableFuture.supplyAsync(() -> invokeReduceTask(key), executor))
                        .collect(Collectors.toList()))
                .thenAccept(results -> joinAll(results).thenAccept(this::printResultsAndShutdown));
    }

    private void printResultsAndShutdown(List<FinalResult<MK, RV>> finalResults) {
        output.println("\nResults: ");
        finalResults.forEach(output::println);
        executor.shutdown();
    }

    private <T> CompletableFuture<List<T>> joinAll(List<CompletableFuture<T>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
                .thenApply(unit -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
    }


    private List<MK> invokeMapTask(String fileName, Worker<MK, RV> worker) {
        return worker.mapTask(mapJob, fileName, combineJob);
    }

    private FinalResult<MK, RV> invokeReduceTask(MK key) {
        return worker.reduceTask(reduceJob, key);
    }


    private List<MK> prepareKeys(List<List<MK>> results) {
        return results.stream()
                .flatMap(Collection::stream)
                .distinct()
                .collect(Collectors.toList());
    }

    /*
        Blocks instead of rejecting execution
     */
    public static class LimitedQueue<E> extends LinkedBlockingQueue<E> {

        public LimitedQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        public boolean offer(E e) {
            try {
                put(e);
                return true;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }
}
