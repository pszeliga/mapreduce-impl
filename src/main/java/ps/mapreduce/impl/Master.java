package ps.mapreduce.impl;

import ps.mapreduce.impl.datastore.InputDataStore;
import ps.mapreduce.impl.jobs.CombineJob;
import ps.mapreduce.impl.jobs.Job;
import ps.mapreduce.impl.jobs.MapJobWrapper;
import ps.mapreduce.impl.jobs.ReduceJobWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Master implements Runnable {

    /*
            One of the copies of the program is special – the
    master. The rest are workers that are assigned work
    by the master. There are M map tasks and R reduce
    tasks to assign. The master picks idle workers and
    assigns each one a map task or a reduce task.
     */

    private InputDataStore inputDataStore = new InputDataStore() {
        @Override
        public Stream<String> read(String fileName) {
//            if (fileName.equals("1")) return Stream.of("asdfasdf", "sdfgdsf", "sdfgdsf");
//            if (fileName.equals("2")) return Stream.of("gfd", "sdf", "gsdfgdsf", "234gdf", "234gdf");
//            if (fileName.equals("3")) return Stream.of("234gdf", "gfd", "gsdfgg344dsf");
            return null;
        }

        @Override
        public Stream<String> fileNames(String location) {
            return Stream.of("1", "2", "3");
        }
    };

    private final ExecutorService scheduler;
    private final ExecutorService workers;

    private final BlockingQueue<Job> jobs;

    private final AtomicInteger mapJobsCount;

    private final int workersCount;

    private final String inputLocation = "a";
    private MapJob mapJob;
    private ReduceJob reduceJob;
    private CombineJob combineJob;

    public Master(int workersCount, MapJob mapJob, ReduceJob reduceJob,
                  ReduceJob combinerJob) {
        this.mapJob = mapJob;
        this.reduceJob = reduceJob;
        this.combineJob = combinerJob;
        this.scheduler = Executors.newSingleThreadExecutor(newNamedThreadFactory("map-reduce-scheduler"));
        this.workers = new ThreadPoolExecutor(workersCount, workersCount, 0L, TimeUnit.MILLISECONDS,
                new LimitedQueue<>(workersCount));
        this.workersCount = workersCount;
        this.jobs = new LinkedBlockingQueue<>();
        this.mapJobsCount = new AtomicInteger(0);
//        this.scheduleMapJobs();
    }

    private ThreadFactory newNamedThreadFactory(String group) {
        ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
        return runnable -> {
            Thread newThread = defaultThreadFactory.newThread(runnable);
            newThread.setName(group + ": " +newThread.getName());
            return newThread;
        };
    }

//    public void scheduleMapJobs() {
//        inputDataStore.fileNames(inputLocation)
//                .forEach(fileName -> {
//                        jobs.offer(new MapJobWrapper(fileName, mapJob, combineJob));
//                        mapJobsCount.incrementAndGet();
//                });
//    }

    @Override
    public void run() {
        Worker worker = new Worker();

//        while(jobsWaiting()) {
//            try {
//                Job job = jobs.take();
//                if (job instanceof MapJobWrapper) {
//                    MapJobWrapper mapJobWrapper = (MapJobWrapper) job;
//                    workers.execute(() -> {
//                        List resultLocation = worker.mapTask((MapJob<String, Integer>) mapJobWrapper.getMapJob(),
//                                mapJobWrapper.getFileName(), mapJobWrapper.getCombineJob());
//                        int mapJobsLeft = mapJobsCount.decrementAndGet();
////                        new ReduceJobWrapper()
////                        jobs.put();
//                    });
//                } else {
//
//                }
//
//
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//        workers.shutdown();
//
//        List collect = inputDataStore.fileNames(inputLocation)
//                .map(fileName ->  {
//                           return CompletableFuture.
//
//                        }
//
//                )
//                .collect(Collectors.toList());


//        List<CompletableFuture<List>> completableFutures =
//        List<CompletableFuture<List>> collect = inputDataStore.fileNames(inputLocation)
//                .map(fileName ->  {
//                    CompletableFuture<List> listCompletableFuture = CompletableFuture.<List>supplyAsync(() -> {
//                        MapJobWrapper mapJobWrapper = new MapJobWrapper(fileName, mapJob, combineJob);
//                        List list = worker.mapTask((MapJob<String, Integer>) mapJobWrapper.getMapJob(),
//                                mapJobWrapper.getFileName(), mapJobWrapper.getCombineJob());
//                        return list;
//                    }, workers);
//                    return listCompletableFuture;
//
//                    }
//
//                    )
//                .collect(Collectors.<CompletableFuture<List>>toList());


        List<CompletableFuture<List>> completableFutures = inputDataStore.fileNames("").
                map(fileName -> CompletableFuture.supplyAsync(() -> (List) lulu(fileName, worker), workers)).
                collect(Collectors.toList());


//                .collect(Collectors.toList());
        CompletableFuture<Void> allFuturesResult = CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));
        CompletableFuture<List<List>> allDone = allFuturesResult.thenApply(v ->
                completableFutures.stream()
                        .map(future -> future.join())
                        .collect(Collectors.toList()));
//        CompletableFuture<List> listCompletableFuture =
        CompletableFuture<Object> finalRes = allDone
                .thenApply(results -> sortResults(results))
                .thenApply(sorted -> sorted.stream()
                        .map(key -> CompletableFuture.supplyAsync(() -> worker.reduceTask(reduceJob, (String) key), workers))
                        .collect(Collectors.toList()))

                ;
        finalRes.thenAccept(object -> {
            List<CompletableFuture> list = (List<CompletableFuture>) object;
//            CompletableFuture<Void> result = CompletableFuture.allOf(list.toArray(new CompletableFuture[list.size()]));
            for (CompletableFuture completableFuture : list) {
                try {
                    System.out.println(completableFuture.get());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }

        });


    }

    private List lulu(String fileName, Worker worker) {
        MapJobWrapper mapJobWrapper = new MapJobWrapper(fileName, mapJob, combineJob);
        List list = worker.mapTask((MapJob<String, Integer>) mapJobWrapper.getMapJob(),
                mapJobWrapper.getFileName(), mapJobWrapper.getCombineJob());
        return list;
    }



    private List sortResults(List<List> results) {

        Object collect = results.stream()
                .flatMap(e -> e.stream())
                .distinct()
                .collect(Collectors.toList());
        return (List) collect;
    }

    private boolean jobsWaiting() {
        return mapJobsCount.intValue() != 0 && !jobs.isEmpty();

    }


    public void submitJob() {

    }


    public static class LimitedQueue<E> extends LinkedBlockingQueue<E>
    {
        public LimitedQueue(int maxSize)
        {
            super(maxSize);
        }

        @Override
        public boolean offer(E e)
        {
            // turn offer() and add() into a blocking calls (unless interrupted)
            try {
                put(e);
                return true;
            } catch(InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return false;
        }

    }

//    public static class Scheduler implements Runnable {
//
//        private final BlockingQueue<UserJob> jobs;
//        private final ExecutorService workers;
//        private final LongAdder jobLeftCounter;
//
//        public Scheduler(BlockingQueue<UserJob> jobs, ExecutorService workers, LongAdder jobLeftCounter) {
//            this.jobs = jobs;
//            this.workers = workers;
//            this.jobLeftCounter = jobLeftCounter;
//        }
//
//        @Override
//        public void run() {
//            while(jobLeftCounter.intValue() != 0) {
//                try {
//                    UserJob job = jobs.take();
//                    workers.submit()
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }
}
