package bottleneckdetector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import scala.collection.JavaConverters;
import scala.collection.Seq;


class Listener extends SparkListener {
    private BottleneckDetector detector;
    protected static final ConcurrentHashMap<Integer, List<CompletableFuture<Void>>> jobTaskFutures = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Integer> taskToJob = new ConcurrentHashMap<Long, Integer>();
    protected static long appStartTime;
    

    protected static final ExecutorService bottleneckDetector = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.setName("bottleneck-detector");
        return t;
    });


    public Listener(SparkContext sc, double cpuThreshold, double memoryThreshold) {
        this.detector = new BottleneckDetector(sc, cpuThreshold, memoryThreshold);
    }


    @Override 
    public void onApplicationStart(SparkListenerApplicationStart appStart) {
        Listener.appStartTime = Instant.now().getEpochSecond();
    }


    @Override
    public void onJobStart(SparkListenerJobStart job) {
        bottleneckDetector.submit(() -> {
            jobTaskFutures.put(job.jobId(), new CopyOnWriteArrayList<>());
            Messages.JobSignal jobSignal = new Messages.JobSignal(job.jobId(), job.stageIds(), "START");

            try {
                detector.stageMap.put(job.jobId(), new ArrayList<Integer>());
                Seq<Object> stageIds = job.stageIds();
                JavaConverters.asJavaCollection(stageIds).forEach(s -> {
                    detector.stageMap.get(job.jobId()).add((int) s);
                    detector.stageToJob.put((int) s, job.jobId());
                });
            } catch (Exception e) {
                System.out.println("ERROR: " + e.getMessage());
                e.printStackTrace();
            }

            Driver.sendJobSignal(jobSignal);
        });
    }


    // On job end, run automatic bottleneck detection (threshold method only for now)
    @Override
    public void onJobEnd(SparkListenerJobEnd job) {
        int jobId = job.jobId();
        List<CompletableFuture<Void>> taskFutures = jobTaskFutures.get(jobId);
        
        if (taskFutures == null || taskFutures.isEmpty()) {
            bottleneckDetector.submit(() -> detector.processJob(jobId));
        } else {
            CompletableFuture<Void> allTasks = CompletableFuture.allOf(taskFutures.toArray(new CompletableFuture[0]));
            allTasks.thenRunAsync(() -> {
                // System.out.println("All " + taskFutures.size() + " tasks completed for job " + jobId + ", starting job analysis");
                detector.processJob(jobId);
            }, bottleneckDetector).exceptionally(throwable -> {
                System.err.println("Failed to process job " + jobId + ": " + throwable.getMessage());
                return null;
            });
        }
    }


    // On stage submitted, add create a new entry and list for the stage and its tasks in the task map
    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        detector.taskMap.put(stageSubmitted.stageInfo().stageId(), new ArrayList<Long>());
    }


    // On task start, add the task ID to the parent stage
    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) {
        Integer jobId = detector.stageToJob.get(taskStart.stageId());
        if (jobId != null) {
            taskToJob.put(taskStart.taskInfo().taskId(), jobId);
        }
        detector.taskMap.get(taskStart.stageId()).add(taskStart.taskInfo().taskId());
    }


    // On task end, send the task related profiles to Pyroscope
    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        long taskId = taskEnd.taskInfo().taskId();
        Integer jobId = taskToJob.get(taskId);
        
        if (jobId != null) {
            CompletableFuture<Void> taskFuture = CompletableFuture.supplyAsync(() -> detector.processTask(taskId), bottleneckDetector)
                .thenCompose(Function.identity())
                .exceptionally(throwable -> {
                    System.err.println("Failed to process task " + taskId + ": " + throwable.getMessage());
                    return null;
                });
            
            List<CompletableFuture<Void>> futures = jobTaskFutures.get(jobId);
            if (futures != null) {
                futures.add(taskFuture);
            }
        }
        
        taskToJob.remove(taskId);
    }
}
