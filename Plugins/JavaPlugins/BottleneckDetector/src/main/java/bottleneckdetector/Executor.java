package bottleneckdetector;

import org.apache.spark.api.plugin.*;
import org.apache.spark.rpc.*;
import java.util.*;
import org.apache.spark.SparkEnv;
import scala.PartialFunction;
import scala.collection.Seq;
import scala.runtime.AbstractPartialFunction;
import scala.runtime.BoxedUnit;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import io.pyroscope.javaagent.PyroscopeAgent;
import io.pyroscope.javaagent.config.Config;
import io.pyroscope.javaagent.config.ProfilerType;
import io.pyroscope.labels.v2.LabelsSet;
import io.pyroscope.labels.v2.ScopedContext;
import io.pyroscope.javaagent.EventType;
import io.pyroscope.http.Format;
import org.apache.spark.TaskContext;
import scala.collection.JavaConverters;


public class Executor extends Thread implements ExecutorPlugin, ThreadSafeRpcEndpoint {
    private String executorId;
    private RpcEnv rpcEnv;
    private RpcEndpointRef selfRef;
    private static final ThreadLocal<ScopedContext> currThread = new ThreadLocal<>();
    private ConcurrentHashMap<Integer, Integer> stageMap;
    private PluginContext ctx;


    @Override
    public void init(PluginContext ctx, Map<String,String> extraConf) {
        this.executorId = ctx.executorID();
        this.rpcEnv = SparkEnv.get().rpcEnv();
        this.selfRef = rpcEnv.setupEndpoint("executor-endpoint:" + executorId, this);
        this.stageMap = new ConcurrentHashMap<Integer,Integer>();
        this.ctx = ctx;

        String serverAddress = extraConf.getOrDefault("pyroscope.server", "");
        int profilingInterval = Integer.parseInt(extraConf.getOrDefault("pyroscope.profilingInterval", "10"));
        int samplingDuration = Integer.parseInt(extraConf.getOrDefault("pyroscope.samplingDuration", "1000"));
        int uploadInterval = Integer.parseInt(extraConf.getOrDefault("pyroscope.uploadInterval", "1000"));
        String memory = extraConf.getOrDefault("pyroscope.memory", "500k");
        String appName = extraConf.getOrDefault("pyroscope.name", "spark-app");

        PyroscopeAgent.start(new Config.Builder()
            .setApplicationName(appName)
            .setServerAddress(serverAddress)
            .setProfilerType(ProfilerType.ASYNC)
            .setProfilingEvent(EventType.CPU)
            .setProfilingAlloc(memory)
            .setProfilingInterval(Duration.ofMillis(profilingInterval))
            .setSamplingDuration(Duration.ofMillis(samplingDuration))
            .setUploadInterval(Duration.ofMillis(uploadInterval))
            .setFormat(Format.JFR)
            .build()
        );

        try {
            // Use Job -1 for executor init
            ctx.send(new Messages.ExecutorSignal(executorId, selfRef, "INIT"));
        } catch (Exception e) {
            System.out.println("Error while sending initialization signal to driver: " + e.getMessage());
        }
    }


    @Override
    public void shutdown() {
        if (rpcEnv != null && selfRef != null) {
            rpcEnv.stop(selfRef);
        }
        try {
            // Use Job -2 for executor shutdown
            ctx.send(new Messages.ExecutorSignal(executorId, selfRef, "SHUTDOWN"));
        } catch (Exception e) {
            System.out.println("Error while sending shutdown signal to driver: " + e.getMessage());
        }
        
    }


    @Override
    public PartialFunction<Object, BoxedUnit> receive() {
        return new AbstractPartialFunction<Object, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Object message) {
                if (message instanceof Messages.JobSignal) {
                    Messages.JobSignal signal = (Messages.JobSignal) message;
                    if (signal.getEvent().equals("START")) {
                        try {
                            Seq<Object> stageIds = signal.getStageIds();
                            JavaConverters.asJavaCollection(stageIds).forEach(s -> stageMap.put((int) s, signal.getJobId()));
                        } catch (Exception e) {
                            System.out.println("ERROR: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                }
                return BoxedUnit.UNIT;
            }

            @Override
            public boolean isDefinedAt(Object message) {
                return message instanceof Messages.JobSignal;
            }
        };
    }


    @Override
    public void onTaskStart() {
        TaskContext tc = TaskContext.get();
        int stageId = tc.stageId();
        ScopedContext ctx = new ScopedContext(
             new LabelsSet(
                "job", String.valueOf(stageMap.get(stageId)),
                "stage", String.valueOf(stageId),
                "task",  String.valueOf(tc.taskAttemptId())
            )
        );
        currThread.set(ctx);         
    }


    @Override
    public RpcEnv rpcEnv() {
        return rpcEnv;
    }


    @Override
    public RpcEndpointRef self() {
        return selfRef;
    }
}
