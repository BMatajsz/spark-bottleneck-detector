package bottleneckdetector;

import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.rpc.RpcEndpointRef;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


class Driver implements DriverPlugin {
    private static final ConcurrentHashMap<String, RpcEndpointRef> executorEndpoints = new ConcurrentHashMap<String, RpcEndpointRef>();
    protected static int uploadInterval;

    @Override
    public Map<String,String> init(SparkContext sc, PluginContext ctx) {
        HashMap<String, String> extraConf = new HashMap<String, String>();
        
        extraConf.put("pyroscope.server", sc.getConf().get("spark.pyroscope.server", ""));
        extraConf.put("pyroscope.profilingInterval", sc.getConf().get("spark.pyroscope.profilingInterval", "10"));
        extraConf.put("pyroscope.samplingDuration", sc.getConf().get("spark.pyroscope.samplingDuration", "1000"));
        extraConf.put("pyroscope.uploadInterval", sc.getConf().get("spark.pyroscope.uploadInterval", "1000"));
        extraConf.put("pyroscope.memory", sc.getConf().get("spark.pyroscope.memory", "500k"));
        extraConf.put("pyroscope.name", sc.getConf().get("spark.pyroscope.name", "spark-app"));

        Driver.uploadInterval = Integer.valueOf(sc.getConf().get("spark.pyroscope.uploadInterval", "1000"));
        double cpuThreshold = Double.parseDouble(sc.getConf().get("spark.detector.cpu.threshold", "25"));
        double memoryThreshold = Double.parseDouble(sc.getConf().get("spark.detector.memory.threshold", "25"));

        sc.setLogLevel("WARN");
        sc.addSparkListener(new Listener(sc, cpuThreshold, memoryThreshold));

        return extraConf; 
    }

    
    // Send a signal to executors indicating job start
    protected static void sendJobSignal(Messages.JobSignal jobSignal) {
        for (RpcEndpointRef ref : executorEndpoints.values()) {
            try {
                ref.send(jobSignal);
            } catch (Exception e) {
                System.err.println("There has been an error sending jobSignal: " + e.getMessage());
            }
        }
        System.out.println("Broadcasting job signal: " + jobSignal.getEvent() + ":" + jobSignal.getJobId());
    }


    // Receive messages from executors (executor registration to get RPC endpoints)
    @Override
    public Object receive(Object message) {
        if (message instanceof Messages.ExecutorSignal) {
            Messages.ExecutorSignal executorMsg = (Messages.ExecutorSignal) message;

            if (executorMsg.getMessage().equals("SHUTDOWN")) {
                if (executorEndpoints.contains(executorMsg.getExecutorId())) {
                    executorEndpoints.remove(executorMsg.getExecutorId());
                    System.out.println("Executor shutdown: " + executorMsg.getExecutorId());
                } else {
                    System.out.println("Executor already shutdown: " + executorMsg.getExecutorId());
                }
            } else if (executorMsg.getMessage().equals("INIT")) {
                if (executorEndpoints.contains(executorMsg.getExecutorId())) {
                    System.out.println("Executor already registered: " + executorMsg.getExecutorId() + ":" + executorMsg.getExecutorEndpoint());
                } else {
                    executorEndpoints.put(executorMsg.getExecutorId(), executorMsg.getExecutorEndpoint());
                    System.out.println("Executor registered: " + executorMsg.getExecutorId());
                }
            } 
        }
        return null;
    }


    // Clear the executor RPC endpoints
    @Override
    public void shutdown() {
        try {
            executorEndpoints.clear();
            BottleneckDetector.executorService.shutdown();
            Listener.bottleneckDetector.shutdown();
            if (!BottleneckDetector.executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                BottleneckDetector.executorService.shutdownNow();
            }

            if (!Listener.bottleneckDetector.awaitTermination(60, TimeUnit.SECONDS)) {
                Listener.bottleneckDetector.shutdownNow();
            }
            
        } catch (InterruptedException e) {
            BottleneckDetector.executorService.shutdownNow();
            Listener.bottleneckDetector.shutdownNow();
        }
    }
}
