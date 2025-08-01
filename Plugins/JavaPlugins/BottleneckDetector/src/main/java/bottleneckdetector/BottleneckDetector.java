package bottleneckdetector;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.spark.SparkContext;
import org.fusesource.jansi.AnsiConsole;
import static org.fusesource.jansi.Ansi.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;


class BottleneckDetector {
    protected final ConcurrentHashMap<Integer, ArrayList<Integer>> stageMap = new ConcurrentHashMap<Integer,ArrayList<Integer>>();
    protected final ConcurrentHashMap<Integer, ArrayList<Long>> taskMap = new ConcurrentHashMap<Integer, ArrayList<Long>>();
    protected final ConcurrentHashMap<Long, ConcurrentHashMap<String, Double>> taskCpuTimes = new ConcurrentHashMap<Long, ConcurrentHashMap<String, Double>>();
    protected final ConcurrentHashMap<Long, ConcurrentHashMap<String, Double>> taskMemoryValues = new ConcurrentHashMap<Long, ConcurrentHashMap<String, Double>>();
    protected final ConcurrentHashMap<Integer, Integer> stageToJob = new ConcurrentHashMap<Integer, Integer>();

    // private boolean bottleneck = false;
    private double cpuThreshold;
    private double memoryThreshold;
    private SparkContext sc;

    protected static final ExecutorService executorService = Executors.newCachedThreadPool();
    private final int MAX_POLLING_ATTEMPTS = 10;
    private final long POLLING_INTERVAL_MS = 1000;
    private final long POLLING_TIMEOUT_MS = 10000;


    BottleneckDetector(SparkContext sc, double cpuThreshold, double memoryThreshold) {
        this.sc = sc;
        this.cpuThreshold = cpuThreshold;
        this.memoryThreshold = memoryThreshold;
        AnsiConsole.systemInstall();
    }


    // Filter and aggregate UDF data 
    private Map<String, Long> udfTotals(JsonNode root, Map<String, String> udfMap) {
        Map<String, String> implToUdf = new HashMap<>();
        udfMap.forEach((udf, impl) -> implToUdf.put(impl, udf));

        ArrayNode names  = (ArrayNode) root.at("/flamebearer/names");
        ArrayNode levels = (ArrayNode) root.at("/flamebearer/levels");

        Map<Integer, String> idxToUdf = new HashMap<>();
        for (int i = 0; i < names.size(); i++) {
            String frame = names.get(i).asText();
            if (frame.equals("total")) {
                idxToUdf.put(i, "total");
            }
            for (Map.Entry<String,String> e : implToUdf.entrySet()) {
                String slashed = e.getKey().replace(".", "/");
                if (frame.contains(slashed)) {  
                    idxToUdf.put(i, e.getValue()); 
                    break;                          
                }
            }
        }

        Map<String,Long> totals = new HashMap<>();
        for (JsonNode level : levels) {
            for (int p = 0; p < level.size(); p += 4) {
                int  nameIdx = level.get(p + 3).asInt();
                long incTime = level.get(p + 1).asLong();  

                if (idxToUdf.containsKey(nameIdx)) {
                    totals.merge(idxToUdf.get(nameIdx), incTime, Math::max);
                }
            }
        }

        return totals;
    }


    // Method to check if profile data is valid and complete
    private boolean isValidProfileData(JsonNode root, String unit, long jobId) {
        try {
            if (root == null) {
                return false;
            }

            if (!root.has("flamebearer") || 
                !root.at("/flamebearer").has("names") || 
                !root.at("/flamebearer").has("levels")) {
                return false;
            }

            ArrayNode names = (ArrayNode) root.at("/flamebearer/names");
            ArrayNode levels = (ArrayNode) root.at("/flamebearer/levels");
            
            if (names.size() == 0 || levels.size() == 0) {
                return false;
            }

            if (unit.equals("job")) {
                if (levels.get(0).get(1).asLong() == 0) {
                    return false;
                }
            }

            return true;

        } catch (Exception e) {
            return false;
        }
    }


    // Query Pyroscope for profiles
    private JsonNode queryPyro(String mode, String unit, long id) throws Exception {
        ObjectMapper JSON = new ObjectMapper();
        String appName = sc.getConf().get("spark.pyroscope.name", "spark-app");
        String serverUrl = sc.getConf().get("spark.pyroscope.server", "http://host.docker.internal:4000");

        String selector = mode + "{service_name=\"" + appName + "\"," + unit + "=\"" + id + "\"}";
        String url = serverUrl + "/pyroscope/render"
                + "?query=" + URLEncoder.encode(selector, StandardCharsets.UTF_8)
                + "&from=" + Listener.appStartTime + "&until=" + Instant.now().getEpochSecond()
                + "&groupBy=name";

        HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
        HttpResponse<String> res = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());

        if (res.statusCode() != 200) {
            throw new IllegalStateException("Pyroscope returned " + res.statusCode());
        }

        JsonNode resJson = JSON.readTree(res.body());
        return resJson;
    }


    // Query Pyroscope
    private JsonNode queryPyroWithValidation(String mode, String unit, long id) throws Exception {
        JsonNode query = null;

        try {
            query = queryPyro(mode, unit, id);
        } catch (Exception e) {
            System.out.println("Exception thrown by query: " + e.getMessage());
        }
        
        if (isValidProfileData(query, unit, id)) {
            return query;
        }

        throw new IllegalStateException("Query data is invalid.");
    }


    // Poll for job CPU data
    private CompletableFuture<JsonNode> pollCpuData(String unit, long id) {
        return CompletableFuture.supplyAsync(() -> {
          for (int attempt = 0; attempt < MAX_POLLING_ATTEMPTS; attempt++) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new RuntimeException("CPU polling was cancelled for job " + id);
                }
                try {
                    JsonNode result = queryPyroWithValidation(
                        "process_cpu:cpu:nanoseconds:cpu:nanoseconds", 
                        unit, 
                        id
                    );
                    
                    // System.out.println("CPU data ready for " + unit + " " + id + " after " + (attempt + 1) + " attempts");
                    return result;
                    
                } catch (Exception e) {
                    if (attempt == MAX_POLLING_ATTEMPTS - 1) {
                        throw new RuntimeException("Failed to get CPU data after " + MAX_POLLING_ATTEMPTS + " attempts, retrying", e);
                    }
                    
                    try {
                        System.out.println(unit + " " + id + " CPU query failed -> attempt " + (attempt + 1));
                        Thread.sleep(POLLING_INTERVAL_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Polling interrupted", ie);
                    }
                }
            }
            throw new RuntimeException("Polling failed - should not reach here");
        }, executorService);
    }


    // Poll for job MEM data 
    private CompletableFuture<JsonNode> pollMemData(String unit, long id) {
        return CompletableFuture.supplyAsync(() -> {
            for (int attempt = 0; attempt < MAX_POLLING_ATTEMPTS; attempt++) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new RuntimeException("Memory polling was cancelled for " + unit + " " + id);
                }
                try {
                    JsonNode result = queryPyroWithValidation(
                        "memory:alloc_in_new_tlab_bytes:bytes:space:bytes", 
                        unit, 
                        id
                    );
                    
                    // System.out.println("Memory data ready for " + unit + " " + id + " after " + (attempt + 1) + " attempts");
                    return result;
                    
                } catch (Exception e) {
                    if (attempt == MAX_POLLING_ATTEMPTS - 1) {
                        throw new RuntimeException("Failed to get memory data after " + MAX_POLLING_ATTEMPTS + " attempts", e);
                    }
                    
                    try {
                        System.out.println(unit + " " + id + " MEM query failed -> attempt " + (attempt + 1));
                        Thread.sleep(POLLING_INTERVAL_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Polling interrupted", ie);
                    }
                }
            }
            throw new RuntimeException("Polling failed - should not reach here");
        }, executorService);
    }


    // Method to handle both CPU and memory data polling concurrently
    protected void processJob(int jobId) {
        // Wait the upload interval to make sure all profiles are uploaded
        try {
            Thread.sleep(Driver.uploadInterval);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        CompletableFuture<JsonNode> cpuDataFuture = pollCpuData("job", jobId).orTimeout(POLLING_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        cpuDataFuture.thenAccept((cpuData) -> {
            try {    
                detectBottlenecks(jobId, cpuData, "CPU"); 
                Listener.jobTaskFutures.remove(jobId);
            } catch (Exception e) {
                System.err.println("Error processing job data for job " + jobId + ": " + e.getMessage());
                e.printStackTrace();
            }
        }).orTimeout(POLLING_TIMEOUT_MS, TimeUnit.MILLISECONDS);


        CompletableFuture<JsonNode> memoryDataFuture = pollMemData("job", jobId).orTimeout(POLLING_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        memoryDataFuture.thenAccept((memData) -> {
            try {                                        
                detectBottlenecks(jobId, memData, "MEM");
            } catch (Exception e) {
                System.err.println("Error processing job data for job " + jobId + ": " + e.getMessage());
                e.printStackTrace();
            }
        }).orTimeout(POLLING_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        
    }


    private void detectBottlenecks(int jobId, JsonNode data, String mode) {
        // bottleneck = false;
        try {
            Map<String,Long> udfMapping = udfTotals(data, MappingAgent.getUDFMappings());

            double jobTotal;
            if (mode.equals("CPU")) {
                jobTotal = (double) udfMapping.get("total") / 1000000000.0;
            } else {
                jobTotal = (double) udfMapping.get("total") / 1048576.0;
            }

            for (int stage : stageMap.get(jobId)) {
                if (!taskMap.containsKey(stage)) {
                    continue;
                }
                for (long task : taskMap.get(stage)) {
                    if (mode.equals("CPU")) {
                        if (!taskCpuTimes.containsKey(task)) {
                            System.out.println(ansi().fgMagenta().a("[BOTTLENECK DETECTOR]").fgCyan().a("[CPU]").reset().a(" Task data is missing: job " + jobId + ", stage " + stage + ", task " + task));
                            continue;
                        } 
                        taskCpuTimes.get(task).forEach((key, val) -> {
                            double percentage = val / jobTotal * 100.0;
                            if (percentage >= cpuThreshold && !key.equals("total")) {
                                // bottleneck = true;
                                    System.out.println(ansi().fgMagenta().a("[BOTTLENECK DETECTOR]").fgCyan().a("[CPU]").reset().a(" job " + jobId + " -> stage " + stage + " -> task " + task + ": ")
                                        .fgCyan().a(key) 
                                        .reset().a(" at " + MappingAgent.getUDFMappings().get(key) + ": " + String.format("%.2f", percentage) + "%, " + String.format("%.2f", val) + "s/" + String.format("%.2f", jobTotal) + "s"));
                            }
                        });
                    } else {
                        if (!taskMemoryValues.containsKey(task)) {
                            System.out.println(ansi().fgMagenta().a("[BOTTLENECK DETECTOR]").fgCyan().a("[MEM]").reset().a(" Task data is missing: job " + jobId + ", stage " + stage + ", task " + task));
                            continue;
                        } 
                        taskMemoryValues.get(task).forEach((key, val) -> {
                            double percentage = val / jobTotal * 100.0;
                            if (percentage >= memoryThreshold && !key.equals("total")) {
                                // bottleneck = true;
                                System.out.println(ansi().fgMagenta().a("[BOTTLENECK DETECTOR]").fgCyan().a("[MEM]").reset().a(" job " + jobId + " -> stage " + stage + " -> task " + task + ": ")
                                    .fgCyan().a(key)
                                    .reset().a(" at " + MappingAgent.getUDFMappings().get(key) + ": " + String.format("%.2f", percentage) + "%, " + String.format("%.2f", val) + "MB/" + String.format("%.2f", jobTotal) + "MB"));
                            }
                        });
                    }
                }
            }

            /* if (!bottleneck) {
                 System.out.println(ansi().fgMagenta().a("[BOTTLENECK DETECTOR]").fgCyan().a("[" + mode + "]").fgGreen().a(" Job " + jobId + " -> no bottlenecks detected.").reset());
            } */

        } catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }


    protected CompletableFuture<Void> processTask(long taskId) {
        // Wait the upload interval to make sure all profiles are uploaded
        try {
            Thread.sleep(Driver.uploadInterval);
        } catch (Exception e) {
            e.printStackTrace();
        }

        CompletableFuture<JsonNode> cpuDataFuture = pollCpuData("task", taskId).orTimeout(POLLING_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        CompletableFuture<Void> cpuAdded =cpuDataFuture.thenAccept((cpuData) -> {
            try {
                Map<String,Long> result = udfTotals(cpuData, MappingAgent.getUDFMappings());
                result.forEach((key, val) -> {
                    double seconds = (double) val / 1000000000.0;
                    taskCpuTimes.putIfAbsent(taskId, new ConcurrentHashMap<String, Double>());
                    taskCpuTimes.get(taskId).put(key, seconds);
                });
            } catch (Exception e) {
                System.err.println("Error processing task CPU data for task " + taskId + ": " + e.getMessage());
                e.printStackTrace();
            }
        }).orTimeout(POLLING_TIMEOUT_MS, TimeUnit.MILLISECONDS);


        CompletableFuture<JsonNode> memoryDataFuture = pollMemData("task", taskId).orTimeout(POLLING_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        CompletableFuture<Void> memoryAdded = memoryDataFuture.thenAccept((memData) -> {
            try {                                        
                Map<String,Long> resultMem = udfTotals(memData, MappingAgent.getUDFMappings());
                resultMem.forEach((key, val) -> {
                    double megaBytes = (double) val / 1048576.0;
                    taskMemoryValues.putIfAbsent(taskId, new ConcurrentHashMap<String, Double>());
                    taskMemoryValues.get(taskId).put(key, megaBytes);
                });
            } catch (Exception e) {
                System.err.println("Error processing task MEM data for task " + taskId + ": " + e.getMessage());
                e.printStackTrace();
            }
        }).orTimeout(POLLING_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        return CompletableFuture.allOf(cpuAdded, memoryAdded);
    }
}
