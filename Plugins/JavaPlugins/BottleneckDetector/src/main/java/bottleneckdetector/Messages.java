package bottleneckdetector;


import java.io.Serializable;
import org.apache.spark.rpc.RpcEndpointRef;
import scala.collection.Seq;


public class Messages {
    public static class JobSignal implements Serializable {
        private final int jobId;
        private final Seq<Object> stageIds;
        private final String event;


        public JobSignal(int jobId, Seq<Object> stageIds, String event) {
            this.jobId = jobId;
            this.stageIds = stageIds;
            this.event = event;
        }


        public JobSignal(int jobId, String event) {
            this.jobId = jobId;
            this.stageIds = null;
            this.event = event;
        }


        public int getJobId() {
            return jobId;
        }


        public Seq<Object> getStageIds() {
            return stageIds;
        }


        public String getEvent() {
            return event;
        }


        @Override
        public String toString() {
            return "JobSignal{jobId=" + jobId + ", event=" + event + "}";
        }
    }

    public static class ExecutorSignal implements Serializable {
        private final String executorId;
        private final RpcEndpointRef executorEndpoint;
        private final String message;


        public ExecutorSignal(String executorId, RpcEndpointRef executorEndpoint, String message) {
            this.executorId = executorId;
            this.executorEndpoint = executorEndpoint;
            this.message = message;
        }


        public String getExecutorId() {
            return executorId;
        }


        public RpcEndpointRef getExecutorEndpoint() {
            return executorEndpoint;
        }


        public String getMessage() {
            return message;
        }


        @Override
        public String toString() {
            return "ExecutorSignal{executorId='" + executorId + "', executorEndpoint='" + executorEndpoint + "', message='" + message + "'}";
        }
    }
}