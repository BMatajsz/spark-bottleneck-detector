package bottleneckdetector;

import org.apache.spark.api.plugin.*;


public class BottleneckDetectorPlugin implements SparkPlugin {
    @Override
    public DriverPlugin driverPlugin() {
        return new Driver();
    }


    @Override
    public ExecutorPlugin executorPlugin() {
        return new Executor();
    }
}
