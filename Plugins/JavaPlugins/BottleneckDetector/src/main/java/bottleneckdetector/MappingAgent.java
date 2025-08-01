package bottleneckdetector;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.matcher.ElementMatchers;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import java.util.concurrent.Callable;
import java.util.Map;


public class MappingAgent {
    private static final Map<String, String> udfMappings = new ConcurrentHashMap<>();
    public static final Map<UserDefinedFunction, Class<?>> udafImpl = new ConcurrentHashMap<>();
    

    public static void premain(String agentArgs, Instrumentation inst) {
        installAgent(inst);
    }
    

    public static void agentmain(String agentArgs, Instrumentation inst) {
        installAgent(inst);
    }


    private static void installAgent(Instrumentation instrumentation) {
        new AgentBuilder.Default()
            .type(ElementMatchers.named("org.apache.spark.sql.UDFRegistration"))
            .transform((builder, typeDescription, classLoader, module, protectionDomain) -> 
                builder.method(ElementMatchers.named("register")
                    .and(ElementMatchers.takesArguments(3))
                    .or(ElementMatchers.takesArguments(2))
                    .and(ElementMatchers.takesArgument(0, String.class)))
                .intercept(MethodDelegation.to(UDFInterceptor.class)))
            .installOn(instrumentation);
        
        System.out.println("Spark UDF Mapping Agent installed successfully");

        new AgentBuilder.Default()
            .type(ElementMatchers.nameContains("org.apache.spark.sql.expressions.UserDefinedAggregator")) 
            .transform((builder, typeDescription, classLoader, module, protectionDomain) ->
                builder.constructor(ElementMatchers.any())
                .intercept(Advice.to(UDAFInterceptor.class)))
            .installOn(instrumentation);
        
        System.out.println("Spark UDAF Mapping Agent installed successfully");


        new AgentBuilder.Default()
            .type(ElementMatchers.named(
                   "org.apache.spark.sql.catalyst.analysis.SimpleFunctionRegistry"))
            .transform((builder, td, cl, m, pd) ->
                builder.method(ElementMatchers.named("registerFunction"))
                       .intercept(MethodDelegation.to(SQLDDLInterceptor.class)))
            .installOn(instrumentation);

        System.out.println("Spark SQL-DDL UDF Mapping Agent installed successfully");
    }
    

    public static Map<String, String> getUDFMappings() {
        return new ConcurrentHashMap<>(udfMappings);
    }
    

    public static class UDFInterceptor {
        @RuntimeType
        public static Object intercept(@AllArguments Object[] args, 
                                     @SuperCall Callable<?> callable,
                                     @Origin Method method) throws Exception {
                         
            if (args.length >= 2 && args[0] instanceof String && args[1] != null) {
                String udfName = (String) args[0];
                Object udfImplementation = args[1];
                String implName = (udfImplementation instanceof UserDefinedFunction) 
                    ? udafImpl.get(udfImplementation).getName() 
                    : udfImplementation.getClass().getName();
                
                udfMappings.put(udfName, implName);
                System.out.println("UDF/UDAF Registration intercepted: " + udfName + " -> " + implName);
            }
            
            return callable.call();
        }
    }


    public static class UDAFInterceptor {
        @Advice.OnMethodExit
        static void exit(@Advice.This Object wrapper,
                        @Advice.Argument(0) Object aggregator) {

            Class<?> impl = aggregator.getClass();
            udafImpl.put((UserDefinedFunction) wrapper, impl); 
        }
    }

    
    public static class SQLDDLInterceptor {
        @RuntimeType
        public static Object intercept(@AllArguments Object[] args,
                                       @SuperCall Callable<?> call,
                                       @Origin Method origin) throws Exception {

            if (args[0] != null) {
                String funcName = args[0].toString();   
                if (udfMappings.get(funcName) == null) {
                    ExpressionInfo exp = (ExpressionInfo) args[1];
                    String implName = exp.getClassName();
                    udfMappings.put(funcName, implName);
                    System.out.println("SQL-DDL UDF Registration intercepted: " + funcName + " -> " + implName);
                }     
            }
            return call.call(); 
        }
    }
 }
