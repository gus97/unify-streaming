package cn.migu.streaming.db;

import cn.migu.streaming.model.JarConfParam;
import org.apache.spark.SparkContext;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.sql.Timestamp;
import java.util.UUID;

/**
 *
 * 启动进程注册
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class RegisterProcess implements Serializable
{
    private String ADDAPP_SQL =
        "INSERT INTO SPARK_APPLICATION ( OBJ_ID, APPID, APP_NAME,STATUS, CORES, SPARK_MEMORY, USER_NAME,SUBMIT_IP, START_TIME, SPARK_MODE) VALUES(?,?,?,?,?,?,?,?,?,?)";

    private String ADDPROC_SQL =
        "INSERT INTO UNIFY_PROCESS (OBJ_ID,APP_ID,SERVER_ID,JAR_ID,PORT,KIND,PROCESS_NO, MEMORY,CPUS,DEAL_TIME, DEAL_USER) VALUES(?,?,?,?,?,?,?,?,?,?,?)";

    private String ADDPROCLOG_SQL =
        "INSERT INTO UNIFY_PROCESS_LOG (OBJ_ID, APP_ID, SERVER_ID,JAR_ID,PORT,STATUS,KIND,PROCESS_NO,ACTION,MEMORY,CPUS,DEAL_TIME,DEAL_USER) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private void addApp(JarConfParam jarConfParam, SparkContext sparkContext)
    {
        Object[] params = new Object[10];
        params[0] = UUID.randomUUID().toString().replace("-","");
        params[1] = sparkContext.getConf().getAppId();
        params[2] = sparkContext.appName();
        params[3] = "RUNNING";
        params[4] = sparkContext.getConf().get("spark.cores.max");
        params[5] = sparkContext.getConf().get("spark.executor.memory");
        params[6] = jarConfParam.getDealUser();
        params[7] = jarConfParam.getServiceId();
        params[8] = new Timestamp(System.currentTimeMillis());
        params[9] = sparkContext.getConf().get("spark.scheduler.mode");

        SimpleDbUtil.getInstance().executeSql(ADDAPP_SQL,params);
    }

    private void addProcess(JarConfParam jarConfParam,SparkContext sparkContext)
    {
        Object[] params = new Object[11];
        params[0] = UUID.randomUUID().toString().replace("-","");
        params[1] = jarConfParam.getAppId();
        params[2] = jarConfParam.getServiceId();
        params[3] = jarConfParam.getJarId();
        params[4] = jarConfParam.getJarPort();
        params[5] = "2";
        params[6] = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        params[7] = sparkContext.getConf().get("spark.executor.memory");
        params[8] = sparkContext.getConf().get("spark.cores.max");
        params[9] = new Timestamp(System.currentTimeMillis());
        params[10] = jarConfParam.getDealUser();

        SimpleDbUtil.getInstance().executeSql(ADDPROC_SQL,params);
    }

    private void addProcessLog(JarConfParam jarConfParam,SparkContext sparkContext)
    {
        Object[] params = new Object[13];
        params[0] = UUID.randomUUID().toString().replace("-","");
        params[1] = jarConfParam.getAppId();
        params[2] = jarConfParam.getServiceId();
        params[3] = jarConfParam.getJarId();
        params[4] = jarConfParam.getJarPort();
        params[5] =  "1";
        params[6] = "2";
        params[7] = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        params[8] = 1;
        params[9] = sparkContext.getConf().get("spark.executor.memory");
        params[10] = sparkContext.getConf().get("spark.cores.max");
        params[11] = new Timestamp(System.currentTimeMillis());
        params[12] = jarConfParam.getDealUser();

        SimpleDbUtil.getInstance().executeSql(ADDPROCLOG_SQL,params);
    }

    public void register(JarConfParam jarConfParam, SparkContext sparkContext)
    {
        this.addApp(jarConfParam,sparkContext);
        this.addProcess(jarConfParam,sparkContext);
        this.addProcessLog(jarConfParam,sparkContext);
    }
}
