package cn.migu.streaming.db;

import cn.migu.streaming.common.ConfigUtils;
import cn.migu.streaming.model.DataCache;
import cn.migu.streaming.model.DataTraceModel;
import cn.migu.streaming.model.FileCleanConf;
import cn.migu.streaming.model.JarConfParam;
import com.typesafe.config.Config;
import kafka.common.TopicAndPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

/**
 * streaming处理数据trace
 * 存数据库
 * author  zhaocan
 * version  [版本号, 2016/10/14]
 * see  [相关类/方法]
 * since  [产品/模块版本]
 */
public final class DataTraceLog
{
    private static String ADDTRACESQL =
        "INSERT INTO UNIFY_STREAMING_DATA_TRACE(OBJ_ID, APP_ID,SERVER_ID,JAR_ID, PROCESSID,TOPIC,GROUPID,PARTITION,OFFSET, REV_COUNT, HDL_COUNT,TOTAL_COUNT, DEALTIME) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

    //private static String DELTRACESQL = "DELETE FROM UNIFY_STREAMING_DATA_TRACE WHERE OBJ_ID=?";

    private Config dbConf;

    private Object[] params = null;

    private static DataTraceLog instance;

    private DataTraceLog()
    {
        this.dbConf = ConfigUtils.getConfig("db.conf");
        params = new Object[13];
    }

    public static DataTraceLog getInstance()
    {
        if (null == instance)
        {
            instance = new DataTraceLog();
        }
        return instance;
    }

    /**
     * 初始化日志实体类对象
     * @param dataCache
     * @param update
     * @return
     */
    public DataTraceModel makeDataTraceModel(DataCache dataCache, boolean update)
    {
        DataTraceModel entity = new DataTraceModel();
        JarConfParam jarConfParam = dataCache.getJarConfParam();
        FileCleanConf fileCleanConf = dataCache.getFileCleanConf();
        entity.setAppId(jarConfParam.getAppId());
        entity.setServerId(jarConfParam.getServiceId());
        entity.setJarId(jarConfParam.getJarId());
        entity.setGroupid(dataCache.getKafkaConf().getUsername());
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        String pid = rt.getName().replaceAll("@.*", "");
        entity.setProcessid(pid);

        if (update)
        {
            entity.setRevCount(fileCleanConf.getMsgCountfromKafka());
            entity.setHdlCount(fileCleanConf.getMsgCountAfterClean());

            if (entity.getHdlCount() > 0)
            {
                JavaRDD<Row> unboundTb = dataCache.getFileCleanConf().getFullData();
                if (null != dataCache.getFileCleanConf().getFullData())
                {
                    entity.setTotalCount(unboundTb.count());
                }
            }
        }

        //entity.setRevCount(dataCache.getFileCleanConf().getMsgCountfromKafka());

        return entity;
    }

    /**
     * 系统初始化时记录kafka offset信息
     * @param traceLog
     * @param offsets
     */
    @Deprecated
    public void initTrace(DataTraceModel traceLog, Map<TopicAndPartition, Long> offsets)
    {
        params[1] = traceLog.getAppId();
        params[2] = traceLog.getServerId();
        params[3] = traceLog.getJarId();
        params[4] = traceLog.getProcessid();
        params[6] = traceLog.getGroupid();
        params[9] = 0;
        params[10] = 0;
        params[11] = 0;

        String driver = dbConf.getString("db.driver");
        String url = dbConf.getString("db.url");
        String username = dbConf.getString("db.username");
        String password = dbConf.getString("db.password");

        Connection conn = SimpleDbUtil.getInstance().getDbConnect(driver, url, username, password);

        for (Map.Entry<TopicAndPartition, Long> entry : offsets.entrySet())
        {
            TopicAndPartition key = entry.getKey();
            long offset = entry.getValue();
            params[0] = UUID.randomUUID().toString().replace("-", "");
            params[5] = key.topic();
            params[7] = key.partition();
            params[8] = offset;
            params[12] = new Timestamp(new Date().getTime());

            SimpleDbUtil.getInstance().executeSql(conn, ADDTRACESQL, params);
        }

        SimpleDbUtil.getInstance().close(conn);
    }

    /**
     * streaming单次处理后kafka offset,处理数据量等信息
     * @param traceLog
     * @param offsets
     */
    public void updateTrace(DataTraceModel traceLog, Map<TopicAndPartition, Long> offsets)
    {
        /*if(0 == traceLog.getRevCount())
        {
            SimpleDbUtil.getInstance().executeSql(conn, DELTRACESQL, new Object[]{});
        }*/

        if(0 == traceLog.getRevCount())
        {
            return;
        }

        params[1] = traceLog.getAppId();
        params[2] = traceLog.getServerId();
        params[3] = traceLog.getJarId();
        params[4] = traceLog.getProcessid();
        params[6] = traceLog.getGroupid();
        params[9] = traceLog.getRevCount();
        params[10] = traceLog.getHdlCount();
        params[11] = traceLog.getTotalCount();

        String driver = dbConf.getString("db.driver");
        String url = dbConf.getString("db.url");
        String username = dbConf.getString("db.username");
        String password = dbConf.getString("db.password");

        Connection conn = SimpleDbUtil.getInstance().getDbConnect(driver, url, username, password);

        for (Map.Entry<TopicAndPartition, Long> entry : offsets.entrySet())
        {
            TopicAndPartition key = entry.getKey();
            long offset = entry.getValue();
            params[0] = UUID.randomUUID().toString().replace("-", "");
            params[5] = key.topic();
            params[7] = key.partition();
            params[8] = offset;
            params[12] = new Timestamp(new Date().getTime());

            SimpleDbUtil.getInstance().executeSql(conn, ADDTRACESQL, params);
        }

        SimpleDbUtil.getInstance().close(conn);
    }

}
