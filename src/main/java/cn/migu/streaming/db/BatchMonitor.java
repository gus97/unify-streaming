package cn.migu.streaming.db;

import cn.migu.streaming.common.ConfigUtils;
import cn.migu.streaming.common.DateUtils;
import com.typesafe.config.Config;

import java.sql.Connection;
import java.util.Date;

/**
 * 批次处理监控
 * author  zhaocan
 * version  [版本号, 2017/5/2]
 * see  [相关类/方法]
 * since  [产品/模块版本]
 */
public class BatchMonitor
{
    private final static String BATCH_UPDATE_TIMESTAMP_SQL = "UPDATE UNIFY_JAR_MONITOR_VALUE SET MONITOR_VALUE=? WHERE KEY_VALUE=? AND CODE='lastUpdateTime'";

    private final static String BATCH_UPDATE_ORIGNUM_SQL = "UPDATE UNIFY_JAR_MONITOR_VALUE SET MONITOR_VALUE=MONITOR_VALUE+? WHERE KEY_VALUE=? AND CODE='originalNum'";

    private final static String BATCH_UPDATE_VALIDNUM_SQL = "UPDATE UNIFY_JAR_MONITOR_VALUE SET MONITOR_VALUE=MONITOR_VALUE+? WHERE KEY_VALUE=? AND CODE='validNum'";

    private static BatchMonitor instance;

    private Config config;

    private Object[] params = null;

    private BatchMonitor()
    {
        this.config = ConfigUtils.getConfig("db.conf");
        params = new Object[2];
    }

    public static BatchMonitor getInstance()
    {
        if(null == instance)
        {
            instance = new BatchMonitor();
        }
        return instance;
    }

    public void update(long batchInputNum,long batchValidNum,String jarId)
    {
        String driver = config.getString("db.driver");
        String url = config.getString("db.url");
        String username = config.getString("db.username");
        String password = config.getString("db.password");

        Date refreshTime = new Date();

        params[1] = jarId;

        Connection conn = SimpleDbUtil.getInstance().getDbConnect(driver, url, username, password);
        params[0] = String.valueOf(batchInputNum);
        SimpleDbUtil.getInstance().executeSqlIgnoreExcep(conn, BATCH_UPDATE_ORIGNUM_SQL, params);

        params[0] = String.valueOf(batchValidNum);
        SimpleDbUtil.getInstance().executeSqlIgnoreExcep(conn, BATCH_UPDATE_VALIDNUM_SQL, params);

        params[0] = DateUtils.getCurrentDate("yyyy-MM-dd HH:mm:ss");
        SimpleDbUtil.getInstance().executeSqlIgnoreExcep(conn, BATCH_UPDATE_TIMESTAMP_SQL, params);

        SimpleDbUtil.getInstance().close(conn);

    }

}
