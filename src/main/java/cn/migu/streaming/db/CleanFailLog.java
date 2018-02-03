package cn.migu.streaming.db;

import cn.migu.streaming.common.ConfigUtils;
import cn.migu.streaming.model.CleanFailRecord;
import com.typesafe.config.Config;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.UUID;

/**
 *
 *
 * author  zhaocan
 * version  [版本号, 2017/2/6]
 * see  [相关类/方法]
 * since  [产品/模块版本]
 */
public class CleanFailLog implements Serializable
{
    private static String CLEANFAILLOG_ADD =
        "INSERT INTO UNIFY_CLEAN_FAIL(OBJ_ID, SERVER_ID,JAR_ID, SOURCE_ID,FAIL_KIND,FAIL_RECORD,NOTE, START_NUM, DEAL_TIME) VALUES ( ?,?,?,?,?,?,?,?,?)";


    private Object[] params = null;

    private Config config;

    private static CleanFailLog instance = null;

    private CleanFailLog()
    {
        this.config = ConfigUtils.getConfig("db.conf");
        params = new Object[9];
    }

    public static CleanFailLog getInstance()
    {
        if (null == instance)
        {
            instance = new CleanFailLog();
        }

        return instance;
    }

    /**
     * 失败记录入库
     * @param failRecord
     */
    public void insert(CleanFailRecord failRecord)
    {
        params[0] = UUID.randomUUID().toString().replace("-", "");
        params[1] = failRecord.getServerId();
        params[2] = failRecord.getJarId();
        params[3] = failRecord.getSourceId();
        params[4] = failRecord.getFailKind();
        params[5] = failRecord.getFailRecord();
        params[6] = failRecord.getFailCode();
        params[7] = failRecord.getStartNum();
        params[8] = new Timestamp(failRecord.getDealTime().getTime());

        String driver = config.getString("db.driver");
        String url = config.getString("db.url");
        String username = config.getString("db.username");
        String password = config.getString("db.password");

        Connection conn = SimpleDbUtil.getInstance().getDbConnect(driver, url, username, password);
        SimpleDbUtil.getInstance().executeSql(conn, CLEANFAILLOG_ADD, params);

        SimpleDbUtil.getInstance().close(conn);
    }
}
