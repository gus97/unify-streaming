package cn.migu.streaming.db;

import cn.migu.streaming.common.ConfigUtils;
import cn.migu.streaming.model.DataCache;
import cn.migu.streaming.model.OperateSql;
import cn.migu.streaming.model.PreloadTb;
import cn.migu.streaming.model.SqlLogModel;
import com.typesafe.config.Config;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;

/**
 *
 * sql日志操作类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class SqlLog implements Serializable
{
    private static String SQLLOG_ADD =
        "INSERT INTO UNIFY_SQL_LOG(OBJ_ID, APP_ID,SERVER_ID,JAR_ID, KIND,SEQ,JAR_SQL,START_TIME, END_TIME, STATE, ELAPSE) VALUES ( ?,?,?,?,?,?,?,?,?,?,?)";

    private Object[] params = null;

    private Config config;

    private static SqlLog instance = null;

    public SqlLog()
    {
        this.config = ConfigUtils.getConfig("db.conf");
        params = new Object[11];
    }

    public static SqlLog getInstance()
    {
        if (null == instance)
        {
            instance = new SqlLog();
        }

        return instance;
    }

    /**
     * 组件sql log对象
     * @param out 输出sql对象
     * @param cache streaming数据业务处理上下文
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    public SqlLogModel makeSqlLog(OperateSql out, DataCache cache)
    {
        SqlLogModel sqlLog = new SqlLogModel();
        sqlLog.setAppId(cache.getJarConfParam().getAppId());
        sqlLog.setJarId(cache.getJarConfParam().getJarId());
        sqlLog.setServerId(cache.getJarConfParam().getServiceId());
        sqlLog.setJarSql(out.getSql());
        sqlLog.setStartTime(new Date());
        sqlLog.setSeq(out.getSeq());

        return sqlLog;
    }

    /**
     * 组件sql log对象
     * @param in 输出sql对象
     * @param cache streaming数据业务处理上下文
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    public SqlLogModel makeSqlLog(PreloadTb in, DataCache cache)
    {
        SqlLogModel sqlLog = new SqlLogModel();
        sqlLog.setAppId(cache.getJarConfParam().getAppId());
        sqlLog.setJarId(cache.getJarConfParam().getJarId());
        sqlLog.setServerId(cache.getJarConfParam().getServiceId());
        sqlLog.setJarSql(in.getRunSql());
        sqlLog.setStartTime(new Date());
        //sqlLog.setKind(ds.getKind());
        //sqlLog.setSeq(in.getSeq());

        return sqlLog;
    }

    /**
     * 保存sql log实体类
     * @param entity sql model实体类
     * @param state  状态
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    public void save(SqlLogModel entity, int state, int type)
    {
        Date endTime = new Date();
        entity.setEndTime(endTime);

        long elapse = endTime.getTime() - entity.getStartTime().getTime();
        entity.setElapse(elapse);

        entity.setState(state);

        this.insert(entity, type);

    }

    /**
     * 保存sql model log
     * @param entity sql model实体类
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    private void insert(SqlLogModel entity, int type)
    {
        params[0] = UUID.randomUUID().toString().replace("-", "");
        params[1] = entity.getAppId();
        params[2] = entity.getServerId();
        params[3] = entity.getJarId();
        params[4] = type;
        params[5] = entity.getSeq();
        params[6] = entity.getJarSql();
        params[7] = new Timestamp(entity.getStartTime().getTime());
        params[8] = new Timestamp(entity.getEndTime().getTime());
        params[9] = entity.getState();
        params[10] = entity.getElapse();

        //SimpleDbUtil.getInstance().executeSql(SQLLOG_ADD, params);
        String driver = config.getString("db.driver");
        String url = config.getString("db.url");
        String username = config.getString("db.username");
        String password = config.getString("db.password");

        Connection conn = SimpleDbUtil.getInstance().getDbConnect(driver, url, username, password);
        SimpleDbUtil.getInstance().executeSql(conn, SQLLOG_ADD, params);

        SimpleDbUtil.getInstance().close(conn);
    }
}
