package cn.migu.streaming.db;

import cn.migu.streaming.common.ConfigUtils;
import com.typesafe.config.Config;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/**
 * 数据库操作类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class SimpleDbUtil {

    private static final Logger log = LoggerFactory.getLogger("streaming");

    private static SimpleDbUtil instance;

    private Config dbConf;

    private SimpleDbUtil() {
        dbConf = ConfigUtils.getConfig("db.conf");
    }

    public static SimpleDbUtil getInstance() {
        if (null == instance) {
            instance = new SimpleDbUtil();
        }

        return instance;
    }

    /**
     * 数据库连接对象
     *
     * @param driver
     * @param url
     * @param user
     * @param password
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    public Connection getDbConnect(String driver, String url, String user, String password) {
        Connection _connect = null;
        try {
            if (DbUtils.loadDriver(driver)) {
                _connect = DriverManager.getConnection(url, user, password);
            } else {
                log.info("load driver error:{}", driver);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("数据库连接错误:", e);
            System.exit(1);
        }

        return _connect;
    }

    /**
     * 根据sql查询返回实体类集合
     *
     * @param sql
     * @param clazz
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    public <T> List<T> query(String sql, Class<T> clazz) {
        List<T> beans = null;
        try {
            String driver = dbConf.getString("db.driver");
            String url = dbConf.getString("db.url");
            String username = dbConf.getString("db.username");
            String password = dbConf.getString("db.password");

            Connection conn = this.getDbConnect(driver, url, username, password);
            beans = (List<T>) new QueryRunner().query(conn, sql, new BeanListHandler(clazz));

            this.close(conn);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(String.format("%s查询错误:", sql), e);
            System.exit(1);
            return null;
        }

        return beans;
    }

    /**
     * 根据sql查询返回实体类
     *
     * @param sql
     * @param clazz
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    public <T> T queryOne(String sql, Class<T> clazz) {
        List<T> coll = this.query(sql, clazz);

        if (null != coll && coll.size() > 0) {
            return coll.get(0);
        }

        return null;
    }

    /**
     * 执行sql
     *
     * @param conn 数据库连接对象
     * @param sql  执行sql语句
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    public void executeSql(Connection conn, String sql) {
        try {
            new QueryRunner().update(conn, sql);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(String.format("%s执行错误:", sql), e);
            close(conn);
            System.exit(1);
        }
    }

    /**
     * 执行sql
     *
     * @param conn 数据库连接对象
     * @param sql  执行sql语句
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    public void executeSql(Connection conn, String sql, Object... params) {
        try {
            new QueryRunner().update(conn, sql, params);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(String.format("%s执行错误:", sql), e);
            close(conn);
            System.exit(1);
        }
    }

    /**
     * 执行sql,只记录异常
     *
     * @param conn
     * @param sql
     * @param params
     */
    public void executeSqlIgnoreExcep(Connection conn, String sql, Object... params) {
        try {
            new QueryRunner().update(conn, sql, params);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(String.format("%s执行错误:", sql), e);
        }
    }

    /**
     * 执行sql
     *
     * @param sql    sql语句
     * @param params sql语句中的参数
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    public void executeSql(String sql, Object... params) {
        try {
            String driver = dbConf.getString("db.driver");
            String url = dbConf.getString("db.url");
            String username = dbConf.getString("db.username");
            String password = dbConf.getString("db.password");

            Connection conn = this.getDbConnect(driver, url, username, password);

            new QueryRunner().update(conn, sql, params);

            this.close(conn);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(String.format("%s执行错误:", sql), e);
            System.exit(1);
        }
    }

    /**
     * 关闭连接
     *
     * @param conn
     */
    public void close(Connection conn) {
        try {
            DbUtils.close(conn);
        } catch (Exception e) {
            log.error("关闭数据库连接失败", e);
        }
    }

}
