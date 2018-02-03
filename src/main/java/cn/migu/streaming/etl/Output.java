package cn.migu.streaming.etl;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import cn.migu.scala.streaming.ScalaToJavaUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import cn.migu.unify.hugetable.jdbc.DbUtils;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import cn.migu.streaming.common.DateUtils;
import cn.migu.streaming.common.MethodCallUtils;
import cn.migu.streaming.db.SimpleDbUtil;
import cn.migu.streaming.db.SqlLog;
import cn.migu.streaming.model.DataCache;
import cn.migu.streaming.model.DataSourceConf;
import cn.migu.streaming.model.DateFormatStr;
import cn.migu.streaming.model.OperateSql;
import cn.migu.streaming.model.SqlLogModel;
import cn.migu.streaming.redis.JedisConnectionPool;
import redis.clients.jedis.Jedis;
import scala.collection.JavaConverters;

/**
 * 文件清洗后数据格式化输出
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class Output implements Serializable {
    private static final Logger log = LoggerFactory.getLogger("streaming");

    private static Output instance;

    private final Map<String, String> replDateMap;

    private Output() {
        replDateMap = Maps.newConcurrentMap();
        replDateMap.put("$TODAY_D$", "inLineToDay");
        replDateMap.put("$TODAY_H$", "inLineToHour");
        replDateMap.put("$TODAY_M$", "inLineToMinite");
        replDateMap.put("$TODAY_S$", "inLineToSec");
        replDateMap.put("$TODAYD$", "compactToDay");
        replDateMap.put("$TODAYH$", "compactToHour");
        replDateMap.put("$TODAYM$", "compactToMinite");
        replDateMap.put("$TODAYS$", "compactToSec");

        replDateMap.put("$TODAY_D_FIX$", "inLineToDayFixJar");
        replDateMap.put("$TODAY_H_FIX$", "inLineToHourFixJar");
        replDateMap.put("$TODAY_M_FIX$", "inLineToMiniteFixJar");
        replDateMap.put("$TODAY_S_FIX$", "inLineToSecFixJar");
        replDateMap.put("$TODAYD_FIX$", "compactToDayFixJar");
        replDateMap.put("$TODAYH_FIX$", "compactToHourFixJar");
        replDateMap.put("$TODAYM_FIX$", "compactToMiniteFixJar");
        replDateMap.put("$TODAYS_FIX$", "compactToSecFixJar");

    }

    public static Output getInstance() {
        if (null == instance) {
            instance = new Output();
        }
        return instance;
    }

    /**
     * 根据配置数据源数据
     *
     * @param config 环境配置
     * @param cache  streaming数据业务处理上下文
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    public void execute(final Config config, DataCache cache) {
        List<OperateSql> outOperation = cache.getOutputSqls();

        if (null != outOperation) {
            String origDsSql = config.getString("sql.datasourcesql");

            DateFormatStr.instance().update();

            DataSourceConf ds;

            for (OperateSql out : outOperation) {
                ds = null;

                //直接入hive表时,不需要输出数据源配置
                if (RunMode.valueOf(out.getRunMode()) != RunMode.DIRECT_HIVE) {
                    String dataSourceSql = String.format(origDsSql, out.getSourceId());

                    ds = SimpleDbUtil.getInstance().queryOne(dataSourceSql, DataSourceConf.class);
                    if (null == ds) {
                        continue;
                    }

                    log.info("输出数据源配置为:{}", ds.toString());
                }

                //替换输出sql标签
                String oSql = out.getSql();
                oSql = this.replaceSqlLabel(oSql, DateFormatStr.instance());
                //out.setSql(oSql);

                OperateSql _out = this.copyOutSql(out);
                _out.setSql(oSql);

                SqlLogModel sqlLogModel = SqlLog.getInstance().makeSqlLog(_out, cache);
                try {
                    this.output(ds, _out, cache);
                    SqlLog.getInstance().save(sqlLogModel, 1, 5);
                } catch (Exception e) {
                    log.error("输出错误", e);
                    SqlLog.getInstance().save(sqlLogModel, 2, 5);
                    System.exit(1);
                }
            }
        }
    }


    public String getHttpURL(final Config config, DataCache cache) {
        List<OperateSql> outOperation = cache.getOutputSqls();

        if (null != outOperation) {
            String origDsSql = config.getString("sql.datasourcesql");

            DateFormatStr.instance().update();

            DataSourceConf ds;

            for (OperateSql out : outOperation) {
                ds = null;

                //直接入hive表时,不需要输出数据源配置
                if (RunMode.valueOf(out.getRunMode()) != RunMode.DIRECT_HIVE) {
                    String dataSourceSql = String.format(origDsSql, out.getSourceId());

                    ds = SimpleDbUtil.getInstance().queryOne(dataSourceSql, DataSourceConf.class);
                    if (null == ds) {
                        continue;
                    }

                    return ds.toString();
                }

            }
        }
        return null;
    }

    /**
     * 拷贝数据库原始数据sql配置信息
     *
     * @param outSql
     * @return
     */
    private OperateSql copyOutSql(OperateSql outSql) {
        OperateSql _out = new OperateSql();
        _out.setSourceId(outSql.getSourceId());
        _out.setSeq(outSql.getSeq());
        _out.setMapTable(outSql.getMapTable());
        _out.setRunMode(outSql.getRunMode());

        return _out;
    }

    /**
     * 输出操作
     *
     * @param ds    数据源配置
     * @param out   输出配置
     * @param cache streaming数据业务处理上下文
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    private void output(DataSourceConf ds, OperateSql out, DataCache cache) {
        //运行方式:1-加载内存表,2-jdbc连接,3-直接sparksql操作hive方式
        RunMode mode = RunMode.valueOf(out.getRunMode());

        if (RunMode.DIRECT_HIVE == mode) {
            this.toHive(out, cache);
        } else {
            //输出源类型
            SourceType type = SourceType.valueOf(ds.getKind());

            switch (type) {
                case KAFKA:
                    break;
                case ORACLE:
                case HBASE:
                case MYSQL:
                    this.toDb(out, ds, cache);
                    break;
                case REDIS:
                    this.toRedis(out, ds, cache);
                    break;
                case FTP:
                    toFtpServer(out, ds, cache);
                    break;
            }
        }

    }

    /**
     * 输出sql标签替换
     *
     * @param sql     执行sql
     * @param dateStr 日期替换标签
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    private String replaceSqlLabel(String sql, DateFormatStr dateStr) {
        String _sql = sql;

        //dateStr.update();

        for (Map.Entry<String, String> entry : this.replDateMap.entrySet()) {
            _sql = StringUtils.replace(_sql,
                    entry.getKey(),
                    (String) MethodCallUtils.invokeGetMethod(dateStr, entry.getValue()));
        }

        return _sql;
    }

    /**
     * 输出至HIVE
     *
     * @param out   输出配置
     * @param cache streaming数据业务处理上下文
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    private void toHive(OperateSql out, DataCache cache) {
        SparkSession hiveContext = cache.getSparkSession();

        String sql = out.getSql();

        log.info("DIRECT_HIVE SQL:{}", sql);
        hiveContext.sql(sql);
        //hiveContext.executePlan(hiveContext.sql(sql).logicalPlan());
        /*int runMode = out.getRunMode();
        
        RunMode mode = RunMode.valueOf(runMode);
        switch (mode)
        {
            case HIVE_DIRECT:
                log.info("HIVE_DIRECT SQL:{}", sql);
                hiveContext.sql(sql);
                break;
            case HIVE_APPEND:
                String appendSql = StringUtils.join("insert into ", out.getMapTable(), " ", sql);
                log.info("HIVE_APPEND SQL:{}", appendSql);
                hiveContext.sql(appendSql);
                break;
            case HIVE_OVERWRITE:
                String overwriteSql = StringUtils.join("insert overwrite table ", out.getMapTable(), " ", sql);
                log.info("HIVE_OVERWRITE SQL:{}", overwriteSql);
                hiveContext.sql(overwriteSql);
                break;
        }*/

    }

    /**
     * 输出至关系数据库
     *
     * @param out   输出配置
     * @param ds    数据源配置
     * @param cache streaming数据业务处理上下文
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    private void toDb(OperateSql out, DataSourceConf ds, DataCache cache) {
        int runMode = out.getRunMode();

        SparkSession hiveContext = cache.getSparkSession();

        RunMode mode = RunMode.valueOf(runMode);

        switch (mode) {
            case JDBC:
                SimpleDbUtil dbMgr = SimpleDbUtil.getInstance();
                Connection conn =
                        dbMgr.getDbConnect(ds.getDriver(), ds.getConnectUrl(), ds.getUserName(), ds.getPassword());
                log.info("JDBC-CONNETC-SQL:{}", out.getSql());
                
                /*boolean isHt = isHtJdbcConn(ds.getDriver());
                if(isHt)
                {
                    log.info("is hugetable connect");
                    Statement stat = DbUtils.getStatement(conn);
                    cache.setStatement(stat);
                    DbUtils.executeSql(stat,out.getSql());
                
                    cache.setStatement(null);
                    log.info("is hugetable connect end");
                }
                else
                {
                    log.info("is not hugetable connect");
                    dbMgr.executeSql(conn, out.getSql());
                }*/
                dbMgr.executeSql(conn, out.getSql());
                dbMgr.close(conn);

                break;
            case DATAFRAME_TO_DB:
                log.info("DATAFRAME_TO_DB-SQL:{}", out.getSql());
                Dataset<Row> df = hiveContext.sql(out.getSql()).toDF();
                //df.show();
                /*StructField[] ss = df.schema().fields();
                for(StructField field : ss)
                {
                    log.info(field.dataType().toString());
                }*/

                Properties props = new Properties();
                props.put("user", ds.getUserName());
                props.put("password", ds.getPassword());
                props.put("driver", ds.getDriver());
                props.put("batchsize", "2000");
                JDBCOptions jdbc = ScalaToJavaUtil.getJDBCOptions(props, ds.getConnectUrl(), ds.getTableName());
                ScalaToJavaUtil.saveTable(df, true, jdbc);
                break;
        }

    }

    /**
     * 是否为hugetable jdbc连接
     *
     * @param driverClassName
     * @return
     */
    private boolean isHtJdbcConn(String driverClassName) {
        return StringUtils.equals(driverClassName, "com.chinamobile.cmss.ht.Driver");
    }

    /**
     * 配置sql -> dataframe -> redis
     * redis选择(key,field,value)方式存入
     *
     * @param out   配置sql
     * @param ds    redis数据源配置
     * @param cache 上下文
     */
    private void toRedis(OperateSql out, DataSourceConf ds, DataCache cache) {
        SparkSession hiveContext = cache.getSparkSession();

        log.info("GENERATE DATAFRAME'S SQL :{}", out.getSql());
        Dataset<Row> df = hiveContext.sql(out.getSql());

        if (df.schema().size() < 3) {
            throw new RuntimeException("根据配置sql计算出的redis输出格式不正确");
        }

        String[] fields = df.schema().fieldNames();

        String ip = ds.getAddress();
        String portStr = ds.getConnectUrl();
        int port = StringUtils.isEmpty(portStr) ? 6379 : Integer.parseInt(portStr);

        String password = StringUtils.isEmpty(ds.getPassword()) ? null : ds.getPassword();
        String dbStr = ds.getTableName();

        int db = StringUtils.isEmpty(dbStr) ? 0 : Integer.parseInt(dbStr);

        log.info("REDIS CONFIG :host={},port={},db={},password={}", ip, portStr, dbStr, password);

        JedisConnectionPool pool = JedisConnectionPool.getInstance();
        df.javaRDD().foreachPartition(iterRow -> {
            Jedis conn = pool.getConnection(ip, password, port, db);
            iterRow.forEachRemaining(row -> {
                saveHSetToRedis(row, conn, fields);
            });
            conn.close();
        });

    }

    /**
     * 按照HSET格式保存数据到redis
     *
     * @param row
     * @param conn
     * @param fields
     */
    private void saveHSetToRedis(Row row, Jedis conn, String[] fields) {
        String key = row.get(0).toString();
        String field = row.get(1).toString();
        String value = row.get(2).toString();

        if (null != key && !key.trim().equals("")) {
            if (null != field && !field.trim().equals("") && null != value && !value.trim().equals("")) {
                conn.hset(key, field, value);
            }

            if (fields.length > 3) {
                for (int i = 3; i < fields.length; i++) {
                    String extraField = fields[i];
                    String extraValue = row.get(i).toString();

                    if (null != extraField && !extraField.trim().equals("") && null != extraValue
                            && !extraValue.trim().equals("")) {
                        conn.hset(key, extraField.toLowerCase(), extraValue);
                    }
                }
            }

        }
    }

    /**
     * 输出至ftp server
     *
     * @param out   输出配置
     * @param ds    数据源配置
     * @param cache streaming数据业务处理上下文
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    private void toFtpServer(OperateSql out, DataSourceConf ds, DataCache cache) {
        SparkSession hiveContext = cache.getSparkSession();

        String sql = out.getSql();

        if (StringUtils.isEmpty(ds.getConnectUrl()) || StringUtils.isEmpty(ds.getTableName())) {
            log.error("文件路径或生成文件为空");
            return;
        }

        //文件名标签替换
        String ymdStr = DateUtils.getSpecFormatDate("yyyyMMdd");
        String hmsStr = DateUtils.getSpecFormatDate("HHmmss");
        String fname = ds.getTableName();
        fname = StringUtils.replace(fname, "{yyyyMMdd}", ymdStr);
        fname = StringUtils.replace(fname, "{hhmmss}", hmsStr);

        String filePath = ds.getConnectUrl() + fname;
        File file = new File(filePath);
        if (file.exists()) {
            log.error("{}-本地文件已存在", filePath);
            return;
        }

        log.info("EXTRACT-DATA-FTP-SQL:{}", sql);
        Dataset<Row> df = hiveContext.sql(sql);
        List<Row> lines = df.collectAsList();

        if (null != lines) {
            for (Row row : lines) {
                StringBuffer message = new StringBuffer();
                for (int i = 0; i < row.size(); i++) {

                    if (i != row.size() - 1) {
                        message.append(row.get(i)).append(ds.getDecollator());
                    } else {
                        message.append(row.get(i));
                    }
                }
                message.append("\r\n");
                this.write(file, message, "UTF-8");

            }

            this.upload(ds, fname);

        }

    }

    /**
     * 写文件
     *
     * @param file     文件对象
     * @param data     内容
     * @param encoding 编码
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    private void write(File file, CharSequence data, String encoding) {
        try {
            FileUtils.write(file, data, encoding, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 文件ftp上传
     *
     * @param ds    数据源配置
     * @param fname 文件名
     * @return
     * @throws Exception
     * @see [类、类#方法、类#成员]
     */
    private void upload(DataSourceConf ds, String fname) {
        String cmd = "%s %s %s %s %s %s";
        cmd = String.format(cmd,
                ds.getAddress(),
                ds.getUserName(),
                ds.getPassword(),
                ds.getConnectUrl(),
                ds.getConnectUrl(),
                fname);
        Process process = null;

        log.info("执行上传FTP命令:{}", cmd);

        try {
            process = Runtime.getRuntime().exec(cmd);

            process.waitFor();
        } catch (IOException e) {
            log.error("上传文件至FTP异常:", e);
            e.printStackTrace();
        } catch (InterruptedException e) {
            log.error("上传文件至FTP异常:", e);
            e.printStackTrace();
        }

    }

}
