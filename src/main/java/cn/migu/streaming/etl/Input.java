package cn.migu.streaming.etl;

import cn.migu.streaming.db.SimpleDbUtil;
import cn.migu.streaming.db.SqlLog;
import cn.migu.streaming.model.*;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 预加载数据
 */
public class Input implements Serializable
{
    private static final Logger log = LoggerFactory.getLogger("streaming");

    private static Input instance;

    private Input()
    {

    }

    public static Input getInstance()
    {
        if (null == instance)
        {
            instance = new Input();
        }

        return instance;
    }

    /**
     * 预加载数据配置
     * @param config
     * @param cache
     * @return
     */
    public List<PreloadTb> preloadTbConf(Config config, DataCache cache)
    {
        JarConfParam jarConfParam = cache.getJarConfParam();

        String tbSql = config.getString("sql.preloadtbsql");

        tbSql = String.format(tbSql, jarConfParam.getAppId(), jarConfParam.getServiceId(), jarConfParam.getJarId());

        return SimpleDbUtil.getInstance().query(tbSql, PreloadTb.class);
    }

    /**
     * 启动时内存表加载
     * @param config
     * @param cache
     */
    public void load(Config config, DataCache cache)
    {
        SparkSession hiveContext = cache.getSparkSession();

        String dataSourceSql = config.getString("sql.datasourcesql");

        List<PreloadTb> tbs = cache.getPreloadTbs();

        if (null != tbs)
        {

            log.info("数据预加载-->开始");
            log.info("数据预加载配置信息:{}", tbs);
            for (PreloadTb plt : tbs)
            {
                String dsid = plt.getDataSourceId();

                String sourceSql = String.format(dataSourceSql, dsid);

                DataSourceConf ds = SimpleDbUtil.getInstance().queryOne(sourceSql, DataSourceConf.class);
                if (null == ds)
                {
                    continue;
                }

                log.info("数据源配置:{}", ds);

                SqlLogModel sqlLogModel = SqlLog.getInstance().makeSqlLog(plt, cache);
                try
                {
                    this.load(hiveContext, plt, ds);
                    SqlLog.getInstance().save(sqlLogModel, 1, 1);
                }
                catch (Exception e)
                {
                    log.error("预加载错误", e);
                    SqlLog.getInstance().save(sqlLogModel, 2, 1);
                    System.exit(1);
                }

            }
            log.info("数据预加载-->结束");
        }
    }

    /**
     * 预加载数据
     * @param hiveContext
     * @param plt
     * @param ds
     */
    public void load(SparkSession hiveContext, PreloadTb plt, DataSourceConf ds)
    {
        SourceType type = SourceType.valueOf(ds.getKind());
        Dataset<Row> df = null;
        switch (type)
        {
            case HIVE:
                df = hiveContext.sql(plt.getRunSql());
                break;
            case ORACLE:
            case MYSQL:
            case HBASE:
                Map<String, String> options = new HashMap<String, String>();
                options.put("url", ds.getConnectUrl());
                options.put("driver", ds.getDriver());
                options.put("user", ds.getUserName());
                options.put("password", ds.getPassword());
                options.put("dbtable", ds.getTableName());

                df = hiveContext.read().format("jdbc").options(options).load();
                break;
        }

        if (null != df)
        {
            df.registerTempTable(plt.getTableName());
        }
        else
        {
            log.warn("本次预加载数据集为空");
        }
    }
}
