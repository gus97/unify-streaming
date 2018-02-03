package cn.migu.streaming.db;

import cn.migu.streaming.common.DateUtils;
import cn.migu.streaming.model.*;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * 数据预加载操作
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class DataLoad implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(DataLoad.class);

    /**
     * 加载kafka配置数据
     *
     * @param config
     * @param jarParam
     * @return
     */
    public static KafkaConfigParam loadKafkaConf(final Config config, final JarConfParam jarParam) {
        String inputSql = config.getString("sql.kafkadatasql");

        inputSql = String.format(inputSql, jarParam.getAppId(), jarParam.getServiceId(), jarParam.getJarId());

        return SimpleDbUtil.getInstance().queryOne(inputSql, KafkaConfigParam.class);
    }

    /**
     * 加载文件清洗配置
     *
     * @param config
     * @param jarParam
     * @return
     */
    public static FileCleanConf loadFileCleanConf(final Config config, final JarConfParam jarParam) {
        String jarCleanSql = config.getString("sql.jarcleansql");

        String fileCleanSql = config.getString("sql.filecleansql");

        jarCleanSql = String.format(jarCleanSql, jarParam.getJarId());

        fileCleanSql = String.format(fileCleanSql, jarParam.getAppId(), jarParam.getServiceId(), jarParam.getJarId());

        JarCleanConf jarCleanConf = SimpleDbUtil.getInstance().queryOne(jarCleanSql, JarCleanConf.class);

        FileCleanConf fileCleanConf = SimpleDbUtil.getInstance().queryOne(fileCleanSql, FileCleanConf.class);

        if (StringUtils.equals(jarCleanConf.getIsclean(), "1")) {
            fileCleanConf.setClean(true);
        }

        if (StringUtils.equals(jarCleanConf.getIssavealldata(), "5")) {
            fileCleanConf.setSaveAllData(true);
        }

        fileCleanConf.setRunSql(jarCleanConf.getRunsql());

        //初始化更新日期
        fileCleanConf.setRefreshDate(DateUtils.getSpecFormatDate("yyyy-MM-dd"));

        return fileCleanConf;
    }

    /**
     * 加载列清洗配置
     *
     * @param config
     * @param jarParam
     * @return
     */
    public static List<ColumnCleanConf> loadColumnCleanConf(final Config config, final JarConfParam jarParam) {
        String columnCleanSql = config.getString("sql.colcleansql");

        columnCleanSql =
                String.format(columnCleanSql, jarParam.getAppId(), jarParam.getServiceId(), jarParam.getJarId());

        return SimpleDbUtil.getInstance().query(columnCleanSql, ColumnCleanConf.class);
    }

    /**
     * 加载业务sql
     *
     * @param config
     * @param jarParam
     * @param type
     * @return
     */
    public static List<OperateSql> loadOperateSqlConf(final Config config, final JarConfParam jarParam, int type) {
        String serviceSql = config.getString("sql.servicesql");

        serviceSql = String.format(serviceSql, jarParam.getAppId(), jarParam.getServiceId(), jarParam.getJarId(), type);

        return SimpleDbUtil.getInstance().query(serviceSql, OperateSql.class);
    }
}
