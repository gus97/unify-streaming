package cn.migu.streaming;

import cn.migu.streaming.cli.CommandParamParse;
import cn.migu.streaming.common.ConfigUtils;
import cn.migu.streaming.db.DataLoad;
import cn.migu.streaming.db.SimpleDbUtil;
import cn.migu.streaming.model.*;
import cn.migu.streaming.spark.StreamingComputeLayer;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Main {
    private static final Logger log = LoggerFactory.getLogger("streaming");


    /**
     *
     * @param args
     * "appId"
     * "serviceId"
     * "jarId"
     * "sparkMaster"
     * "hdfsPath"
     * "jarPort"
     * "appName"
     * "dealUser"
     * "totalCores"
     * "executorMemory"
     * "maxRatePerPartition"
     * "duration"
     */
    public static void main(String[] args) {
        Config conf = ConfigUtils.getDefault();

        try {
            log.info("配置信息初始化-->开始");
            JarConfParam jarConf = CommandParamParse.parse(args, conf);
            log.info("jar paramater config:{}", jarConf);

            KafkaConfigParam kafkaConf = DataLoad.loadKafkaConf(conf, jarConf);
            log.info("kafka paramater config:{}", kafkaConf);

            FileCleanConf fileCleanConf = DataLoad.loadFileCleanConf(conf, jarConf);
            log.info("file clean paramater config:{}", fileCleanConf);

            List<ColumnCleanConf> colsConf = DataLoad.loadColumnCleanConf(conf, jarConf);
            log.info("file column clean paramater config:{}", colsConf);

            List<OperateSql> sqls = DataLoad.loadOperateSqlConf(conf, jarConf, 5);
            log.info("output sql config:{}", sqls);
            log.info("配置信息初始化-->结束");

            DataCache cache = new DataCache(kafkaConf, fileCleanConf, colsConf, sqls);
            cache.setJarConfParam(jarConf);

            StreamingComputeLayer<?, ?, ?> speedLayer = new StreamingComputeLayer<>(conf, cache);
            //HadoopUtils.closeAtShutdown(speedLayer);
            speedLayer.start();
            speedLayer.await();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("系统异常:", e);
        }

    }

}
