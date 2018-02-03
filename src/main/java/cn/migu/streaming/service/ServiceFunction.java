/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package cn.migu.streaming.service;

import cn.migu.streaming.common.ClassUtils;
import cn.migu.streaming.etl.Output;
import cn.migu.streaming.etl.SourceType;
import cn.migu.streaming.model.DataCache;
import cn.migu.streaming.model.DataSourceConf;
import cn.migu.streaming.model.FileCleanConf;
import cn.migu.streaming.timer.TimerRefresh;
import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * streaming业务处理入口类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public final class ServiceFunction<K, M, U> implements VoidFunction<JavaPairRDD<K, M>> {

    private static final Logger log = LoggerFactory.getLogger("streaming");

    private DataCache cache;

    private Config config;

    public ServiceFunction(Config config, DataCache cache) {
        this.config = config;

        this.cache = cache;
    }

    /**
     * 业务处理流程
     *
     * @param newData 数据rdd实例
     * @return null
     */
    @Override
    public void call(JavaPairRDD<K, M> newData)
            throws Exception {
        //重置更新内存表
        this.refreshDfTb();

        if (newData.isEmpty()) {
            this.cache.getFileCleanConf().setMsgCountfromKafka(0);
            this.cache.getFileCleanConf().setMsgCountAfterClean(0);
            log.info("本批次接收数据集为空");
        } else {
            try {
                //初始化时间字符串
                /*DateFormatStr dfs = DateFormatStr.instance();
                dfs.init();
                this.cache.setDateFormatStr(dfs);*/

                String implClassName = config.getString("impl-class");
                AbstractServiceManager manager = ClassUtils.loadInstanceOf(implClassName,
                        AbstractServiceManager.class,
                        new Class<?>[]{DataCache.class},
                        new Object[]{this.cache});

                log.info("数据文件清洗-->开始");
                JavaRDD<U> rdd = manager.handler(newData);
                log.info("数据文件清洗-->结束");





                log.info("加载清洗后数据到内存表-->开始");
                boolean hasDataSet = manager.loadDf(rdd);
                log.info("加载清洗后数据到内存表-->结束");

                if (hasDataSet) {
                    log.info("数据格式化输出-->开始");
                    Output out = Output.getInstance();
                    out.execute(this.config, this.cache);
                    log.info("数据格式化输出-->结束");
                }
            } catch (Exception e) {
                log.error("异常^^:", e);
                System.exit(1);
            }

            log.info("---------当前批次数据完成-------------");
        }

    }

    /**
     * 清空unbound table内容
     *
     * @return null
     */
    private void refreshDfTb() {
        FileCleanConf fcc = this.cache.getFileCleanConf();

        TimerRefresh.getInstance().refresh(fcc);
    }

}
