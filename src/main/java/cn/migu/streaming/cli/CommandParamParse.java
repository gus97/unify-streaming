package cn.migu.streaming.cli;

import cn.migu.streaming.common.MethodCallUtils;
import cn.migu.streaming.model.JarConfParam;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * 命令行参数解析
 *
 * @author zhaocan
 * @version [版本号, 2016年9月12日]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class CommandParamParse implements Serializable {
    private static final Logger log = LoggerFactory.getLogger("streaming");

    public static JarConfParam parse(String[] args, Config config) {
        JarConfParam jarConfParam = new JarConfParam();

        //1.按业务传入参数顺序存放,如果JarConfParam类增加或删除属性，下面的数组要一起同步
        //2.请不要使用反射的方式获取JarConfParam的fields
        String[] fields =
                {"appId", "serviceId", "jarId", "sparkMaster", "hdfsPath", "jarPort", "appName", "dealUser", "totalCores",
                        "executorMemory", "maxRatePerPartition", "duration"};

        int useLen = fields.length;

        int realLen = args.length;

        int len = realLen > useLen ? useLen : realLen;

        String paramsLogStr = "";
        for (int i = 0; i < len; i++) {
            paramsLogStr = StringUtils.join(paramsLogStr, fields[i], "=", args[i], ",");
        }
        log.info("boot paramaters=>{}", paramsLogStr);

        for (int i = 0; i < len; i++) {
            if (StringUtils.isEmpty(args[i]) || StringUtils.equals(args[i], "null")) {
                continue;
            }
            MethodCallUtils.invokeSetMethod(jarConfParam, fields[i], new Object[]{args[i]});
        }

        //设置默认值
        if (StringUtils.isEmpty(jarConfParam.getSparkMaster())) {
            jarConfParam.setSparkMaster(config.getString("master"));
        }

        if (StringUtils.isEmpty(jarConfParam.getMaxRatePerPartition())) {



            jarConfParam.setMaxRatePerPartition(config.getString("max-rate-per-partition"));
        }

        if (StringUtils.isEmpty(jarConfParam.getDuration())) {
            jarConfParam.setDuration(config.getString("generation-interval-sec"));
        } else {
            //目前配置按毫秒单位,转换为单位秒
            int duration = Integer.valueOf(jarConfParam.getDuration());

            jarConfParam.setDuration(String.valueOf(duration / 1000));
        }

        if (StringUtils.isEmpty(jarConfParam.getTotalCores())) {
            jarConfParam.setTotalCores(config.getString("max-cores"));
        }

        if (StringUtils.isEmpty(jarConfParam.getExecutorMemory())) {
            jarConfParam.setExecutorMemory(config.getString("executor-memory"));
        }

        return jarConfParam;
    }

}
