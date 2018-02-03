package cn.migu.streaming.log4j;

import org.apache.log4j.RollingFileAppender;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.UUID;

/**
 * 自定义log4j appender
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class CustomFileAppender extends RollingFileAppender
{
    @Override public void setFile(String file)
    {
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        String pid = rt.getName().replaceAll("@.*", "");
        super.setFile(file + "_" + pid + "_" + UUID.randomUUID().toString().replace("-", ""));
    }
}


