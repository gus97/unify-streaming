package cn.migu.streaming.common;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * 环境配置加载工具类
 *
 * @author  zhaocan
 * @version  [版本号, 2016年9月12日]
 * @see  [相关类/方法]
 * @since  [产品/模块版本]
 */
public final class ConfigUtils
{

    private static final Logger log = LoggerFactory.getLogger(ConfigUtils.class);

    private static final Pattern REDACT_PATTERN =
        Pattern.compile("(\\w*password\\w*\\s*=\\s*).+", Pattern.CASE_INSENSITIVE);

    private static final Config DEFAULT_CONFIG = ConfigFactory.load();

    private static final ConfigRenderOptions RENDER_OPTS =
        ConfigRenderOptions.defaults().setComments(false).setOriginComments(false).setFormatted(true).setJson(false);

    private ConfigUtils()
    {
    }

    /**
     * 默认文件配置(reference.conf)
     *
     * @return default configuration
     */
    public static Config getDefault()
    {
        return DEFAULT_CONFIG;
    }

    /**
     * 指定文件配置
     * @param conf 配置文件名
     * @return
     */
    public static Config getConfig(String conf)
    {
        return ConfigFactory.load(conf);
    }

    /**
     * 覆盖文件配置
     */
    public static Config overlayOn(Map<String, ?> overlay, Config underlying)
    {
        StringBuilder configFileString = new StringBuilder();
        for (Map.Entry<String, ?> entry : overlay.entrySet())
        {
            configFileString.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
        }
        String configFile = configFileString.toString();
        log.debug("Overlaid config: \n{}", configFile);
        return ConfigFactory.parseString(configFile).resolve().withFallback(underlying);
    }

    /**
     * 指定键值查询
     */
    public static String getOptionalString(Config config, String key)
    {
        return config.hasPath(key) ? config.getString(key) : null;
    }

    /**
     * 指定list值查询
     */
    public static List<String> getOptionalStringList(Config config, String key)
    {
        return config.hasPath(key) ? config.getStringList(key) : null;
    }

    /**
     * 覆盖属性设置
     *
     * @param overlay 需要覆盖的属性Map
     * @param key 键值
     * @param path 路径值
     * @throws IOException
     */
    public static void set(Map<String, Object> overlay, String key, Path path)
        throws IOException
    {
        Path finalPath =
            Files.exists(path, LinkOption.NOFOLLOW_LINKS) ? path.toRealPath(LinkOption.NOFOLLOW_LINKS) : path;
        overlay.put(key, "\"" + finalPath.toUri() + "\"");
    }

    /**
     * @param config 配置对象
     * @return  json字符串
     *
     */
    public static String serialize(Config config)
    {
        return config.root().withOnlyKey("migu").render(ConfigRenderOptions.concise());
    }

    /**
     * @param serialized json字符串
     * @return 反序列化为Config对象
     */
    public static Config deserialize(String serialized)
    {
        return ConfigFactory.parseString(serialized).resolve().withFallback(DEFAULT_CONFIG);
    }

    /**
     * @param config 配置对象
     * @return 对象字符串
     *  inherited from the local JVM environment
     */
    public static String prettyPrint(Config config)
    {
        return redact(config.root().withOnlyKey("migu").render(RENDER_OPTS));
    }

    static String redact(String s)
    {
        return REDACT_PATTERN.matcher(s).replaceAll("$1*****");
    }

    /**
     * @param keyValues key-value参数序列
     * @return 将传入的key-value参数对转换成的Properties对象
     */
    public static Properties keyValueToProperties(Object... keyValues)
    {
        Preconditions.checkArgument(keyValues.length % 2 == 0);
        Properties properties = new Properties();
        for (int i = 0; i < keyValues.length; i += 2)
        {
            properties.setProperty(keyValues[i].toString(), keyValues[i + 1].toString());
        }
        return properties;
    }

}
