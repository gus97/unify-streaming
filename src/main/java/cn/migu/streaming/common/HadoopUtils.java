package cn.migu.streaming.common;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * Hadoop相关方法
 */
public final class HadoopUtils
{

    private static final Logger log = LoggerFactory.getLogger("streaming");

    private static final ShutdownHook SHUTDOWN_HOOK = new ShutdownHook();

    private HadoopUtils()
    {
    }

    /**
     * 增加shutdown回调,尝试调用{@link Closeable#close()}.
     * 与Hadoop的 {@link ShutdownHookManager} 整合在一起
     * 是为了更好的与spark进行交互
     *
     * @param closeable Closeable对象
     */
    public static void closeAtShutdown(Closeable closeable)
    {
        if (SHUTDOWN_HOOK.addCloseable(closeable))
        {
            try
            {
                // Spark使用SHUTDOWN_HOOK_PRIORITY + 30; 优先级高的先调用
                ShutdownHookManager.get().addShutdownHook(SHUTDOWN_HOOK, FileSystem.SHUTDOWN_HOOK_PRIORITY + 40);
            }
            catch (IllegalStateException ise)
            {
                log.warn("Can't close {} at shutdown since shutdown is in progress", closeable);
            }
        }
    }

}
