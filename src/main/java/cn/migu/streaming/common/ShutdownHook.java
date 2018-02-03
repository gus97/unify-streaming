package cn.migu.streaming.common;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;

/**
 * 使用链表来管理Closeable对象
 */
public final class ShutdownHook implements Runnable
{
    private static final Logger log = LoggerFactory.getLogger("streaming");

    private final Deque<Closeable> closeAtShutdown = new LinkedList<>();

    private volatile boolean triggered;

    @Override public void run()
    {
        triggered = true;
        synchronized (closeAtShutdown)
        {
            for (Closeable c : closeAtShutdown)
            {
                closeQuietly(c);
            }
        }
    }

    /**
     * @param closeable 关闭对象
     * @return {@code true} 是否为第一个注册对象
     * @throws IllegalStateException if already shutting down
     */
    public boolean addCloseable(Closeable closeable)
    {
        Objects.requireNonNull(closeable);
        Preconditions.checkState(!triggered, "Can't add closeable %s; already shutting down", closeable);
        synchronized (closeAtShutdown)
        {
            boolean wasFirst = closeAtShutdown.isEmpty();
            log.info("--------------wasFirst="+wasFirst);
            closeAtShutdown.push(closeable);
            return wasFirst;
        }
    }

    /**
     * 关闭操作
     * @param closeable
     */
    public static void closeQuietly(Closeable closeable)
    {
        if (closeable != null)
        {
            try
            {
                log.info("--------------hhhhhhhhhhhhhhh=");
                closeable.close();
            }
            catch (IOException e)
            {

            }
        }
    }

}
