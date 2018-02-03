package cn.migu.streaming.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.Serializable;

/**
 * jedis连接池
 *
 * author  zhaocan
 * version  [版本号, 2017/3/14]
 * see  [相关类/方法]
 * since  [产品/模块版本]
 */
public class JedisConnectionPool implements Serializable
{
    private static JedisConnectionPool instance = null;

    private JedisPoolConfigSerializable config = null;

    private JedisPool pool = null;

    private JedisConnectionPool()
    {
        config = new JedisPoolConfigSerializable();

        config.setMaxTotal(10);

        config.setMaxIdle(5);

        config.setTestOnBorrow(true);

        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override public void run()
            {
                if (null != pool)
                {
                    pool.destroy();
                }
            }
        });
    }

    public synchronized static JedisConnectionPool getInstance()
    {
        if (null == instance)
        {
            instance = new JedisConnectionPool();
        }

        return instance;
    }

    /**
     * 获取jedis连接
     * @param ip 连接地址
     * @param password 密码
     * @param port 连接端口
     * @param db 数据库index
     * @return
     */
    public Jedis getConnection(String ip, String password, int port, int db)
    {
        pool = new JedisPool(config, ip, port, 2000, password, db, null);
        return pool.getResource();
    }

}
