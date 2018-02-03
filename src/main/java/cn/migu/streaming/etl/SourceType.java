package cn.migu.streaming.etl;

import java.io.Serializable;

/**
 * 数据源类型
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public enum SourceType implements Serializable
{
    KAFKA(1),HIVE(2),ORACLE(3),REDIS(4),MYSQL(5),FTP(6),HBASE(7);

    private int value = 0;

    private SourceType(int value)
    {
        this.value = value;
    }

    public static SourceType valueOf(int value)
    {
        switch(value)
        {
            case 1:
                return KAFKA;
            case 2:
                return HIVE;
            case 3:
                return ORACLE;
            case 4:
                return REDIS;
            case 5:
                return MYSQL;
            case 6:
                return FTP;
            case 7:
                return HBASE;
            default:
                return null;
        }
    }
}
