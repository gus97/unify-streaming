package cn.migu.streaming.etl;

import java.io.Serializable;

/**
 * 输出sql运行模式
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public enum RunMode implements Serializable
{
    DATAFRAME_TO_DB(1),JDBC(2),DIRECT_HIVE(3);

    private int value = 0;

    private RunMode(int value)
    {
        this.value = value;
    }

    public static RunMode valueOf(int value)
    {
        switch(value)
        {
            case 1:
                return DATAFRAME_TO_DB;
            case 2:
                return JDBC;
            case 3:
                return DIRECT_HIVE;
            default:
                return null;
        }

    }
}
