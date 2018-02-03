package cn.migu.streaming.customparser;

import java.io.Serializable;

/**
 *
 *
 * author  zhaocan
 * version  [版本号, 2017/3/13]
 * see  [相关类/方法]
 * since  [产品/模块版本]
 */
public interface Parser extends Serializable
{
    /**
     * 转换成列数组
     * @param message
     * @param columnNames
     * @return
     */
    String[] toColumn(String message, String[] columnNames);
}
