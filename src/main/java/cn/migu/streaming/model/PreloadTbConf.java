package cn.migu.streaming.model;

import java.io.Serializable;

/**
 * 预加载输入数据表列配置实体类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class PreloadTbConf implements Serializable
{
    private String tableName;

    private String field;

    private int fieldType;

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public String getField()
    {
        return field;
    }

    public void setField(String field)
    {
        this.field = field;
    }

    public int getFieldType()
    {
        return fieldType;
    }

    public void setFieldType(int fieldType)
    {
        this.fieldType = fieldType;
    }
}
