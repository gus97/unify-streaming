package cn.migu.streaming.model;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * 列清洗配置实体类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class ColumnCleanConf implements Serializable
{
    private boolean isClean;

    private boolean isNull;

    private boolean isDelete;

    private String enumValue;

    private String kind;

    private String maxValue;

    private String minValue;

    private Integer length;

    private String field;

    private int startPos;

    private int endPos;

    private String regex;

    public boolean isClean()
    {
        return isClean;
    }

    public void setIsClean(String clean)
    {
        if (StringUtils.equals(clean, "1"))
        {
            isClean = true;
        }
    }

    public boolean isNull()
    {
        return isNull;
    }

    public void setIsNull(String aNull)
    {
        if (StringUtils.equals(aNull, "0"))
        {
            isNull = true;
        }
    }

    public boolean isDelete()
    {
        return isDelete;
    }

    public void setIsDelete(String delete)
    {
        if (StringUtils.equals(delete, "1"))
        {
            isDelete = true;
        }
    }

    public String getEnumValue()
    {
        return enumValue;
    }

    public void setEnumValue(String enumValue)
    {
        this.enumValue = enumValue;
    }

    public String getKind()
    {
        return kind;
    }

    public void setKind(String kind)
    {
        this.kind = kind;
    }

    public String getMaxValue()
    {
        return maxValue;
    }

    public void setMaxValue(String maxValue)
    {
        this.maxValue = maxValue;
    }

    public String getMinValue()
    {
        return minValue;
    }

    public void setMinValue(String minValue)
    {
        this.minValue = minValue;
    }

    public Integer getLength()
    {
        return length;
    }

    public void setLength(Integer length)
    {
        this.length = length;
    }

    public String getField()
    {
        return field;
    }

    public void setField(String field)
    {
        this.field = field;
    }

    public int getStartPos()
    {
        return startPos;
    }

    public void setStartPos(int startPos)
    {
        this.startPos = startPos;
    }

    public int getEndPos()
    {
        return endPos;
    }

    public void setEndPos(int endPos)
    {
        this.endPos = endPos;
    }

    public String getRegex()
    {
        return regex;
    }

    public void setRegex(String regex)
    {
        this.regex = regex;
    }

    @Override public String toString()
    {
        return "ColumnCleanConf{" + "isClean=" + isClean + ", isNull=" + isNull + ", isDelete=" + isDelete
            + ", enumValue='" + enumValue + '\'' + ", kind='" + kind + '\'' + ", maxValue=" + maxValue + ", minValue="
            + minValue + ", length=" + length + ", field='" + field + '\'' + ", startPos=" + startPos + ", endPos="
            + endPos + ", regex='" + regex + '\'' + '}';
    }
}
