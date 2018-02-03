package cn.migu.streaming.model;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * 数据行概要清洗配置
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class FileCleanConf implements Serializable
{
    private boolean isClean;

    private int kind;

    private int colNum;

    private String separator;

    private String runSql;

    private String tableName;

    private int bytesPerLine;

    private String resetTime = "00:00";

    private String refreshDate;

    private boolean isSaveAllData;

    private JavaRDD<Row> fullData;

    private String characterEncode;

    private int msgCountfromKafka;

    private int msgCountAfterClean;

    public static final String CHARACTER_UTF8 = "1";

    public static final String CHARACTER_GBK = "2";

    public static final String CHARACTER_ISO8859_1 = "3";

    public static final String CHARACTER_GB2312 = "4";

    public boolean isClean()
    {
        return isClean;
    }

    public void setClean(boolean clean)
    {
        isClean = clean;
    }

    public int getKind()
    {
        return kind;
    }

    public void setKind(int kind)
    {
        this.kind = kind;
    }

    public int getColNum()
    {
        return colNum;
    }

    public void setColNum(int colNum)
    {
        this.colNum = colNum;
    }

    public String getSeparator()
    {
        return separator;
    }

    public void setSeparator(String separator)
    {
        this.separator = separator;
    }

    public String getRunSql()
    {
        return runSql;
    }

    public void setRunSql(String runSql)
    {
        this.runSql = runSql;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public int getBytesPerLine()
    {
        return bytesPerLine;
    }

    public void setBytesPerLine(int bytesPerLine)
    {
        this.bytesPerLine = bytesPerLine;
    }

    public String getResetTime()
    {
        return resetTime;
    }

    public void setResetTime(String resetTime)
    {
        this.resetTime = resetTime;
    }

    public boolean isSaveAllData()
    {
        return isSaveAllData;
    }

    public void setSaveAllData(boolean saveAllData)
    {
        isSaveAllData = saveAllData;
    }

    public String getCharacterEncode()
    {
        return characterEncode;
    }

    public void setCharacterEncode(String characterEncode)
    {
        switch (characterEncode)
        {
            case CHARACTER_UTF8:
                this.characterEncode = "UTF-8";
                break;
            case CHARACTER_GBK:
                this.characterEncode = "GBK";
                break;
            case CHARACTER_ISO8859_1:
                this.characterEncode = "ISO-9958-1";
                break;
            case CHARACTER_GB2312:
                this.characterEncode = "GB2312";
                break;
        }
    }

    public String getRefreshDate()
    {
        return refreshDate;
    }

    public void setRefreshDate(String refreshDate)
    {
        this.refreshDate = refreshDate;
    }

    public JavaRDD<Row> getFullData()
    {
        return fullData;
    }

    public void setFullData(JavaRDD<Row> fullData)
    {
        this.fullData = fullData;
    }

    public int getMsgCountfromKafka()
    {
        return msgCountfromKafka;
    }

    public void setMsgCountfromKafka(int msgCountfromKafka)
    {
        this.msgCountfromKafka = msgCountfromKafka;
    }

    public int getMsgCountAfterClean()
    {
        return msgCountAfterClean;
    }

    public void setMsgCountAfterClean(int msgCountAfterClean)
    {
        this.msgCountAfterClean = msgCountAfterClean;
    }

    @Override public String toString()
    {
        return "FileCleanConf{" + "isClean=" + isClean + ", kind=" + kind + ", colNum=" + colNum + ", separator='"
            + separator + '\'' + ", runSql='" + runSql + '\'' + ", tableName='" + tableName + '\'' + ", bytesPerLine="
            + bytesPerLine + ", resetTime='" + resetTime + '\'' + ", refreshDate='" + refreshDate + '\''
            + ", isSaveAllData=" + isSaveAllData + ", characterEncode='" + characterEncode + '\'' + '}';
    }
}
