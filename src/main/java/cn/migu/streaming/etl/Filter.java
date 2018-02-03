package cn.migu.streaming.etl;

import cn.migu.streaming.common.DateUtils;
import cn.migu.streaming.db.CleanFailLog;
import cn.migu.streaming.model.CleanFailRecord;
import cn.migu.streaming.model.ColumnCleanConf;
import cn.migu.streaming.model.DataCache;
import cn.migu.streaming.model.FileCleanConf;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 *
 * 文件内容过滤清洗
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class Filter implements Serializable
{

    private static final Logger log = LoggerFactory.getLogger("streaming");

    /**
     * 文件行清洗
     * @param msg
     * @param dataCache
     * @return
     */
    public static boolean jarFilter(String msg, DataCache dataCache, String[] failLog)
    {
        FileCleanConf fileCleanConf = dataCache.getFileCleanConf();
        if (!fileCleanConf.isClean())
        {
            return true;
        }

        if (Content.isCustomFormat(fileCleanConf.getKind()))
        {
            return true;
        }

        if (StringUtils.isEmpty(fileCleanConf.getSeparator()))
        {
            if (msg.length() >= fileCleanConf.getBytesPerLine())
            {
                return true;
            }
            else
            {
                failLog[0] = String.format("文件行字节数[%d]少于配置清洗长度[%d]", msg.length(), fileCleanConf.getBytesPerLine());
                log.error(failLog[0]);
                return false;
            }
        }
        else
        {
            //String[] cols = StringUtils.splitPreserveAllTokens(msg, fileCleanConf.getSeparator());
            String[] cols = msg.split(fileCleanConf.getSeparator(), -1);
            if (cols.length >= fileCleanConf.getColNum())
            {
                return true;
            }
            else
            {
                failLog[0] = String.format("文件行列数[%d]少于配置清洗列数[%d]", cols.length, fileCleanConf.getColNum());
                log.error(failLog[0]);
                return false;
            }
        }

    }

    /**
     * 列清洗
     * @param msg
     * @param dataCache
     * @return
     */
    public static boolean columnFilter(String msg, DataCache dataCache, String[] failLog)
    {
        String[] cols = Content.splitToColumns(msg, dataCache);

        List<ColumnCleanConf> colsCleanConf = dataCache.getColsCleanConf();

        ColumnCleanConf colConf = null;

        //log.info("数据分隔列数:{},业务配置列数:{}", cols.length, colsCleanConf.size());

        int colLen = (colsCleanConf.size() - 1) <= cols.length ? (colsCleanConf.size() - 1) : cols.length;

        /*for (int i = 0; i < colLen; i++)
        {
            log.error(String.format("第%d列内容为%s", (i + 1),cols[i]));
        }*/

        for (int i = 0; i < colLen; i++)
        {
            String str = StringUtils.trim(cols[i]);

            colConf = colsCleanConf.get(i + 1);

            String fieldType = colConf.getKind();

            //列不需要清洗，继续下一列的清洗动作
            if (!colConf.isClean())
            {
                continue;
            }

            //列非空
            if (!colConf.isNull() && StringUtils.isEmpty(cols[i]))
            {
                failLog[0] = String.format("第%d列不应为空,但实际为空", (i + 1));
                log.error(failLog[0]);
                return false;
            }

            //枚举
            String enumValue = colConf.getEnumValue();
            if (StringUtils.isNotEmpty(enumValue))
            {
                String[] values = StringUtils.split(enumValue, ",");
                boolean contains = Arrays.stream(values).anyMatch(x -> StringUtils.equals(x, str));
                if (!contains)
                {
                    failLog[0] = String.format("第%d列枚举值不正确,%s", (i + 1), str);
                    log.error(failLog[0]);
                    return false;
                }
            }

            //正则表达式
            String regx = colConf.getRegex();
            if (!StringUtils.equals(fieldType, "date"))
            {
                if (StringUtils.isNotEmpty(regx))
                {
                    boolean match = Pattern.matches(regx, str);
                    if (!match)
                    {
                        failLog[0] = String.format("第%d列正则表达式匹配不正确,%s", (i + 1), str);
                        log.error(failLog[0]);
                        return false;
                    }
                }
            }

            switch (fieldType)
            {
                case "string":
                    //当列长度被设置时,需要校验列长度
                    if (null != colConf.getLength() && cols[i].length() != colConf.getLength())
                    {
                        failLog[0] =
                            String.format("第%d列string类型的长度不正确:[%d,%d]", (i + 1), cols[i].length(), colConf.getLength());
                        log.error(failLog[0]);
                        return false;
                    }
                    break;
                case "int":
                case "float":
                case "double":
                    if (NumberUtils.isNumber(str))
                    {
                        double n = Double.valueOf(str);
                        double min = Double.valueOf(colConf.getMinValue());
                        double max = Double.valueOf(colConf.getMaxValue());

                        if (!(n >= min && n <= max))
                        {
                            failLog[0] = String.format("第%d列数字范围[%d,%d]不正确:%s", (i + 1), min, max, str);
                            log.error(failLog[0]);
                            return false;
                        }
                    }
                    else
                    {
                        failLog[0] = String.format("第%d列非数字类型:%s", (i + 1), str);
                        log.error(failLog[0]);
                        return false;
                    }
                    break;
                case "date":
                    boolean dateColFilterRst =
                        dateColumnFilter(regx, str, i, colConf.getMinValue(), colConf.getMaxValue(), failLog);
                    if (!dateColFilterRst)
                    {
                        return false;
                    }
                    break;
            }

        }

        return true;
    }

    /**
     * 文件内容清洗操作
     * @param s
     * @param cache
     * @return
     */
    public static boolean filter(String s, DataCache cache)
    {
        String line = Content.getContent(s);

        //错误信息记录
        String[] failLog = new String[1];

        //行清洗
        boolean flag = Filter.jarFilter(line, cache, failLog);

        if (!flag)
        {
            log.error("错误行信息:{}", s);
            //addCleanFailLog(s, cache, failLog[0]);
            return false;
        }

        //列清洗
        boolean flag1 = Filter.columnFilter(line, cache, failLog);

        if (!flag1)
        {
            log.error("错误行信息:{}", s);
            //addCleanFailLog(s, cache, failLog[0]);
            return false;
        }

        return true;
    }

    /**
     * 清洗错误记录入库
     * @param s
     * @param cache
     * @param failMsg
     */
    private static void addCleanFailLog(String s, DataCache cache, String failMsg)
    {
        CleanFailRecord failRecord = new CleanFailRecord();
        failRecord.setServerId(cache.getJarConfParam().getServiceId());
        failRecord.setJarId(cache.getJarConfParam().getJarId());
        failRecord.setFailKind(2);
        failRecord.setFailRecord(StringUtils.substringAfter(s, " "));
        failRecord.setFailCode(failMsg);
        String metaData = Content.getFileMetaData(s);
        //String lineNumInfo = StringUtils.split(metaData, ",")[0];
        failRecord.setSourceId(StringUtils.substringAfterLast(metaData, ","));

        //lineNumInfo = lineNumInfo.replaceAll("\\D+", "");

        failRecord.setDealTime(new Date());

        CleanFailLog.getInstance().insert(failRecord);
    }

    /**
     * 日志类型列过滤
     * @param regx
     * @param str
     * @param i
     * @param max
     * @param min
     * @return
     */
    private static boolean dateColumnFilter(final String regx, final String str, final int i, final String max,
        final String min, String[] failLog)
    {
        if (StringUtils.isNotEmpty(regx))
        {
            if (!StringUtils.equals(regx, "时间戳"/*bull shit*/))
            {
                boolean vflag = DateUtils.validateDateFormat(str, regx);
                if (!vflag)
                {
                    failLog[0] = String.format("第%d列不符合日期格式[%s]:%s", (i + 1), regx, str);
                    log.error(failLog[0]);
                    return false;
                }

                //日期有效值范围校验
                String minDateStr = min;
                String maxDateStr = max;

                if (StringUtils.isNotEmpty(minDateStr))
                {
                    minDateStr = minDateStr.replaceAll("\\s+", "");

                    String beforeN = StringUtils.substringAfter(minDateStr, "sysdate-");
                    if (StringUtils.isEmpty(beforeN))
                    {
                        beforeN = StringUtils.substringAfter(minDateStr, "sysdate+");
                    }
                    if (NumberUtils.isNumber(beforeN))
                    {
                        String bDateStr = DateUtils.getBeforeNDay(Integer.valueOf(beforeN), regx);
                        if (DateUtils.compareDate(str, bDateStr, regx) < 0)
                        {
                            failLog[0] = String.format("第%d列不符合日期最小值[%s]:%s,%s", (i + 1), minDateStr, str, regx);
                            log.error(failLog[0]);
                            return false;
                        }
                    }
                }

                if (StringUtils.isNotEmpty(maxDateStr))
                {
                    maxDateStr = maxDateStr.replaceAll("\\s+", "");

                    String afterN = StringUtils.substringAfter(maxDateStr, "sysdate+");
                    if (StringUtils.isEmpty(afterN))
                    {
                        afterN = StringUtils.substringAfter(maxDateStr, "sysdate-");
                    }
                    if (NumberUtils.isNumber(afterN))
                    {
                        String bDateStr = DateUtils.getAfterNDay(Integer.valueOf(afterN), regx);
                        if (DateUtils.compareDate(str, bDateStr, regx) > 0)
                        {
                            failLog[0] = String.format("第%d列不符合日期最大值[%s]:%s,%s", (i + 1), maxDateStr, str, regx);
                            log.error(failLog[0]);
                            return false;
                        }
                    }
                }
            }
        }

        return true;
    }

}
