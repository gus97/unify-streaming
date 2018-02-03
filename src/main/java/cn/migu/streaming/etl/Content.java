package cn.migu.streaming.etl;

import cn.migu.streaming.common.ClassUtils;
import cn.migu.streaming.customparser.Parser;
import cn.migu.streaming.model.ColumnCleanConf;
import cn.migu.streaming.model.DataCache;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.stream.IntStream;

/**
 *
 * 文件行内容截取
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class Content implements Serializable
{
    /**
     * 获取实际文件行内容
     * @param input
     * @return
     */
    public static String getContent(String input)
    {
        String content = input;

        if (StringUtils.contains(input, "FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF"))
        {
            content = StringUtils.substringAfterLast(input, "FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF");
        }
        return content;
    }

    /**
     * 获取文件metadata信息
     * @param input
     * @return
     */
    public static String getFileMetaData(String input)
    {
        String content = input;

        if (StringUtils.contains(input, "FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF"))
        {
            content = StringUtils.substringBeforeLast(input, "FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF");
        }
        return content;
    }

    public static boolean isCustomFormat(int fileFormat)
    {
        return fileFormat == 11;
    }

    /**
     * 行内容分隔成列
     * @param input
     * @param cache
     * @return
     */
    public static String[] splitToColumns(String input, DataCache cache)
    {
        String separator = cache.getFileCleanConf().getSeparator();

        int fileFormat = cache.getFileCleanConf().getKind();

        int totalColNums = cache.getFileCleanConf().getColNum();

        List<ColumnCleanConf> colsCleanConf = cache.getColsCleanConf();

        String[] cols = null;

        if (isCustomFormat(fileFormat))
        {
            String className = StringUtils.join("cn.migu.streaming.customparser.", separator);

            Class<Parser> clazz = ClassUtils.loadClass(className);

            Parser parser = ClassUtils.loadInstanceOf(clazz);

            //搜集所有列名信息
            String[] colNames = IntStream.range(0, colsCleanConf.size()).filter(i -> 0 != i).mapToObj(i -> colsCleanConf.get(i).getField()).toArray(String[]::new);
            cols = parser.toColumn(input, colNames);
        }
        else
        {
            if (StringUtils.isEmpty(separator))
            {
                cols = new String[totalColNums];
                for (int i = 1; i < colsCleanConf.size(); i++)
                {
                    ColumnCleanConf ccc = colsCleanConf.get(i);
                    cols[i - 1] = StringUtils.substring(input, ccc.getStartPos() - 1, ccc.getEndPos());
                }
            }
            else
            {
                //cols = StringUtils.splitPreserveAllTokens(input, separator);
                cols = input.split(separator, -1);
            }
        }

        return cols;
    }

}
