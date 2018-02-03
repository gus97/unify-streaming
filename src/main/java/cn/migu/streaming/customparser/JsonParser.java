package cn.migu.streaming.customparser;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 *
 *
 * author  zhaocan
 * version  [版本号, 2017/3/13]
 * see  [相关类/方法]
 * since  [产品/模块版本]
 */
public class JsonParser implements Parser
{

    /*private JsonParser instance = null;

    private JsonParser()
    {

    }

    public JsonParser getInstance()
    {
        if(null == instance)
        {
            instance = new JsonParser();
        }
        return instance;
    }*/

    public String[] toColumn(String message, String[] columnNames)
    {
        String payload = message;
        if (null == payload || payload.trim().equals(""))
        {
            return new String[0];
        }

        JSONObject obj = JSONObject.parseObject(payload);

        String colVal = null;
        String[] realCols = new String[columnNames.length];
        for (int i = 0; i < realCols.length; i++)
        {
            colVal = obj.getString(columnNames[i]);
            if (null != colVal && !colVal.trim().equals(""))
            {
                String trimColVal = colVal.trim();
                if (trimColVal.startsWith("[") && trimColVal.endsWith("]"))
                {
                    JSONArray jsonArr = JSONArray.parseArray(trimColVal);
                    if (null != jsonArr)
                    {
                        StringBuilder strBuff = new StringBuilder();
                        for (int j = 0; j < jsonArr.size(); j++)
                        {
                            strBuff.append(jsonArr.get(j)).append(",");
                        }

                        colVal = strBuff.substring(0, strBuff.length() - 1);
                    }

                }
                realCols[i] = colVal;
            }
        }

        return realCols;
    }

    public static void main(String[] args)
    {
        //String testMessage = "{\"result\":[{\"payload\":\"{\\\"batchNo\\\":\\\"fasdfsadfadsf\\\",\\\"deviceId\\\":\\\"asfasdfsdf\\\",\\\"gcid\\\":\\\"1000000000333333\\\",\\\"operateType\\\":\\\"2\\\",\\\"phone\\\":\\\"133333333333\\\"}\",\"outCode\":\"payload\"}]}";
        //String[] names=  {"batchNo","deviceId","gcid","operateType","phone"};

        /*String testMessage =
            "{\"result\":[{\"payload\":\"{\\\"deviceId\\\":\\\"fasfasfasd\\\",\\\"labelList\\\":[\\\"98989439\\\",\\\"98989434\\\",\\\"98989436\\\"],\\\"phone\\\":\\\"13333333333\\\"}\",\"outCode\":\"payload\"}]}";
        String[] names = {"deviceId", "labelList", "phone"};

        String className = StringUtils.join("cn.migu.streaming.customparser.", "JsonParser");

        Class<Parser> clazz = ClassUtils.loadClass(className);

        Parser parser = ClassUtils.loadInstanceOf(clazz);

        String[] values = parser.toColumn(testMessage, names);
        for (String val : values)
        {
            System.out.print(val + " ");
        }
        System.out.println();*/
        List<String> testList = new ArrayList<String>();
        testList.add("23");
        testList.add("ab");
        testList.add("354");

        String[] colNames = IntStream.range(0, testList.size())
            .filter(i -> 0 != i)
            .mapToObj(i -> testList.get(i))
            .toArray(String[]::new);

        for (String col : colNames)
        {
            System.out.print(col + " ");
        }
        System.out.println();

    }
}
