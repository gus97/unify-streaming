package cn.migu.streaming.db;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Option;

import java.sql.Types;

/**
 *
 * oracle方言配置
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class OracleDialect
{
    public static void init()
    {
        JdbcDialect oracleDialect = new JdbcDialect()
        {
            @Override public boolean canHandle(String url)
            {
                return url.startsWith("jdbc:oracle") || url.contains("oracle");
            }

            @Override public Option<JdbcType> getJDBCType(DataType dt)
            {
                if (DataTypes.StringType.sameType(dt))
                {
                    return Option.apply(new JdbcType("VARCHAR2(255)", java.sql.Types.VARCHAR));
                }
                else if (DataTypes.BooleanType.sameType(dt))
                {
                    return Option.apply(new JdbcType("NUMBER(1)", java.sql.Types.NUMERIC));
                }
                else if (DataTypes.IntegerType.sameType(dt))
                {
                    return Option.apply(new JdbcType("NUMBER(10)", java.sql.Types.NUMERIC));
                }
                else if (DataTypes.LongType.sameType(dt))
                {
                    return Option.apply(new JdbcType("NUMBER(19)", java.sql.Types.NUMERIC));
                }
                else if (DataTypes.DoubleType.sameType(dt))
                {
                    return Option.apply(new JdbcType("NUMBER(19,4)", java.sql.Types.NUMERIC));
                }
                else if (DataTypes.FloatType.sameType(dt))
                {
                    return Option.apply(new JdbcType("NUMBER(19,4)", java.sql.Types.NUMERIC));
                }
                else if (DataTypes.ShortType.sameType(dt))
                {
                    return Option.apply(new JdbcType("NUMBER(5)", java.sql.Types.NUMERIC));
                }
                else if (DataTypes.ByteType.sameType(dt))
                {
                    return Option.apply(new JdbcType("NUMBER(3)", java.sql.Types.NUMERIC));
                }
                else if (DataTypes.BinaryType.sameType(dt))
                {
                    return Option.apply(new JdbcType("BLOB", java.sql.Types.BLOB));
                }
                else if (DataTypes.TimestampType.sameType(dt))
                {
                    return Option.apply(new JdbcType("DATE", Types.DATE));
                }
                else if (DataTypes.DateType.sameType(dt))
                {
                    return Option.apply(new JdbcType("DATE", Types.DATE));
                }
                else if (DataTypes.createDecimalType().sameType(dt))
                { //unlimited
                    //      return  DecimalType.Fixed(precision, scale) => Some(JdbcType("NUMBER(" + precision + "," + scale + ")", java.sql.Types.NUMERIC))
                    return Option.apply(new JdbcType("NUMBER(38,4)", Types.NUMERIC));
                }
                return Option.empty();
            }
        };

        JdbcDialects.registerDialect(oracleDialect);
    }
}
