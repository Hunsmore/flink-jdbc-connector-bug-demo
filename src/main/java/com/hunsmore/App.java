package com.hunsmore;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
                .inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        String sinkSql = "CREATE TABLE `example_table` (\n" +
                "  `str_col` VARCHAR(2000),\n" +
                "  `int_col` BIGINT\n" +
                ")\n" +
                "COMMENT ''\n" +
                "WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'root',\n" +
                "  'table-name' = 'example_table',\n" +
                "  'url' = 'jdbc:mysql://127.0.0.1:3306/flink_demo'\n" +
                ")";
        System.out.println(sinkSql);
        tEnv.executeSql(sinkSql);

        Table table = tEnv.sqlQuery("select count(*) from example_table");
        TableResult res = table.execute();
        res.print();
    }
}
