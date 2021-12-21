package com.hrbb.compute;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName EventTimesDuration
 * @Description TODO
 * @Author zby
 * @Date 2021-11-24 14:01
 * @Version 1.0
 **/
public class EventTimesDuration {
    public static void main(String[] args) throws Exception {

        if (args == null || args.length < 1) {

            throw new IllegalArgumentException("缺少<sql-file>");
        }

        //获取sql-file的全路径名
        String fileName = args[0];

        //解析内容，获取sql语句
        List<String> sqlLines = Files.readAllLines(Paths.get(fileName));

        List<String> sqls = parse(sqlLines);

        //提交sql job
        EventTimesDuration.run(sqls);

    }

    /**
     * 解析sql文件
     * @param sqlLines
     * @return
     */
    private static List<String> parse(List<String> sqlLines) {

        List<String> sqlList = new ArrayList<>();

        StringBuilder stmt = new StringBuilder();

        for (String line : sqlLines) {
            if (line.trim().isEmpty() || line.trim().startsWith("#"))
            {
                // 过滤掉空行以及注释行
                continue;
            }

            stmt.append("\n").append(line.trim());

            if (line.trim().endsWith(";")) {

                sqlList.add(stmt.substring(0, stmt.length() - 1).trim());

                //清空，复用
                stmt.setLength(0);
            }
        }

        return sqlList;
    }

    /**
     * 提交sql
     * @param sqls
     * @throws Exception
     */
    private static void run(List<String> sqls) throws Exception {

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        TableConfig tableConfig = bsTableEnv.getConfig();
        //设置状态的保留时间
        tableConfig.setIdleStateRetentionTime(Time.hours(1), Time.hours(2));

        //sql文本中只有create、insert语句
        for (String sql : sqls)
        {
            bsTableEnv.executeSql(sql);
        }

    }
}
