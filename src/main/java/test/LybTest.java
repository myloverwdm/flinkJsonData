package test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author LaiYongBin
 * @date 创建于 2022/3/30 16:02
 * @apiNote DO SOMETHING
 */
public class LybTest {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        TableEnvironment tbEnv = TableEnvironment.create(conf);

        String jsonData = "[{  \"map\":{    \"flink\":123  },  \"mapinmap\":[\"AAA\",\"BBB\", \"CCC\"]}]";
//        tbEnv.executeSql(
//                "CREATE TABLE IF NOT EXISTS lyb_test_table01 (\n" +
//                        "    `map`         MAP<STRING,BIGINT>,\n" +
//                        "    mapinmap      ARRAY<STRING>" +
//                        ") " +
//                        "WITH ( 'connector' = 'lyb-array-source' ," +
//                        "'data'='" + jsonData + "' )");
        tbEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS lyb_test_table01 (\n" +
                        "    `map`         MAP<STRING,BIGINT>,\n" +
                        "    mapinmap      ARRAY<STRING>" +
                        ") " +
                        "WITH ( 'connector' = 'filesystem' ," +
                        "'path'='" + jsonData + "' )");
        tbEnv.executeSql("select * from lyb_test_table01").print();
        tbEnv.executeSql("select * from lyb_test_table01").print();
    }
}
