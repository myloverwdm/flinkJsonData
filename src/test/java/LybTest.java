import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

/**
 * @author LaiYongBin
 * @date 创建于 2022/3/30 15:47
 * @apiNote DO SOMETHING
 */
public class LybTest {
    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);
        Configuration configuration = parameter.getConfiguration();
        EnvironmentSettings settings;
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        settings = EnvironmentSettings.fromConfiguration(configuration);
        TableEnvironmentImpl tbEnv = TableEnvironmentImpl.create(settings);
        String jsonData = "[{  \"map\":{    \"flink\":123  },  \"mapinmap\":[\"AAA\",\"BBB\", \"CCC\"]}]";
        tbEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS lyb_test_table01 (\n" +
                        "    `map`         MAP<STRING,BIGINT>,\n" +
                        "    mapinmap      ARRAY<STRING>" +
                        ") " +
                        "WITH ( 'connector' = 'lyb-array-source' ," +
                        "'data'='" + jsonData + "' )");
        tbEnv.executeSql("select * from lyb_test_table01").print();
        tbEnv.executeSql("select * from lyb_test_table01").print();
    }
}
