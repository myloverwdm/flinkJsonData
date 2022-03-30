package cn.com.lyb.array.souce;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.*;

/**
 * @author LaiYongBin
 * @date 创建于 13:02 2022/3/24
 * @apiNote DO SOMETHING
 */
public class LybArraySourceFlinkSqlFactory implements DynamicTableSourceFactory {

    /**
     * 按顺序存储DTO列表
     */
    List<LybFiledMsgDto[]> fieldList = new ArrayList<>();

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // 获取jsonArray数据
        Map<String, String> options = context.getCatalogTable().getOptions();
        JSONArray jsonArray = JSON.parseArray(options.get("data"));
        CatalogTable catalogTable = context.getCatalogTable();
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(catalogTable.getSchema());
        String[][] fieldExpressions = new String[schema.getFieldCount()][];
        for (int i = 0; i < fieldExpressions.length; i++) {
            String fieldName = schema.getFieldName(i).get();
            DataType dataType = schema.getFieldDataType(i).get();
            this.putKv2FiledMap(fieldName, dataType);
        }
        return new LybArraySourceTableSource(jsonArray, this.fieldList);
    }

    @Override
    public String factoryIdentifier() {
        // 连接器的名称
        return "lyb-array-source";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        // 必要的参数 个数
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(
                ConfigOptions.key("data")
                        .stringType().noDefaultValue()
                        .withDescription("jsonArray数据"));
        // use pre-defined option for format
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return null;
    }

    /**
     * 处理字段类型,添加进FIELD_MAP
     */
    private void putKv2FiledMap(String fieldName, DataType fieldType) {
        if (fieldType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.ROW)) {
            // ROW类型
            List<DataType> children = fieldType.getChildren();
            String[] rowFiledArray = fieldType.toString().substring(4, fieldType.toString().length() - 1).replace("`", "").split(",");
            LybFiledMsgDto[] filedMsgArray = new LybFiledMsgDto[rowFiledArray.length];
            int index = 0;
            for (String fieldSchema : rowFiledArray) {
                String[] filedArray = fieldSchema.trim().split(" ");
                String key = fieldName + "." + filedArray[0];
                filedMsgArray[index] = new LybFiledMsgDto(key, children.get(index));
                index++;
            }
            fieldList.add(filedMsgArray);
            return;
        }
        // 将类型 和 值 存储在 fieldList 里
        fieldList.add(new LybFiledMsgDto[]{new LybFiledMsgDto(fieldName, fieldType)});
    }
}
