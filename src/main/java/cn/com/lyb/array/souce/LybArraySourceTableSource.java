package cn.com.lyb.array.souce;

import com.alibaba.fastjson.JSONArray;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;

import java.util.List;

/**
 * @author LaiYongBin
 * @date 创建于 14:18 2022/3/24
 * @apiNote DO SOMETHING
 */
public class LybArraySourceTableSource implements ScanTableSource, LookupTableSource {
    private final JSONArray jsonArray;
    private final List<LybFiledMsgDto[]> fieldList;

    LybArraySourceTableSource(JSONArray jsonArray, List<LybFiledMsgDto[]> fieldList) {
        this.jsonArray = jsonArray;
        this.fieldList = fieldList;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        return null;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        // 绑定MakeDataSourceFunction作为输入数据源, 第二个参数标志是否是有界流, 数据组件 默认有界
        return SourceFunctionProvider.of(
                new LybArraySourceFunction(this.jsonArray, this.fieldList), true
        );
    }

    @Override
    public DynamicTableSource copy() {
        return new LybArraySourceTableSource(this.jsonArray, this.fieldList);
    }

    @Override
    public String asSummaryString() {
        return "ArraySource";
    }
}
