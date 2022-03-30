package cn.com.lyb.array.souce;

import org.apache.flink.table.types.DataType;

import java.io.Serializable;

/**
 * @author LaiYongBin
 * @date 创建于 16:38 2022/3/24
 * @apiNote DO SOMETHING
 */
public class LybFiledMsgDto implements Serializable {
    /**
     * 字段名
     */
    private String fieldName;
    /**
     * 字段类型
     */
    private DataType fieldType;

    public LybFiledMsgDto(String fieldName, DataType fieldType) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public DataType getFieldType() {
        return fieldType;
    }

    public void setFieldType(DataType fieldType) {
        this.fieldType = fieldType;
    }
}
