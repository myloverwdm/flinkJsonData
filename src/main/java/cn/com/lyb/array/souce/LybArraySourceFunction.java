package cn.com.lyb.array.souce;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author LaiYOngBin
 * @date 创建于 14:21 2022/3/24
 * @apiNote DO SOMETHING
 */
public class LybArraySourceFunction extends RichParallelSourceFunction<RowData> {
    private final JSONArray jsonArray;
    private final List<LybFiledMsgDto[]> fieldList;

    private static final AtomicLong COUNT = new AtomicLong(0);

    LybArraySourceFunction(JSONArray jsonArray, List<LybFiledMsgDto[]> fieldList) {
        this.jsonArray = jsonArray;
        this.fieldList = fieldList;
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) {
        synchronized (LybArraySourceFunction.class) {
            if (COUNT.getAndIncrement() % this.getRuntimeContext().getNumberOfParallelSubtasks() != 0) {
                return;
            }
        }
        this.jsonArray.forEach(js -> {
            JSONObject jsonObject = (JSONObject) js;
            GenericRowData contextRow = new GenericRowData(this.fieldList.size());
            for (int i = 0; i < this.fieldList.size(); i++) {
                LybFiledMsgDto[] field = fieldList.get(i);
                LogicalType logicalType = field[0].getFieldType().getLogicalType();
                if (field.length > 1) {
                    // 这一条是Row类型
                    String key = field[0].getFieldName().split("\\.")[0];
                    JSONObject json = jsonObject.getJSONObject(key);
                    contextRow.setField(i, getRowTypeRow(json, field));
                    continue;
                } else if (LogicalTypeRoot.MAP.equals(logicalType.getTypeRoot())) {
                    String key = field[0].getFieldName();
                    JSONObject json = jsonObject.getJSONObject(key);
                    contextRow.setField(i, getMapRow(field[0].getFieldType(), json));
                    continue;
                } else if (LogicalTypeRoot.ARRAY.equals(logicalType.getTypeRoot())) {
                    String key = field[0].getFieldName();
                    JSONArray jsArray = jsonObject.getJSONArray(key);
                    contextRow.setField(i, getArrayRow(field[0].getFieldType(), jsArray));
                    continue;
                }
                // 普通类型
                String value = jsonObject.getString(field[0].getFieldName());
                contextRow.setField(i, stringValueToType(value, logicalType.getTypeRoot()));
            }
            sourceContext.collect(contextRow);
        });
    }

    @Override
    public void cancel() {
        // do Nothing
    }

    /**
     * 构建map类型的返回值
     */
    private GenericMapData getMapRow(DataType dataType, JSONObject json) {
        Map<Object, Object> resultMap = new HashMap<>(1);
        KeyValueDataType keyValueDataType = (KeyValueDataType) dataType;
        json.forEach((key, value) -> {
            Object mapKey = stringValueToType(key, keyValueDataType.getKeyDataType().getLogicalType().getTypeRoot());
            Object mapValue;
            // 对Value的类型进行解析
            DataType valueType = keyValueDataType.getValueDataType();
            mapValue = this.getNextData(valueType, valueType.getLogicalType().getTypeRoot(), String.valueOf(value));
            resultMap.put(mapKey, mapValue);
        });
        return new GenericMapData(resultMap);
    }

    /**
     * 构建Array类型的返回值
     */
    private GenericArrayData getArrayRow(DataType dataType, JSONArray jsArray) {
        CollectionDataType valueType = (CollectionDataType) dataType;
        Object[] arrayElements = new Object[jsArray.size()];
        for (int i = 0; i < jsArray.size(); i++) {
            String value = String.valueOf(jsArray.get(i));
            arrayElements[i] = this.getNextData(valueType, valueType.getElementDataType().getLogicalType().getTypeRoot(), value);
        }
        return new GenericArrayData(arrayElements);
    }


    /**
     * ROW、Array以及其它结构化类型中，构建一条数据
     *
     * @param valueDataType 结构中值的类型 如Array<STRING> 则为STRING;
     * @param dataType      本身结构的类型
     * @param value         要构建的字符串数据
     */
    private Object getNextData(DataType valueDataType, LogicalTypeRoot dataType, String value) {
        switch (dataType) {
            case MAP:
                JSONObject jsonObject = JSON.parseObject(String.valueOf(value));
                return getMapRow(valueDataType, jsonObject);
            case ARRAY:
                JSONArray array = JSON.parseArray(value);
                return getArrayRow(valueDataType, array);
            default:
                return stringValueToType(String.valueOf(value), dataType);
        }
    }

    private GenericRowData getRowTypeRow(JSONObject json, LybFiledMsgDto[] filedMsgDto) {
        GenericRowData row = new GenericRowData(json.size());
        for (int i = 0; i < filedMsgDto.length; i++) {
            String value = String.valueOf(json.get(filedMsgDto[i].getFieldName().split("\\.")[1]));
            LogicalType resType = filedMsgDto[i].getFieldType().getLogicalType();
            row.setField(i, stringValueToType(value, resType.getTypeRoot()));
        }
        return row;
    }

    /**
     * 将字符串类型的造数结果转换为对应的普通类型
     */
    Object stringValueToType(String value, LogicalTypeRoot logicalTypeRoot) {
        switch (logicalTypeRoot) {
            case CHAR:
            case VARCHAR:
                return StringData.fromString(value);
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case DECIMAL:
                BigDecimal bd = new BigDecimal(value);
                return DecimalData.fromBigDecimal(bd, bd.precision(), bd.scale());
            case TINYINT:
                return Byte.parseByte(value);
            case SMALLINT:
                return Short.parseShort(value);
            case INTEGER:
                return Integer.parseInt(value);
            case BIGINT:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case DATE:
                try {
                    return (int) (Long.parseLong(value) / (86400 * 1000));
                } catch (NumberFormatException ignored) {
                }
                String timeFormat = getTimeFormat(value);
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
                    return (int) (sdf.parse(value).getTime() / (86400 * 1000));
                } catch (ParseException e) {
                    throw new RuntimeException("字符串" + value + "无法使用`" + timeFormat + "`格式化解析为时间类型");
                }
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                try {
                    return Long.parseLong(value);
                } catch (NumberFormatException ignored) {
                }
                String format = getTimeFormat(value);
                try {
                    SimpleDateFormat sdfTimeStamp = new SimpleDateFormat(format);
                    return TimestampData.fromTimestamp(new Timestamp(sdfTimeStamp.parse(value).getTime()));
                } catch (ParseException e) {
                    throw new RuntimeException("字符串" + value + "无法使用`" + format + "`格式化解析为时间类型");
                }

            default:
                throw new RuntimeException("错误的数据类型: " + logicalTypeRoot.name());
        }
    }

    /**
     * 构建时间的format格式化字符串
     */
    private String getTimeFormat(String timeValue) {
        char[] timeValueCharArray = timeValue.toCharArray();
        // yyyy-MM-dd HH:mm:ss
        int index = 0;
        int y = 0;
        while (y < 4 && index < timeValueCharArray.length) {
            char numChar = timeValueCharArray[index];
            if (numChar >= '0' && numChar <= '9') {
                timeValueCharArray[index] = 'y';
                y++;
            }
            index++;
        }

        int M = 0;
        while (M < 2 && index < timeValueCharArray.length) {
            char numChar = timeValueCharArray[index];
            if (numChar >= '0' && numChar <= '9') {
                timeValueCharArray[index] = 'M';
                M++;
            }
            index++;
        }

        int d = 0;
        while (d < 2 && index < timeValueCharArray.length) {
            char numChar = timeValueCharArray[index];
            if (numChar >= '0' && numChar <= '9') {
                timeValueCharArray[index] = 'd';
                d++;
            }
            index++;
        }

        int H = 0;
        while (H < 2 && index < timeValueCharArray.length) {
            char numChar = timeValueCharArray[index];
            if (numChar >= '0' && numChar <= '9') {
                timeValueCharArray[index] = 'H';
                H++;
            }
            index++;
        }

        int m = 0;
        while (m < 2 && index < timeValueCharArray.length) {
            char numChar = timeValueCharArray[index];
            if (numChar >= '0' && numChar <= '9') {
                timeValueCharArray[index] = 'm';
                m++;
            }
            index++;
        }

        int s = 0;
        while (s < 2 && index < timeValueCharArray.length) {
            char numChar = timeValueCharArray[index];
            if (numChar >= '0' && numChar <= '9') {
                timeValueCharArray[index] = 's';
                s++;
            }
            index++;
        }
        return new String(timeValueCharArray);
    }
}
