package org.apache.nifi.processors.network.parser.v9;

import java.util.Arrays;
import java.util.HashMap;

public class NetflowDataRecord {
    private NetflowTemplateRecord template;
    /**
     * Mapping between FieldType -> data of the DataRecord.
     */
    private HashMap<NetflowFieldType, NetflowDataRecordValue> data = new HashMap<>();

    public NetflowDataRecord(NetflowTemplateRecord template) {
        this.template = template;
    }

    public final int parse(final byte[] buffer, int offset) throws Throwable {
        for (NetflowTemplateField field : template.getFields()) {
            byte[] recordValue = Arrays.copyOfRange(buffer, offset, offset + field.getLength());
            this.data.put(field.getType(), NetflowDataRecordValueFactory.getNetflowDataRecordValue(field.getType(), recordValue));
            offset += field.getLength();
        }

        return offset;
    }

    public NetflowTemplateRecord getTemplate() {
        return template;
    }

    public HashMap<NetflowFieldType, NetflowDataRecordValue> getData() {
        return data;
    }

    public NetflowDataRecordValue getValue(NetflowFieldType type) {
        return this.data.getOrDefault(type, null);
    }

    @Override
    public String toString() {
        return "NetflowDataRecord{" +
                "templateId=" + template.getTemplateId() +
                ", fields=" + data.size() +
                '}';
    }
}