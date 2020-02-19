package org.apache.nifi.processors.network.parser.v9;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.nifi.processors.network.parser.util.ConversionUtil.toShort;

public class NetflowTemplateRecord {
    private static final int TEMPLATE_FIELDS_OFFSET = 4;
    /**
     * As a router generates different template FlowSets to match the type of NetFlow data it will be exporting, each template is given a unique ID. This uniqueness is local to the router that generated the template ID.
     * Templates that define data record formats begin numbering at 256 since 0-255 are reserved for FlowSet IDs.
     */
    private short templateId;
    /**
     * This field gives the number of fields in this template record.
     * Because a template FlowSet may contain multiple template records,
     * this field allows the parser to determine the end of the current template record and the start of the next.
     */
    private short fieldCount;
    private List<NetflowTemplateField> fields = new ArrayList<>();

    protected final int parse(final byte[] buffer, int offset) throws Throwable {
//            if( !isValid(buffer.length) ) {
//                throw new Exception("Invalid Header Length");
//            }

        this.templateId = toShort(buffer, offset, 2);
        this.fieldCount = toShort(buffer, offset + 2, 2);

        for (int i = 0; i < this.fieldCount; i++) {
            int from = offset + TEMPLATE_FIELDS_OFFSET + i * 4;
            int to = from + 4;
            this.fields.add(NetflowTemplateField.parse(Arrays.copyOfRange(buffer, from, to)));
        }

        return offset + TEMPLATE_FIELDS_OFFSET + this.fieldCount * NetflowTemplateField.TEMPLATE_FIELD_SIZE;
    }

    public short getTemplateId() {
        return templateId;
    }

    public short getFieldCount() {
        return fieldCount;
    }

    public List<NetflowTemplateField> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        return "NetflowTemplateRecord{" +
                "templateId=" + templateId +
                ", fieldCount=" + fieldCount +
                '}';
    }
}