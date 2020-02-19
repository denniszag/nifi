package org.apache.nifi.processors.network.parser.v9;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

class NetflowDataFlowSet extends NetflowFlowSet {
    private List<NetflowDataRecord> dataRecords = new ArrayList<>();
    private HashMap<Short, NetflowTemplateRecord> templates;

    NetflowDataFlowSet(HashMap<Short, NetflowTemplateRecord> templates) {
        this.templates = templates;
    }

    protected int parse(final byte[] buffer) throws Throwable {
        super.parse(buffer);

        if( !isValid(this.getFlowSetId()) ) {
            throw new UnknownTemplateException("Invalid FlowSet ID (Data FlowSet ID should reference to an existing Template ID)");
        }

        NetflowTemplateRecord template = templates.get(this.getFlowSetId()); // current template

        int offset = FLOWSET_HEADER_SIZE;
        // As the field lengths are variable V9 has padding to next 32 Bit
        int paddingSize = 4 - (getLength() % 4);  // 4 Byte

        while(offset <= (getLength() - paddingSize)) {
            NetflowDataRecord dataRecord = new NetflowDataRecord(template);
            int recordLength = dataRecord.parse(buffer, offset);
            dataRecords.add(dataRecord);
            offset += recordLength;
        }
        return this.getLength();
    }

    private boolean isValid(final short flowSetId) {
        return templates.containsKey(flowSetId);
    }

    public List<NetflowDataRecord> getDataRecords() {
        return dataRecords;
    }
}