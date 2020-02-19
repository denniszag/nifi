package org.apache.nifi.processors.network.parser.v9;

import java.util.HashMap;

class NetflowTemplateFlowSet extends NetflowFlowSet {
        private HashMap<Short, NetflowTemplateRecord> templates = new HashMap<>();

        public NetflowTemplateFlowSet() {
        }

        public int parse(final byte[] buffer) throws Throwable {
            super.parse(buffer);

            if( !isValid(this.getFlowSetId()) ) {
                throw new Exception("Invalid FlowSet ID (Template FlowSet ID should be in the range 0-255)");
            }

            int offset = FLOWSET_HEADER_SIZE;
            while(offset <= getLength()) {
                NetflowTemplateRecord templateRecord = new NetflowTemplateRecord();
                int templateLength = templateRecord.parse(buffer, offset);
                templates.put(templateRecord.getTemplateId(), templateRecord);
                offset += templateLength;
            }

            return this.getLength();
        }

        private boolean isValid(final int flowSetId) {
            return flowSetId >= 0 && flowSetId <= 255;
        }

        public HashMap<Short, NetflowTemplateRecord> getTemplates() {
            return templates;
        }
    }