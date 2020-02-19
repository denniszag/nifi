package org.apache.nifi.processors.network.parser.v9;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.nifi.processors.network.parser.util.ConversionUtil.toShort;

public class NetflowV9ExportPacket {
        private NetflowHeader header;
        private HashMap<Short, NetflowTemplateRecord> templates;
        private List<NetflowDataRecord> dataRecords;

        private NetflowV9ExportPacket(byte[] buffer) {
            this.templates = new HashMap<>();
            this.dataRecords = new ArrayList<>();
        }

        public static NetflowV9ExportPacket parse(byte[] buffer) throws Throwable {
            NetflowV9ExportPacket exportPacket = new NetflowV9ExportPacket(buffer);

            int offset = 0;
            exportPacket.header = new NetflowHeader();
            offset += exportPacket.header.parse(Arrays.copyOfRange(buffer, 0, 20));

            while(offset < buffer.length) {
                short flowSetId = toShort(buffer, offset, 2);

                // If it's a template
                if (flowSetId >= 0 && flowSetId <= 255) {
                    NetflowTemplateFlowSet templateFlowSet = new NetflowTemplateFlowSet();
                    offset += templateFlowSet.parse(Arrays.copyOfRange(buffer, offset, buffer.length));

                    // Add the template to cache
                    templateFlowSet.getTemplates().forEach((templateId, template) -> {
                        //String uniqueTemplateId = exportPacket.header.getSourceId() + "" + templateId; // template id can be the same across different devices
                        short uniqueTemplateId = templateId;  // TODO: Support unique template id
                        if (!exportPacket.templates.containsKey(uniqueTemplateId))
                            exportPacket.templates.put(uniqueTemplateId, template);
                    });
                } else { // Otherwise, it's a Data FlowSet
                    NetflowDataFlowSet dataFlowSet = new NetflowDataFlowSet(exportPacket.templates);
                    offset += dataFlowSet.parse(Arrays.copyOfRange(buffer, offset, buffer.length));

                    // Add the data
                    exportPacket.dataRecords.addAll(dataFlowSet.getDataRecords());
                }
            }

            return exportPacket;
        }

        public NetflowHeader getHeader() {
            return header;
        }

        public HashMap<Short, NetflowTemplateRecord> getTemplates() {
            return templates;
        }

        public List<NetflowDataRecord> getDataRecords() {
            return dataRecords;
        }

        @Override
        public String toString() {
            return "NetflowV9ExportPacket{" +
                    "version=" + header.getVersion() +
                    ", templates=" + templates.size() +
                    ", dataRecords=" + dataRecords.size() +
                    '}';
        }
    }