/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.network.parser;

import java.util.OptionalInt;

import static org.apache.nifi.processors.network.parser.util.ConversionUtil.*;

/**
 * Networkv5 is Cisco data export format which contains one header and one or more flow records. This Parser parses the netflowv5 format. More information: @see
 * <a href="https://www.cisco.com/c/en/us/td/docs/net_mgmt/netflow_collection_engine/3-6/user/guide/format.html">Netflowv5</a>
 */
public final class Netflowv9Parser {
    private static final int HEADER_SIZE = 24;
    private static final int RECORD_SIZE = 48;

    private static final int SHORT_TYPE = 0;
    private static final int INTEGER_TYPE = 1;
    private static final int LONG_TYPE = 2;
    private static final int IPV4_TYPE = 3;

    private static final String headerField[] = { "version", "count", "sys_uptime", "unix_secs", "unix_nsecs", "flow_sequence", "engine_type", "engine_id", "sampling_interval" };
    private static final String recordField[] = { "srcaddr", "dstaddr", "nexthop", "input", "output", "dPkts", "dOctets", "first", "last", "srcport", "dstport", "pad1", "tcp_flags", "prot", "tos",
            "src_as", "dst_as", "src_mask", "dst_mask", "pad2" };

    private final int portNumber;

    private Object headerData[];
    private Object recordData[][];

    public Netflowv9Parser(final OptionalInt portNumber) {
        this.portNumber = (portNumber.isPresent()) ? portNumber.getAsInt() : 0;
    }

    public final int parse(final byte[] buffer) throws Throwable {
        if( !isValid(buffer.length) ) {
            throw new Exception("Invalid Packet Length");
        }

        final int version = toInt(buffer, 0, 2);
        if( version != 5 ) {
            throw new Exception("Version mismatch");

        }
        final int count = toInt(buffer, 2, 2);

        headerData = new Object[headerField.length];
        headerData[0] = version;
        headerData[1] = count;
        headerData[2] = parseField(buffer, 4, 4, LONG_TYPE);
        headerData[3] = parseField(buffer, 8, 4, LONG_TYPE);
        headerData[4] = parseField(buffer, 12, 4, LONG_TYPE);
        headerData[5] = parseField(buffer, 16, 4, LONG_TYPE);
        headerData[6] = parseField(buffer, 20, 1, SHORT_TYPE);
        headerData[7] = parseField(buffer, 21, 1, SHORT_TYPE);
        headerData[8] = parseField(buffer, 22, 2, INTEGER_TYPE);

        int offset = 0;
        recordData = new Object[count][recordField.length];
        for (int counter = 0; counter < count; counter++) {
            offset = HEADER_SIZE + (counter * RECORD_SIZE);
            recordData[counter][0] = parseField(buffer, offset, 4, IPV4_TYPE);
            recordData[counter][1] = parseField(buffer, offset + 4, 4, IPV4_TYPE);
            recordData[counter][2] = parseField(buffer, offset + 8, 4, IPV4_TYPE);
            recordData[counter][3] = parseField(buffer, offset + 12, 2, INTEGER_TYPE);
            recordData[counter][4] = parseField(buffer, offset + 14, 2, INTEGER_TYPE);
            recordData[counter][5] = parseField(buffer, offset + 16, 4, LONG_TYPE);
            recordData[counter][6] = parseField(buffer, offset + 20, 4, LONG_TYPE);
            recordData[counter][7] = parseField(buffer, offset + 24, 4, LONG_TYPE);
            recordData[counter][8] = parseField(buffer, offset + 28, 4, LONG_TYPE);
            recordData[counter][9] = parseField(buffer, offset + 32, 2, INTEGER_TYPE);
            recordData[counter][10] = parseField(buffer, offset + 34, 2, INTEGER_TYPE);
            recordData[counter][11] = parseField(buffer, offset + 36, 1, SHORT_TYPE);
            recordData[counter][12] = parseField(buffer, offset + 37, 1, SHORT_TYPE);
            recordData[counter][13] = parseField(buffer, offset + 38, 1, SHORT_TYPE);
            recordData[counter][14] = parseField(buffer, offset + 39, 1, SHORT_TYPE);
            recordData[counter][15] = parseField(buffer, offset + 40, 2, INTEGER_TYPE);
            recordData[counter][16] = parseField(buffer, offset + 42, 2, INTEGER_TYPE);
            recordData[counter][17] = parseField(buffer, offset + 44, 1, SHORT_TYPE);
            recordData[counter][18] = parseField(buffer, offset + 45, 1, SHORT_TYPE);
            recordData[counter][19] = parseField(buffer, offset + 46, 2, INTEGER_TYPE);
        }
        return count;
    }

    private final Object parseField(final byte[] buffer, final int startOffset, final int length, final int type) {
        Object value = null;
        switch (type) {
            case SHORT_TYPE:
                value = toShort(buffer, startOffset, length);
                break;
            case INTEGER_TYPE:
                value = toInt(buffer, startOffset, length);
                break;
            case LONG_TYPE:
                value = toLong(buffer, startOffset, length);
                break;
            case IPV4_TYPE:
                value = toIPV4(buffer, startOffset, length);
                break;
            default:
                break;
        }
        return value;
    }

    private boolean isValid(final int length) {
        final int minPacketSize = HEADER_SIZE; // Only HEADER available
        return length >= minPacketSize && length <=  65536;
    }

    public int getPortNumber() {
        return portNumber;
    }

    public static String[] getHeaderFields() {
        return headerField;
    }

    public static String[] getRecordFields() {
        return recordField;
    }

    public Object[] getHeaderData() {
        return headerData;
    }

    public Object[][] getRecordData() {
        return recordData;
    }

    static class NetflowHeaderParser {
        private static final int HEADER_SIZE = 20;

        /**
         * The version of NetFlow records exported in this packet; for Version 9, this value is 0x0009
         */
        private short version;
        /**
         * Number of FlowSet records (both template and data) contained within this packet
         */
        private short count;
        /**
         * Time in milliseconds since this device was first booted
         */
        private int systemUptime;
        /**
         * Seconds since 0000 Coordinated Universal Time (UTC) 1970
         */
        private int unixSeconds;
        /**
         * Incremental sequence counter of all export packets sent by this export device; this value is cumulative, and it can be used to identify whether any export packets have been missed
         *
         * Note: This is a change from the NetFlow Version 5 and Version 8 headers, where this number represented "total flows."
         */
        private int sequenceNumber;
        /**
         * e Source ID field is a 32-bit value that is used to guarantee uniqueness for all flows exported from a particular device. (The Source ID field is the equivalent of the engine type and engine ID fields found in the NetFlow Version 5 and Version 8 headers). The format of this field is vendor specific. In the Cisco implementation, the first two bytes are reserved for future expansion, and will always be zero. Byte 3 provides uniqueness with respect to the routing engine on the exporting device. Byte 4 provides uniqueness with respect to the particular line card or Versatile Interface Processor on the exporting device. Collector devices should use the combination of the source IP address plus the Source ID field to associate an incoming NetFlow export packet with a unique instance of NetFlow on a particular device.
         */
        private int sourceId;

        public final int parse(final byte[] buffer) throws Throwable {
            if( !isValid(buffer.length) ) {
                throw new Exception("Invalid Header Length");
            }

            final short version = toShort(buffer, 0, 2);
            this.version = version;

            if( version != 9 ) {
                throw new Exception("Version mismatch (version: " + version + ", expected: 9)");

            }
            this.count = toShort(buffer, 2, 2);
            this.systemUptime = toInt(buffer, 4, 4);
            this.unixSeconds = toInt(buffer, 8, 4);
            this.sequenceNumber = toInt(buffer, 12, 4);
            this.sourceId = toInt(buffer, 16, 4);

            return this.count;
        }

        private boolean isValid(final int length) {
            return length == HEADER_SIZE;
        }

        public short getVersion() {
            return version;
        }

        public short getCount() {
            return count;
        }

        public int getSystemUptime() {
            return systemUptime;
        }

        public int getUnixSeconds() {
            return unixSeconds;
        }

        public int getSequenceNumber() {
            return sequenceNumber;
        }

        public int getSourceId() {
            return sourceId;
        }
    }

    static class NetflowTemplateParser {
        /**
         * The FlowSet ID is used to distinguish template records from data records.
         * A template record always has a FlowSet ID in the range of 0-255.
         * Currently, the template record that describes flow fields has a FlowSet ID of zero and the template record
         * that describes option fields (described below) has a FlowSet ID of 1. A data record always has a nonzero FlowSet ID greater than 255.
         */
        private short flowSetId;
        /**
         * Length refers to the total length of this FlowSet.
         * Because an individual template FlowSet may contain multiple template IDs (as illustrated above),
         * the length value should be used to determine the position of the next FlowSet record,
         * which could be either a template or a data FlowSet.
         *
         * Length is expressed in Type/Length/Value (TLV) format, meaning that the value includes the bytes used for
         * the FlowSet ID and the length bytes themselves, as well as the combined lengths of all template records included in this FlowSet.
         */
        private short length;

    }
}
