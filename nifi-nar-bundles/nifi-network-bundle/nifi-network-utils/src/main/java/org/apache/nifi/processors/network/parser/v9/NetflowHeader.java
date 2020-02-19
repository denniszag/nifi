package org.apache.nifi.processors.network.parser.v9;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.network.parser.util.ConversionUtil.toInt;
import static org.apache.nifi.processors.network.parser.util.ConversionUtil.toShort;

public class NetflowHeader {
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

        return HEADER_SIZE;
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

    public Map<String, String> getKeyValueAttributes() {
        return new HashMap<>() {{
            put("version", String.valueOf(version));
            put("count", String.valueOf(count));
            put("systemUptime", String.valueOf(systemUptime));
            put("unixSeconds", String.valueOf(unixSeconds));
            put("sequenceNumber", String.valueOf(sequenceNumber));
            put("sourceId", String.valueOf(sourceId));

        }};
    }

    @Override
    public String toString() {
        return "NetflowV9Header{" +
                "version=" + version +
                ", count=" + count +
                ", systemUptime=" + systemUptime +
                ", unixSeconds=" + new Date((long) unixSeconds * 1000) +
                ", sequenceNumber=" + sequenceNumber +
                ", sourceId=" + sourceId +
                '}';
    }
}