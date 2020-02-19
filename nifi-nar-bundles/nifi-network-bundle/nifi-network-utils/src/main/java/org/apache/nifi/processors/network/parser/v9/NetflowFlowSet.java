package org.apache.nifi.processors.network.parser.v9;

import static org.apache.nifi.processors.network.parser.util.ConversionUtil.toShort;

abstract class NetflowFlowSet {
    protected static final int FLOWSET_HEADER_SIZE = 4;
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

    protected int parse(final byte[] buffer) throws Throwable {
        this.flowSetId = toShort(buffer, 0, 2);
        this.length = toShort(buffer, 2, 2);

        return this.length;
    }

    public short getFlowSetId() {
        return flowSetId;
    }

    public short getLength() {
        return length;
    }
}