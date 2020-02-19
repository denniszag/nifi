package org.apache.nifi.processors.network.parser.v9;

import static org.apache.nifi.processors.network.parser.util.ConversionUtil.toInt;

class NetflowTemplateField {
    public static final int TEMPLATE_FIELD_SIZE = 4;
    /**
     * This numeric value represents the type of the field. The possible values of the field type are vendor specific. Cisco supplied values are consistent across all platforms that support NetFlow Version 9.
     *
     * At the time of the initial release of the NetFlow Version 9 code (and after any subsequent changes that could add new field-type definitions), Cisco provides a file that defines the known field types and their lengths.
     *
     * The currently defined field types are detailed in Table 6.
     */
    private NetflowFieldType type;

    /**
     * This number gives the length of the above-defined field, in bytes.
     */
    private int length;

    private NetflowTemplateField(NetflowFieldType type, int length) {
        this.type = type;
        this.length = length;
    }

    public static NetflowTemplateField parse(final byte[] buffer) throws Throwable {
        if( !isValid(buffer.length) ) {
            throw new Exception("Invalid Field Length");
        }
        final int typeValue = toInt(buffer, 0, 2);
        final int length = toInt(buffer, 2, 2);

        NetflowFieldType type = NetflowFieldType.getByValue(typeValue);
        if (type == null) {
            type = NetflowFieldType.UNKNOWN_FIELD_TYPE;
        }

        return new NetflowTemplateField(type, length);
    }

    private static boolean isValid(final int length) {
        return length == TEMPLATE_FIELD_SIZE;
    }

    public NetflowFieldType getType() {
        return type;
    }

    public int getLength() {
        return length;
    }
}