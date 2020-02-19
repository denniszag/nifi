package org.apache.nifi.processors.network.parser.v9;

import java.util.Arrays;

import static org.apache.nifi.processors.network.parser.util.ConversionUtil.toInt;

public class NetflowDataRecordValue {
        private byte[] data;

        public NetflowDataRecordValue(NetflowFieldType type, byte[] data) {
            this.data = data;
        }

        @Override
        public String toString() {
            return "{data=" + Arrays.toString(data) + ", int=" + toInt(data, 0, data.length) + "}";
        }
    }