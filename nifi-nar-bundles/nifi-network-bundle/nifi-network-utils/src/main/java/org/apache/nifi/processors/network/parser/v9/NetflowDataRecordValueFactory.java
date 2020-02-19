package org.apache.nifi.processors.network.parser.v9;

class NetflowDataRecordValueFactory {
        public static NetflowDataRecordValue getNetflowDataRecordValue(NetflowFieldType type, byte[] data) throws Throwable {
            if (type.toString().contains("IPV")) {
                return new NetflowDataRecordIPValue(type, data);
            }

            return new NetflowDataRecordValue(type, data);
        }
    }