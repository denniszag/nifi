package org.apache.nifi.processors.network.parser.v9;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class NetflowDataRecordIPValue extends NetflowDataRecordValue {
        InetAddress ipAddress;

        public NetflowDataRecordIPValue(NetflowFieldType type, byte[] data) throws UnknownHostException {
            super(type, data);
            this.ipAddress = InetAddress.getByAddress(data);
        }

        @Override
        public String toString() {
            return ipAddress.getHostAddress();
        }
    }