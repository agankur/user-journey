package com.bsb.portal.sojourn.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.commons.codec.binary.Base64;


public class EncryptMSISDN extends UDF {

    public String evaluate(String valueToEnc) throws Exception {
            if (null != valueToEnc && "-" != valueToEnc)
            {
            	StringBuilder modifiedMSISDN = new StringBuilder("");
            	modifiedMSISDN = modifiedMSISDN.append(valueToEnc.substring(0, valueToEnc.length()/2))
            			         .append("9").append(valueToEnc.substring(valueToEnc.length()/2,valueToEnc.length()));
                byte[] encryptedValue = Base64.encodeBase64(modifiedMSISDN.toString().getBytes());
            	String result = new String(encryptedValue,"UTF-8");
                return result;
            }
            return valueToEnc;
        }
}
