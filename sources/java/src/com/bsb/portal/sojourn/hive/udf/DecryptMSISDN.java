package com.bsb.portal.sojourn.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.commons.codec.binary.Base64;


public class DecryptMSISDN extends UDF {

	public String evaluate(String valueToDecrypt) throws Exception{
		
		if ((null == valueToDecrypt) || ("-".equals(valueToDecrypt))) {
			return "-";
		}
		else{
			byte[] result = Base64.decodeBase64(valueToDecrypt.getBytes());
			String res = new String(result,"UTF-8");
			StringBuilder msisdn = new StringBuilder ("");
			if (res.length()%2 != 0)
				msisdn = msisdn.append(res.substring(0,res.length()/2)).append(res.substring(res.length()/2+1,res.length()));
			else
				msisdn = msisdn.append(res.substring(0,res.length()/2-1)).append(res.substring(res.length()/2,res.length()));
			return msisdn.toString();
		}
	}
}
