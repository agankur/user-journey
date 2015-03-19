package com.bsb.portal.sojourn.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;

public class SessionNum extends UDF {
    private String preUID;
    private long preTime;
    private int sessionNum;

    public long evaluate(long currTime, String uid)
    {
        long diffSeconds = 0;

        if(uid.equals(preUID))
        {
            long diff = currTime - preTime;
            diffSeconds = diff/1000;
            preTime = currTime;
            if (diffSeconds > 1800 )
            {
                sessionNum++;
            }
        }
        else
        {
            preUID = uid;
            preTime = currTime;
            sessionNum = 1;
        }

        return sessionNum;
    }
}

