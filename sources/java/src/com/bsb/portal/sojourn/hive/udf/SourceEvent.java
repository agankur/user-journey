package com.bsb.portal.sojourn.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;

public class SourceEvent extends UDF {
    private String preUID;
    private long preTime;
    private int preEventId;
    private int sourceId;

    public long evaluate(long currTime, String uid,int eventId)
    {
        long diffSeconds = 0;

        if(uid.equals(preUID))
        {
            long diff = currTime - preTime;
            diffSeconds = diff/1000;
            preTime = currTime;
            if (diffSeconds > 1800 )
            {
                sourceId = -1;
            }
            else{
                sourceId = preEventId;
            }
            preEventId = eventId;
        }
        else
        {
            preUID = uid;
            preTime = currTime;
            preEventId = eventId;
            sourceId = -1;
        }

        return sourceId;
    }
}

