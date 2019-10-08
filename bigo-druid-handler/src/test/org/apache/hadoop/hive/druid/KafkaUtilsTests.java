package org.apache.hadoop.hive.druid;

import com.google.gson.JsonObject;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;

/**
 * @author litao@bigo.sg
 * @date 9/30/19 10:31 AM
 */
public class KafkaUtilsTests {

    @Test
    public void test() {
        ArrayList<String> segmentsInfo = new ArrayList<>();
        JsonObject segmentInfo = new JsonObject();
        segmentInfo.addProperty("dataSource","test");
        segmentInfo.addProperty("created_date",new DateTime().toString());
        segmentInfo.addProperty("start","20190929");
        segmentInfo.addProperty("end","20190930");
        segmentInfo.addProperty("partitioned", true);
        segmentInfo.addProperty("version",12343221);
        segmentInfo.addProperty("used",true);
        segmentInfo.addProperty("payload","{}");
        segmentInfo.addProperty("size",123);

        try {
            Date date1 = new Date();
            for (int i=0; i<2000; i++) {
                segmentsInfo.add(segmentInfo.toString());
            }
            KafkaUtils.sendMessage("localhost:9092",
                    "hive_druid_handler_segment_info", segmentsInfo);
            Date date2 = new Date();
            System.out.println((date2.getTime()-date1.getTime())/1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
