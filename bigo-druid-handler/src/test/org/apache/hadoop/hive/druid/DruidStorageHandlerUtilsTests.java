package org.apache.hadoop.hive.druid;

import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tangyun@bigo.sg
 * @date 5/31/19 2:31 PM
 */
public class DruidStorageHandlerUtilsTests {

    @Test
    public void getIntervalsToOverWriteTest() {

        List<DataSegment> dataSegments = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; ++i) {
            DataSegment dataSegment = new DataSegment(
                    "ds",
                    new Interval(start + i * 100000, start + (i + 1) *100000),
                    "111111",
                    null,
                    null,
                    null,
                    null,
                    null,
                    10
            );
            dataSegments.add(dataSegment);
        }

        List<Interval> intervals =
                DruidStorageHandlerUtils.getIntervalsToOverWrite(dataSegments);
        for (Interval interval: intervals) {
            System.out.println(interval.getStart().toDateTime());
            System.out.println(interval.getEnd().toDateTime());
        }
    }
}
