package com.scz.flk.source;

import com.scz.flk.model.SensorReading;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * Created by shen on 2019/12/25.
 */
public class SensorSource implements SourceFunction<SensorReading> {

    private boolean isRunning;
    private int length;
    private boolean isAscend = false;  // 乱序数据

    public SensorSource(int length) {
        if (length > 0 && length < 10000) {
            this.length = length;
        } else {
            this.length = 100;
        }
        this.isRunning = true;
    }

    public SensorSource(int length, boolean isAscend) {
        this(length);
        this.isAscend = isAscend;
    }

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random random = new Random();
        double[] curTemp = new double[5];
        for (int i = 0; i < curTemp.length; i++) {
            curTemp[i] = 60 + random.nextGaussian() * 20;
        }
        for (int cnt = 0; this.isRunning && cnt++ < this.length;) {
            long timestamp = System.currentTimeMillis() + (isAscend ? 0 : random.nextInt(1500));
            int curNum = random.nextInt(curTemp.length);
            curTemp[curNum] += random.nextGaussian();
            ctx.collect(new SensorReading("sensor_" + curNum, timestamp, curTemp[curNum]));
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
