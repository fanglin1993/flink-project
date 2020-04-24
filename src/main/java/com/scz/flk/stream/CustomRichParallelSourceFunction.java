package com.scz.flk.stream;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Created by shen on 2019/12/22.
 */
public class CustomRichParallelSourceFunction extends RichParallelSourceFunction<Long> {

    private long count = 0L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning && count < 1000) {
            try {
                ctx.collect(++count);
                Thread.sleep(500);
            } catch (Exception e) {
                // do nothing
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
