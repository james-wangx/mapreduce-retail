package com.pineapple.join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class joinReducer extends Reducer<Text, joinBean, joinBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<joinBean> values, Reducer<Text, joinBean, joinBean, NullWritable>.Context context)
            throws IOException, InterruptedException {
        int max = 0;
        joinBean tempBean = new joinBean();

        for (joinBean bean : values)
            if (bean.reviewScore > max) {
                max = bean.reviewScore;
                tempBean = bean;
            }
        context.write(tempBean, NullWritable.get());
    }
}
