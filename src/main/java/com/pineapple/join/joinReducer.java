package com.pineapple.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class joinReducer extends Reducer<Text, joinBean, joinBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<joinBean> values, Reducer<Text, joinBean, joinBean, NullWritable>.Context context)
            throws IOException, InterruptedException {
        int max = 0;
        ArrayList<joinBean> beans = new ArrayList<>();

        for (joinBean bean : values) {
            joinBean tempBean = new joinBean();
            if (bean.reviewScore > max)
                max = bean.reviewScore;

            try {
                BeanUtils.copyProperties(tempBean, bean);
            } catch (InvocationTargetException | IllegalAccessException e) {
                e.printStackTrace();
            }
            beans.add(tempBean);
        }

        for (joinBean bean : beans)
            if (bean.reviewScore == max) {
                context.write(bean, NullWritable.get());
            }
    }
}
