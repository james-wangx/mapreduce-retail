package com.pineapple.dropEmpty;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class deMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (isNotEmpty(line)) {
            context.write(value, NullWritable.get());
        }
    }

    public boolean isNotEmpty(String line) {
        String[] fields = line.split(",", -1);
        for (String field : fields)
            if (field.trim().isEmpty())
                return false;
        return true;
    }
}
