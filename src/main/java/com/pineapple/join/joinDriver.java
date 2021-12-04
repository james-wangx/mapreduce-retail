package com.pineapple.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class joinDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("Join");
        job.setJarByClass(joinMapper.class);

        job.setMapperClass(joinMapper.class);
        job.setReducerClass(joinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(joinBean.class);

        job.setOutputKeyClass(joinBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 加载缓存数据
        job.addCacheFile(new URI("input/store_details.csv"));

        Path input = new Path("output/de/part-r-00000");
        Path output = new Path("output/join");
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(output))
            fileSystem.delete(output, true);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
