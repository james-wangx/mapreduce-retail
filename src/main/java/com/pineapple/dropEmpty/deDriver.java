package com.pineapple.dropEmpty;

import com.pineapple.dropDuplication.duDriver;
import com.pineapple.dropDuplication.duMapper;
import com.pineapple.dropDuplication.duReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class deDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("DropDuplication");
        job.setJarByClass(deDriver.class);

        job.setMapperClass(deMapper.class);
        job.setReducerClass(deReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path input = new Path("output/du/part-r-00000");
        Path output = new Path("output/de");
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(output))
            fileSystem.delete(output, true);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
