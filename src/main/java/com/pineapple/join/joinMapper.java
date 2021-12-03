package com.pineapple.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;

public class joinMapper extends Mapper<LongWritable, Text, Text, joinBean> {

    private final HashMap<String, String[]> detailsMap = new HashMap<>();
    private final Text outK = new Text();
    private final joinBean outV = new joinBean();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, joinBean>.Context context)
            throws IOException {
        // 从缓存文件中读取 store_review.csv
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        // 获取文件系统对象，并打开输入流
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream inputStream = fileSystem.open(path);

        // 从流中读取数据
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] fields = line.split(",");
            // 赋值
            detailsMap.put(fields[0], Arrays.copyOfRange(fields, 1, fields.length));
        }
        // 关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, joinBean>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        String[] details = detailsMap.get(fields[1]);

        outK.set(fields[0] + "," + fields[1]);

        outV.setTransactionId(fields[0]);
        outV.setStoreId(fields[1]);
        outV.setReviewScore(Integer.parseInt(fields[2]));
        outV.setStoreName(details[0]);
        outV.setEmployeeNumber(Integer.parseInt(details[1]));
        context.write(outK, outV);
    }
}
