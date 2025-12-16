package ru.taymir.telecom.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import ru.taymir.telecom.revenue.QuantityWritable;
import ru.taymir.telecom.sales.SalesMapper;
import ru.taymir.telecom.sales.SumReducer;

public final class Aggregate {
    private Aggregate() {
    }

    public static Job configure(
        Configuration conf,
        Path input,
        Path output,
        int reducers,
        int mapThreads
    ) throws Exception
    {
        Job job = Job.getInstance(conf, "aggregate");
        job.setJarByClass(Aggregate.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(MultithreadedMapper.class);
        MultithreadedMapper.setMapperClass(job, SalesMapper.class);
        MultithreadedMapper.setNumberOfThreads(job, mapThreads);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(QuantityWritable.class);

        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setNumReduceTasks(reducers);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(QuantityWritable.class);

        return job;
    }
}



