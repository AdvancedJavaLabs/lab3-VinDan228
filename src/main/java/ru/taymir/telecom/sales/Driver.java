package ru.taymir.telecom.sales;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.taymir.telecom.jobs.Aggregate;
import ru.taymir.telecom.jobs.Sort;

import java.time.Instant;

public class Driver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exit = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(exit);
    }

    @Override
    public int run(String[] args) throws Exception {
        Args a = Args.parse(args);

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        Path input = new Path(a.input);
        Path finalOutput = new Path(a.output);

        String tmpName = a.output.replaceAll("/+$", "") + "_agg_" + Instant.now().toEpochMilli();
        Path tmpAgg = new Path(tmpName);

        if (fs.exists(tmpAgg)) {
            fs.delete(tmpAgg, true);
        }
        if (fs.exists(finalOutput)) {
            fs.delete(finalOutput, true);
        }

        Job jobAggregate = Aggregate.configure(conf, input, tmpAgg, a.reducers, a.mapThreads);
        if (!jobAggregate.waitForCompletion(true)) {
            return 1;
        }

        Job jobSort = Sort.configure(conf, tmpAgg, finalOutput);
        boolean ok = jobSort.waitForCompletion(true);

        try {
            fs.delete(tmpAgg, true);
        } catch (Exception e) {}

        if (ok) {
            return 0;
        }
        return 2;
    }

    private static final class Args {
        final String input;
        final String output;
        final int reducers;
        final int mapThreads;

        private Args(String input, String output, int reducers, int mapThreads) {
            this.input = input;
            this.output = output;
            this.reducers = reducers;
            this.mapThreads = mapThreads;
        }

        static Args parse(String[] args) {
            String input = null;
            String output = null;
            int reducers = 2;
            int mapThreads = 4;

            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                if ("--input".equals(a) && i + 1 < args.length) {
                    input = args[++i];
                } else if ("--output".equals(a) && i + 1 < args.length) {
                    output = args[++i];
                } else if ("--reducers".equals(a) && i + 1 < args.length) {
                    reducers = Integer.parseInt(args[++i]);
                } else if ("--map-threads".equals(a) && i + 1 < args.length) {
                    mapThreads = Integer.parseInt(args[++i]);
                } else if ("-h".equals(a) || "--help".equals(a)) {
                    usageAndExit(0);
                } else {
                    usageAndExit(2);
                }
            }

            if (input == null || output == null) {
                usageAndExit(2);
            }
            if (reducers < 1) {
                reducers = 1;
            }
            if (mapThreads < 1) {
                mapThreads = 1;
            }

            return new Args(input, output, reducers, mapThreads);
        }

        static void usageAndExit(int code) {
            System.err.println("Usage: hadoop jar <jar> [ru.taymir.telecom.sales.Driver] " +
                    "--input <hdfs_input_path> --output <hdfs_output_path> " +
                    "[--reducers N] [--map-threads N]");
            System.exit(code);
        }
    }
}



