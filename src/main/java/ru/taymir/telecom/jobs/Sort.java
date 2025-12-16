package ru.taymir.telecom.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import ru.taymir.telecom.revenue.CategoryKey;
import ru.taymir.telecom.revenue.QuantityWritable;

import java.io.IOException;

public final class Sort {
    private Sort() {
    }

    public static class SortMapper extends Mapper<Text, QuantityWritable, CategoryKey, QuantityWritable> {
        private final CategoryKey outKey = new CategoryKey();
        private final QuantityWritable outVal = new QuantityWritable();

        @Override
        protected void map(Text category, QuantityWritable agg, Context context)
                throws IOException, InterruptedException {
            outKey.set(agg.getRevenueCents(), category.toString());
            outVal.set(agg.getRevenueCents(), agg.getQuantity());
            context.write(outKey, outVal);
        }
    }

    public static class SortReducer extends Reducer<CategoryKey, QuantityWritable, Text, NullWritable> {
        private static String formatCents(long cents) {
            boolean neg = cents < 0;
            long abs = Math.abs(cents);
            long major = abs / 100;
            long minor = abs % 100;
            String s = major + "." + (minor < 10 ? "0" + minor : minor);
            if (neg) {
                return "-" + s;
            }
            return s;
        }

        private final Text outLine = new Text();

        @Override
        protected void reduce(CategoryKey key, Iterable<QuantityWritable> values, Context context)
                throws IOException, InterruptedException {
            long revenueCents = 0L;
            long quantity = 0L;
            for (QuantityWritable v : values) {
                revenueCents = v.getRevenueCents();
                quantity = v.getQuantity();
                break;
            }

            String category = key.getCategory().toString();
            outLine.set(category + "\t" + formatCents(revenueCents) + "\t" + quantity);
            context.write(outLine, NullWritable.get());
        }
    }

    public static Job configure(Configuration conf, Path input, Path output) throws Exception {
        Job job = Job.getInstance(conf, "sales-sort");
        job.setJarByClass(Sort.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(CategoryKey.class);
        job.setMapOutputValueClass(QuantityWritable.class);

        job.setNumReduceTasks(1);
        job.setReducerClass(SortReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job;
    }
}



