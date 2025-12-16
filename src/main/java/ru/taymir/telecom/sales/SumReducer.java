package ru.taymir.telecom.sales;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.taymir.telecom.revenue.QuantityWritable;

import java.io.IOException;

/**
 * Суммирует revenueCents и quantity по категории.
 */
public class SumReducer extends Reducer<Text, QuantityWritable, Text, QuantityWritable> {
    private final QuantityWritable outVal = new QuantityWritable();

    @Override
    protected void reduce(Text key, Iterable<QuantityWritable> values, Context context)
            throws IOException, InterruptedException {
        long sumRevenue = 0L;
        long sumQty = 0L;
        for (QuantityWritable v : values) {
            sumRevenue = Math.addExact(sumRevenue, v.getRevenueCents());
            sumQty = Math.addExact(sumQty, v.getQuantity());
        }
        outVal.set(sumRevenue, sumQty);
        context.write(key, outVal);
    }
}



