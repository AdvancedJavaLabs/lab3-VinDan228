package ru.taymir.telecom.sales;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.taymir.telecom.revenue.QuantityWritable;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Получает на вход файл со следующими полями:
 * transaction_id, product_id, category, price, quantity
 */
public class SalesMapper extends Mapper<LongWritable, Text, Text, QuantityWritable> {
    private final Text outKey = new Text();
    private final QuantityWritable outVal = new QuantityWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        if (line.startsWith("transaction_id,")) {
            return;
        }

        String[] parts = line.split(",", -1);
        if (parts.length < 5) {
            return;
        }

        String category = parts[2].trim();
        if (category.isEmpty()) {
            return;
        }

        String priceStr = parts[3].trim();
        String qtyStr = parts[4].trim();
        if (priceStr.isEmpty() || qtyStr.isEmpty()) {
            return;
        }

        long quantity;
        try {
            quantity = Long.parseLong(qtyStr);
        } catch (NumberFormatException e) {
            return;
        }
        if (quantity <= 0) {
            return;
        }

        long priceCents;
        try {
            BigDecimal price = new BigDecimal(priceStr);
            priceCents = price
                    .movePointRight(2)
                    .setScale(0, RoundingMode.HALF_UP)
                    .longValueExact();
        } catch (Exception e) {
            return;
        }

        long revenueCents = Math.multiplyExact(priceCents, quantity);

        outKey.set(category);
        outVal.set(revenueCents, quantity);
        context.write(outKey, outVal);
    }
}



