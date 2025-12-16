package ru.taymir.telecom.revenue;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class QuantityWritable implements Writable {
    private long revenueCents;
    private long quantity;

    public QuantityWritable() {
    }

    public QuantityWritable(long revenueCents, long quantity) {
        this.revenueCents = revenueCents;
        this.quantity = quantity;
    }

    public long getRevenueCents() {
        return revenueCents;
    }

    public long getQuantity() {
        return quantity;
    }

    public void set(long revenueCents, long quantity) {
        this.revenueCents = revenueCents;
        this.quantity = quantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(revenueCents);
        out.writeLong(quantity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        revenueCents = in.readLong();
        quantity = in.readLong();
    }

    @Override
    public String toString() {
        return revenueCents + "\t" + quantity;
    }
}



