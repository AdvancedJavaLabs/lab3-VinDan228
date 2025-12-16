package ru.taymir.telecom.revenue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Сортирует ключи для джобы. revenue DESC, category ASC.
 */
public class CategoryKey implements WritableComparable<CategoryKey> {
    private long revenueCents;
    private final Text category = new Text();

    public CategoryKey() {}

    public CategoryKey(long revenueCents, String category) {
        this.revenueCents = revenueCents;
        this.category.set(category);
    }

    public long getRevenueCents() {
        return revenueCents;
    }

    public Text getCategory() {
        return category;
    }

    public void set(long revenueCents, String category) {
        this.revenueCents = revenueCents;
        this.category.set(category);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(revenueCents);
        category.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        revenueCents = in.readLong();
        category.readFields(in);
    }

    @Override
    public int compareTo(CategoryKey other) {
        int cmp = -1 * Long.compare(this.revenueCents, other.revenueCents);
        if (cmp != 0) {
            return cmp;
        }
        return this.category.compareTo(other.category);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(revenueCents);
        result = 31 * result + category.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CategoryKey)) {
            return false;
        }
        CategoryKey other = (CategoryKey) obj;
        return revenueCents == other.revenueCents && category.equals(other.category);
    }

    @Override
    public String toString() {
        return revenueCents + "\t" + category;
    }
}



