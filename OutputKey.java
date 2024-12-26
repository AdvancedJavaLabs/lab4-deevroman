import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class OutputKey implements WritableComparable<OutputKey> {
    public Text key;
    public DoubleWritable revenue;

    public OutputKey() {
        key = new Text();
        revenue = new DoubleWritable();
    }

    public OutputKey(Text key, double revenue) {
        this.key = key;
        this.revenue = new DoubleWritable(revenue);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
        out.writeDouble(revenue.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        revenue.set(in.readDouble());
    }

    @Override
    public int compareTo(OutputKey other) {
        int res = Double.compare(revenue.get(), other.revenue.get());

        if (res == 0) {
            return key.toString().compareTo(other.key.toString());
        } else {
            return -res;
        }
    }

    @Override
    public String toString() {
        return key.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        OutputKey outputKey = (OutputKey) o;
        return Objects.equals(key, outputKey.key) && Objects.equals(revenue, outputKey.revenue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, revenue);
    }
}
    