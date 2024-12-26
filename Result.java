import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Result implements Writable {
    public double revenue = 0.0;
    public int quantity = 0;

    public Result() {
    }

    public Result(double revenue, int quantity) {
        this.revenue = revenue;
        this.quantity = quantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(revenue);
        out.writeInt(quantity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        revenue = in.readDouble();
        quantity = in.readInt();
    }

    @Override
    public String toString() {
        return String.format("%d\t%d", Math.round(revenue), quantity);
    }
}
