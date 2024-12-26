import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupReducer extends Reducer<Text, Result, Text, Result> {
    @Override
    protected void reduce(Text k, Iterable<Result> values, Context context)
            throws IOException, InterruptedException {
        Result result = new Result();

        for (Result v : values) {
            result.revenue += v.revenue;
            result.quantity += v.quantity;
        }

        context.write(k, result);
    }
}
