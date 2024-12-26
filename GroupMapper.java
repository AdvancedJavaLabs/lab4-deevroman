import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupMapper extends Mapper<LongWritable, Text, Text, Result> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().startsWith("transaction_id,")) {
            return;
        }

        String[] fields = value.toString().split(",");

        String category = fields[2];
        double price = Double.parseDouble(fields[3].replace(",", "."));
        int quantity = Integer.parseInt(fields[4]);


        Text outKey = new Text(category);
        Result outValue = new Result(price * quantity, quantity);
        context.write(outKey, outValue);
    }
}
