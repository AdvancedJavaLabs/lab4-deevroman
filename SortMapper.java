import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, OutputKey, Result> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        OutputKey outKey = new OutputKey(new Text(fields[0]), Double.parseDouble(fields[1].replace(",", ".")));
        Result outValue = new Result(Double.parseDouble(fields[1].replace(",", ".")), Integer.parseInt(fields[2]));
        context.write(outKey, outValue);
    }
}
