import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {


    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        if (args.length <= 4) {
            System.err.println("Args: <input path> <temp path> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.setLong(FileInputFormat.SPLIT_MAXSIZE, (long) Integer.parseInt(args[4]) * 1024 * 1024);

        Job job1 = Job.getInstance(conf, "group");
        job1.setNumReduceTasks(3);
        job1.setJarByClass(Main.class);
        job1.setMapperClass(GroupMapper.class);
        job1.setCombinerClass(GroupReducer.class);
        job1.setReducerClass(GroupReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Result.class);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileSystem.get(conf).delete(new Path(args[2]), true);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));


        Job job2 = Job.getInstance(conf, "sort");
        job2.setJarByClass(Main.class);
        job2.setMapperClass(SortMapper.class);
        job2.setOutputKeyClass(OutputKey.class);
        job2.setOutputValueClass(Result.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileSystem.get(conf).delete(new Path(args[3]), true);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        boolean ok = job1.waitForCompletion(true) && job2.waitForCompletion(true);

        long endTime = System.currentTimeMillis();

        System.out.printf("SplitSize: %d mb\n", (long) Integer.parseInt(args[4]));
        System.out.printf("Work time: %d ms", endTime - startTime);

        System.exit(ok ? 0 : 1);
    }
}
