import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// public class Q1 {

// }

public class HadoopQuery1 {
    public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text(parts[0]), new Text("cust    " + parts[1] + "    " + parts[5]));
        }
    }

    public static class TransactionMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text(parts[1]), new Text("tnxn    " + parts[2] + "    " + parts[3]));
        }
    }

    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String custIDStr = key.toString();
            int custID = Integer.parseInt(custIDStr);
            String name = "";
            float salary = 0;
            float totalSum = 0;
            int numTrans = 0;
            int minItems = Integer.MAX_VALUE;
            for (Text t : values) {
                String parts[] = t.toString().split("    ");
                if (parts[0].equals("tnxn")) {
                    numTrans++;
                    totalSum += Float.parseFloat(parts[1]);
                    int currItems = Integer.parseInt(parts[2]);
                    if (currItems < minItems) {
                        minItems = currItems;
                    }
                } else if (parts[0].equals("cust")) {
                    name = parts[1];
                    salary = Float.parseFloat(parts[2]);
                }
            }
            String str = String.format("%d %s %f %d %f %d", custID, name, salary, numTrans, totalSum, minItems);
            context.write(null, new Text(str));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Reduce-side join");
        job.setJarByClass(HadoopQuery1.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}