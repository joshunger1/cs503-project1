import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopQuery2 {
    public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text(parts[0]), new Text("cust    " + parts[4]));
        }
    }

    public static class TransactionMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text(parts[1]), new Text("tnxn    " + parts[2]));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String custIDStr = key.toString();
            int custID = Integer.parseInt(custIDStr);
            int countryCode = 0;
            float minTransTotal = Float.MAX_VALUE;
            float maxTransTotal = Float.MIN_VALUE;
            for (Text t : values) {
                String parts[] = t.toString().split("    ");
                if (parts[0].equals("tnxn")) {
                    float transTotal = Float.parseFloat(parts[1]);
                    if (transTotal < minTransTotal) {
                        minTransTotal = transTotal;
                    }
                    if (transTotal > maxTransTotal) {
                        maxTransTotal = transTotal;
                    }
                } else if (parts[0].equals("cust")) {
                    countryCode = Integer.parseInt(parts[1]);
                }
            }
            String str = String.format("%d %d %f %f", custID, countryCode, minTransTotal, maxTransTotal);
            context.write(null, new Text(str));
        }
    }

    public static class CountryMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(" ");
            context.write(new Text(parts[1]), new Text(parts[0] + "    " + parts[2] + "    " + parts[3]));
        }
    }

    public static class GroupReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int numCust = 0;
            String countryCodeStr = key.toString();
            int countryCode = Integer.parseInt(countryCodeStr);
            float minTransTotal = Float.MAX_VALUE;
            float maxTransTotal = Float.MIN_VALUE;
            for (Text t : values) {
                numCust++;
                String parts[] = t.toString().split("    ");
                float currMinTransTotal = Float.parseFloat(parts[1]);
                float currMaxTransTotal = Float.parseFloat(parts[2]);
                if(currMinTransTotal < minTransTotal){
                    minTransTotal = currMinTransTotal;
                } 
                if(currMaxTransTotal > maxTransTotal){
                    maxTransTotal = currMaxTransTotal;
                } 
            }
            String str = String.format("%d %d %f %f", countryCode, numCust, minTransTotal, maxTransTotal);
            context.write(null, new Text(str));
        }
    }

    public static void main(String[] args) throws Exception {
        // job1
        Configuration conf = new Configuration();
        Job job = new Job(conf, "join");
        job.setJarByClass(HadoopQuery2.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        job.waitForCompletion(true);

        // job2
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "group");
        job2.setJarByClass(HadoopQuery2.class);
        job2.setMapperClass(CountryMapper.class);
        job2.setReducerClass(GroupReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}