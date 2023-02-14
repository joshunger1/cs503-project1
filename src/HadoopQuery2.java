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

//overall class for the second query
public class HadoopQuery2 {

    //class for the customer dataset mapper
    public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            
            //convert data to string
            String record = value.toString();
            
            //split string into individual record columns
            String[] parts = record.split(",");
            
            //write the id to the key and the country code to the value along with a tag 
            //to determine if the value is from the customer or transaction dataset
            context.write(new Text(parts[0]), new Text("cust    " + parts[4]));
        }
    }

    public static class TransactionMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            
            //convert data to string
            String record = value.toString();
            
            //split records into columns
            String[] parts = record.split(",");
            
            //write customer id to the key and the transaction total to the value
            context.write(new Text(parts[1]), new Text("tnxn    " + parts[2]));
        }
    }

    //class for the first reducer
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //initate and grab values for output
            String custIDStr = key.toString();
            int custID = Integer.parseInt(custIDStr);
            int countryCode = 0;
            float minTransTotal = Float.MAX_VALUE;
            float maxTransTotal = Float.MIN_VALUE;

            //loop through values
            for (Text t : values) {
                String parts[] = t.toString().split("    ");
                if (parts[0].equals("tnxn")) {
                    //calculate the min and max transaction total for each customer
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
            //write the customer id, the country they are from, and their individual min and max transaction total
            String str = String.format("%d %d %f %f", custID, countryCode, minTransTotal, maxTransTotal);
            context.write(null, new Text(str));
        }
    }

    //class for the mapper for the second map in 2 M/R job
    public static class CountryMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(" ");

            //group by country code, and give it the customer id, and their min and max transactions
            context.write(new Text(parts[1]), new Text(parts[0] + "    " + parts[2] + "    " + parts[3]));
        }
    }

    //reducer class for the second reduce phase
    public static class GroupReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int numCust = 0;
            String countryCodeStr = key.toString();
            int countryCode = Integer.parseInt(countryCodeStr);
            float minTransTotal = Float.MAX_VALUE;
            float maxTransTotal = Float.MIN_VALUE;
            
            //loop through all customers for a country
            for (Text t : values) {
                //increment the customer count
                numCust++;
                String parts[] = t.toString().split("    ");

                //grab the overall min and max for that country
                float currMinTransTotal = Float.parseFloat(parts[1]);
                float currMaxTransTotal = Float.parseFloat(parts[2]);
                if(currMinTransTotal < minTransTotal){
                    minTransTotal = currMinTransTotal;
                } 
                if(currMaxTransTotal > maxTransTotal){
                    maxTransTotal = currMaxTransTotal;
                } 
            }

            //write to output the country code, the number of customers in that country, and that countrys min and max transaction total
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