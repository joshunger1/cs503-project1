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

//overall class for the first query
public class HadoopQuery1 {

    //class for the customer datasets mapper
    public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            
            //convert data to string
            String record = value.toString();

            //split string into individual record columns
            String[] parts = record.split(",");

            //write the id to the key and the name and salary to the value along with a tag 
            //to determine if the value is from the customer or transaction dataset
            context.write(new Text(parts[0]), new Text("cust    " + parts[1] + "    " + parts[5]));
        }
    }

    //class for the transaction datasets mapper
    public static class TransactionMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            
            //convert data to string
            String record = value.toString();

            //split string into individual record columns
            String[] parts = record.split(",");

            //write the customer id to the key and the transaction total and number of items to the value
            //along with a tag to determine if the value is from the customer or transaction dataset
            context.write(new Text(parts[1]), new Text("tnxn    " + parts[2] + "    " + parts[3]));
        }
    }

    //class for the reducer for both datasets
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            //convert the key to a string and parse it to a integer
            String custIDStr = key.toString();
            int custID = Integer.parseInt(custIDStr);
            
            //instantiate variables to store the data for the output string
            String name = "";
            float salary = 0;
            float totalSum = 0;
            int numTrans = 0;
            int minItems = Integer.MAX_VALUE;
           
            //loop through all values for the specific key
            for (Text t : values) {
                //split it into parts
                String parts[] = t.toString().split("    ");
                
                //check if the value came from the transaction or customer dataset
                if (parts[0].equals("tnxn")) {
                    //increment transaction number counter
                    numTrans++;
                    //increment the total sum for that specific customer
                    totalSum += Float.parseFloat(parts[1]);
                    int currItems = Integer.parseInt(parts[2]);
                    //check if the minItems is less than the current min
                    if (currItems < minItems) {
                        minItems = currItems;
                    }
                } else if (parts[0].equals("cust")) {
                    //grab customers name
                    name = parts[1];
                    //grab their salary
                    salary = Float.parseFloat(parts[2]);
                }
            }
            //write to output
            String str = String.format("%d %s %f %d %f %d", custID, name, salary, numTrans, totalSum, minItems);
            context.write(null, new Text(str));
        }
    }

    public static void main(String[] args) throws Exception {

        //set up job config
        Configuration conf = new Configuration();
        Job job = new Job(conf, "join");
        job.setJarByClass(HadoopQuery1.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //allow for multi inputs
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
        Path outputPath = new Path(args[2]);

        //set output path
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        
        //begin job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}