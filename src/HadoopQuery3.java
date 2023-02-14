import java.io.*;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//overall class for the third query
public class HadoopQuery3 {

    //class for the map function, this uses replicated mapping, also called broadcast join
    public static class ReplicatedMap extends Mapper<Object, Text, Text, Text> {

        //create a map to store the customer info
        Map<Integer, String> customerCache = new HashMap<Integer, String>();

        //function to genereate the hashmap for the replicated map to work
        public void setup(Context context) throws IOException, InterruptedException {

            //grab the URI of the file we want to cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try {
                    String line = "";
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path getFilePath = new Path(cacheFiles[0].toString());

                    //create a reader to read in the data from the file
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

                    //loop through all the lines in the dataset
                    while ((line = reader.readLine()) != null) {
                        String[] words = line.split(",");
                        int custID = Integer.parseInt(words[0]);
                        String data = words[2] + "," + words[3];
                        
                        //place the customer data into the map
                        customerCache.put(custID, data);
                    }
                } catch (Exception e) {
                    System.out.println("Unable to read the file");
                    System.exit(1);
                }
            }

        }

        //map function for the replicated join
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            int custID = Integer.parseInt(parts[1]);
            Text joinValue = new Text();
            String groupString = "";

            //check if the cached hashmap contains the customer key
            if (customerCache.containsKey(custID)) {

                //parse info from the two datasets
                String custInfo = customerCache.get(custID);
                String[] custInfoStrings = custInfo.split(",");

                //get the info to do grouping
                int ageInt = Integer.parseInt(custInfoStrings[0]);
                int firstAgeDigit = Integer.parseInt(Integer.toString(ageInt).substring(0, 1));
                
                //edge case to include the 70 year olds in the [60-70] range, since grabbing the first digit wont work
                if(firstAgeDigit == 7){
                    firstAgeDigit = 6;
                }

                String ageDigit = String.valueOf(firstAgeDigit);
                String gender = custInfoStrings[1];

                groupString = ageDigit + "," + gender;

                String joinValueStir = parts[2];

                joinValue.set(joinValueStir);
            }

            //write the data to the reducer
            context.write(new Text(groupString), joinValue);
        }
    }

    //class for the reducer
    public static class DistributedReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int numTransactions = 0;
            float summedTransactions = 0;
            float minTransTotal = Float.MAX_VALUE;
            float maxTransTotal = Float.MIN_VALUE;

            //loop through all the values for the key group
            for (Text t : values) {
                numTransactions++;

                //grab the min and max for each age and gender group
                String transTotalStr = t.toString();
                float transTotal = Float.parseFloat(transTotalStr);
                if (transTotal < minTransTotal) {
                    minTransTotal = transTotal;
                }
                if (transTotal > maxTransTotal) {
                    maxTransTotal = transTotal;
                }

                //sum the transactions for us to calculate the average total
                summedTransactions += transTotal;
            }
            float avgTransTotal = summedTransactions / numTransactions;

            //generate a string to show what each group is
            String keyInfo = key.toString();
            String[] keyStrings = keyInfo.split(",");

            String Gender = keyStrings[1];

            String ageRange = "";
            
            if(keyStrings[0].equals("6")){
                ageRange = keyStrings[0] + "0-70"; 

            }else{
                ageRange = keyStrings[0] + "0-" + keyStrings[0] + "9"; 

            }

            //write data to output
            String str = String.format("%s %s %f %f %f", ageRange, Gender, minTransTotal, maxTransTotal, avgTransTotal);
            context.write(null, new Text(str));
        }
    }

    public static void main(String[] args) throws Exception {

        //job config
        Configuration conf = new Configuration();

        Job job = new Job(conf, "distributed cache join");

        job.setJarByClass(HadoopQuery3.class);
        job.setMapperClass(ReplicatedMap.class);
        job.setReducerClass(DistributedReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //add URI for file we want cached into a hashmap
        try {
            job.addCacheFile(new URI("hdfs://localhost:9000/user/ds503/Data/Customers.txt"));
        } catch (Exception e) {
            System.out.println("File Not Added");
            System.exit(1);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}