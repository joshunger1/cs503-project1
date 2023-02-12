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

public class HadoopQuery3 {

    public static class ReplicatedMap extends Mapper<Object, Text, Text, Text> {

        Map<Integer, String> customerCache = new HashMap<Integer, String>();

        public void setup(Context context) throws IOException, InterruptedException {

            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try {
                    String line = "";
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path getFilePath = new Path(cacheFiles[0].toString());

                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

                    while ((line = reader.readLine()) != null) {
                        String[] words = line.split(",");
                        int custID = Integer.parseInt(words[0]);
                        String data = words[2] + "," + words[3];
                        customerCache.put(custID, data);
                    }
                } catch (Exception e) {
                    System.out.println("Unable to read the file");
                    System.exit(1);
                }
            }

        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            int custID = Integer.parseInt(parts[1]);
            Text joinValue = new Text();
            String groupString = "";

            if (customerCache.containsKey(custID)) {
                String custInfo = customerCache.get(custID);
                String[] custInfoStrings = custInfo.split(",");

                int ageInt = Integer.parseInt(custInfoStrings[0]);
                int firstAgeDigit = Integer.parseInt(Integer.toString(ageInt).substring(0, 1));
                if(firstAgeDigit == 7){
                    firstAgeDigit = 6;
                }

                String ageDigit = String.valueOf(firstAgeDigit);
                String gender = custInfoStrings[1];

                groupString = ageDigit + "," + gender;

                String joinValueStir = parts[2];

                joinValue.set(joinValueStir);
            }

            context.write(new Text(groupString), joinValue);
        }
    }

    public static class DistributedReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int numTransactions = 0;
            float summedTransactions = 0;
            float minTransTotal = Float.MAX_VALUE;
            float maxTransTotal = Float.MIN_VALUE;
            for (Text t : values) {
                numTransactions++;

                String transTotalStr = t.toString();
                float transTotal = Float.parseFloat(transTotalStr);
                if (transTotal < minTransTotal) {
                    minTransTotal = transTotal;
                }
                if (transTotal > maxTransTotal) {
                    maxTransTotal = transTotal;
                }

                summedTransactions += transTotal;
            }
            float avgTransTotal = summedTransactions / numTransactions;

            String keyInfo = key.toString();
            String[] keyStrings = keyInfo.split(",");

            String Gender = keyStrings[1];

            String ageRange = "";
            
            if(keyStrings[0].equals("6")){
                ageRange = keyStrings[0] + "0-70"; 

            }else{
                ageRange = keyStrings[0] + "0-" + keyStrings[0] + "9"; 

            }

            String str = String.format("%s %s %f %f %f", ageRange, Gender, minTransTotal, maxTransTotal, avgTransTotal);
            context.write(null, new Text(str));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = new Job(conf, "distributed cache join");

        job.setJarByClass(HadoopQuery3.class);
        job.setMapperClass(ReplicatedMap.class);
        job.setReducerClass(DistributedReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        try {
            job.addCacheFile(new URI("hdfs://localhost:9000/user/ds503/Data/Customers.txt"));
        } catch (Exception e) {
            System.out.println("File Not Added");
            System.exit(1);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}