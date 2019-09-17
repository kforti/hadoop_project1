package jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Job5 {

    //Job1 {

    //Mapper
    public static class IDmapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // transid, custid, trans_total, trans_num_items, trans_desc
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = value.toString();
            String[] entries = line.split(",");
            String data = "";
            String custid = "";
            String age = "";
            String gender = "";
            String transVal = "";

            if (fileName.contains("transactions.txt")) {
                custid = entries[1];
                transVal = entries[2];
                data = transVal; // sumtotal;

            }
            else if (fileName.contains("customers.txt")) {
                custid = entries[0];
                age = entries[2];
                gender = entries[3];
                data = age + ";" + gender;//age + "," + gender;
            }

            context.write(new Text(custid), new Text(data));
        }
    }

    //----------------------------------------------------------------------------

    //Reducer
    public static class TransactionsReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String age = "";
            String gender = "";
            float trans_num = 0;

            for (Text value:values) {
                String v = value.toString();
                if (v.contains(";")) {
                    String[] data = v.split(";");
                    age = data[0];
                    gender = data[1];
                }
                else {
                    trans_num += Float.parseFloat(v);
                }
            }
            //entry = age  + "," + gender + ","  + Float.toString(trans_num) ;
            context.write(key, new Text(age  + "," + gender + ","  + Float.toString(trans_num)));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job five part 1");
        job.setJarByClass(Job5.class);
        job.setMapperClass(Job5.IDmapper.class);
        job.setReducerClass(Job5.TransactionsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("job_five_part1_output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}