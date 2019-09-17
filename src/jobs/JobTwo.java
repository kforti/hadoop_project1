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

public class JobTwo {

    public static class AgeMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // transid, custid, trans_total, trans_num_items, trans_desc
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = value.toString();
            String[] entries = line.split(",");
            String data = "";
            String custid = "";
            String sumtotal;

            if (fileName.contains("transactions.txt")) {
                custid = entries[1];
                sumtotal = entries[2];

                data = "1," + sumtotal; //custid + "," + sumtotal;

                }
            else if (fileName.contains("customers.txt")) {
                custid = entries[0];
                String cust_name = entries[1];

                data = cust_name;//custid + "," + cust_name;
            }

            context.write(new Text(custid), new Text(data));
            }
        }

    public static class TransactionsReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            float sum_total = 0;
            int trans_num = 0;
            String name = "";
            String entry = "";
            for (Text value:values) {
                String v = value.toString();
                if (v.contains(",")) {
                    String cost = v.split(",")[1];
                    sum_total += Float.parseFloat(cost);
                    trans_num += 1;
                }
                else {
                    name = v;
                }
            }
            entry = name + "," + Integer.toString(trans_num) + "," + Float.toString(sum_total);
            context.write(key, new Text(entry));
        }
    }

    public static class TransactionsCombiner
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            float sum_total = 0;
            int trans_num = 0;
            String name = "";
            String entry = "";
            for (Text value:values) {
                String v = value.toString();
                if (v.contains(",")) {
                    String cost = v.split(",")[1];
                    sum_total += Float.parseFloat(cost);
                    trans_num += 1;
                    entry = Integer.toString(trans_num) + "," + Float.toString(sum_total);
                }
                else {
                    name = v;
                    entry = name;
                }
            }

            context.write(key, new Text(entry));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job two");
        job.setJarByClass(JobOne.class);
        job.setMapperClass(AgeMapper.class);
        job.setCombinerClass(TransactionsCombiner.class);
        job.setReducerClass(TransactionsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("job_two_output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
