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

public class JobThree {
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
            String num_items;

            if (fileName.contains("transactions.txt")) {
                custid = entries[1];
                sumtotal = entries[2];
                num_items = entries[3];
                data = "1," + sumtotal + "," + num_items; //custid + "," + sumtotal;

            }
            else if (fileName.contains("customers.txt")) {
                custid = entries[0];
                String cust_name = entries[1];
                String salary = entries[5];
                data = cust_name + ";" + salary;//custid + "," + cust_name;
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
            String salary = "";
            int num_items;
            int lowest_num_items = 0;

            for (Text value:values) {
                String v = value.toString();
                if (v.contains(",")) {
                    String[] data = v.split(",");
                    String cost = data[1];
                    sum_total += Float.parseFloat(cost);
                    trans_num += Integer.parseInt(data[0]);
                    num_items = Integer.parseInt(data[2]);

                    if (lowest_num_items == 0) {
                        lowest_num_items = num_items;
                    }
                    else if (num_items < lowest_num_items) {
                        lowest_num_items = num_items;
                    }
                }
                else if (v.contains(";")) {
                    String[] data = v.split(";");
                    name = data[0];
                    salary = data[1];
                }
            }
            entry = name  + "," + salary + ","  + Integer.toString(trans_num) + "," + Float.toString(sum_total) + "," + Integer.toString(lowest_num_items);
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
            int num_items;
            int lowest_num_items = 0;

            for (Text value:values) {
                String v = value.toString();
                if (v.contains(",")) {
                    String[] data = v.split(",");
                    String cost = data[1];
                    sum_total += Float.parseFloat(cost);
                    trans_num += 1;
                    num_items = Integer.parseInt(data[2]);
                    if (lowest_num_items == 0) {
                        lowest_num_items = num_items;
                    }
                    else if (num_items < lowest_num_items) {
                        lowest_num_items = num_items;
                    }
                    entry = Integer.toString(trans_num) + "," + Float.toString(sum_total) + "," + Integer.toString(lowest_num_items);
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
        Job job = Job.getInstance(conf, "job three");
        job.setJarByClass(JobThree.class);
        job.setMapperClass(JobThree.AgeMapper.class);
        job.setCombinerClass(JobThree.TransactionsCombiner.class);
        job.setReducerClass(JobThree.TransactionsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("job_three_output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
