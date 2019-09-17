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
            context.write(key, new Text("," + age  + "," + gender + ","  + Float.toString(trans_num)));
        }
    }

    //}
    //-----------------------------------------------------------------------------------------------

    //Job 2{

    //Mapper
    public static class GroupGenderMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // custid, age, gender, trans_total
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = value.toString();
            String[] entries = line.split(",");
            String key = "";
            String ageGroup = "";
            int age = Integer.parseInt(entries[2]);
            String gender = entries[2];
            String transVal = entries[3];

            if ((10 <= age) &&  (age < 20)){
                ageGroup = "[10,20)";
            }
            else if((20 <= age) && (age < 30)){
                ageGroup = "[20,30)";
            }
            else if((30 <= age) && (age < 40)){
                ageGroup = "[30,40)";
            }
            else if((40 <= age) && (age < 50)){
                ageGroup = "[40,50)";
            }
            else if((50 <= age) && (age < 60)){
                ageGroup = "[50,60)";
            }
            else if((60 <= age) && (age <= 70)){
                ageGroup = "[60,70]";
            }

            key = ageGroup + "," + gender;

            String data = transVal;

            context.write(new Text(key), new Text(data));
        }
    }

    //----------------------------------------------------------------------------

    //Reducer
    public static class TransSolverReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float maxTotal = Integer.MIN_VALUE;
            float minTotal = Integer.MAX_VALUE;
            float custTotal = 0;
            float transTotalSum = 0;
            int numOfCusts = 0;
            float avgTotal = 0;

            for (Text value:values) {
                String v = value.toString();
                custTotal = Float.parseFloat(v);
                numOfCusts += 1;
                transTotalSum += custTotal;
                if(custTotal > maxTotal){
                    maxTotal = custTotal;
                }
                else if(custTotal < minTotal){
                    minTotal = custTotal;
                }
            }
            avgTotal = transTotalSum/numOfCusts;

            context.write(key, new Text("," + Float.toString(minTotal)  + "," + Float.toString(maxTotal) + ","  + Float.toString(avgTotal)));
        }
    }

    //}
    //-----------------------------------------------------------------------------------------------

    //Driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "job five part 1");
        job1.setJarByClass(Job5.class);
        job1.setMapperClass(Job5.IDmapper.class);
        job1.setReducerClass(Job5.TransactionsReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path("input"));
        FileOutputFormat.setOutputPath(job1, new Path("job_five_part1_output"));
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "job five part 2");
        job1.setJarByClass(Job5.class);
        job1.setMapperClass(Job5.GroupGenderMapper.class);
        job1.setReducerClass(Job5.TransSolverReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path("input")); // this need to be the file path to output from part 1
        FileOutputFormat.setOutputPath(job1, new Path("job_five_output"));
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}