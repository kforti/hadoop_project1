package jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class JobOne {

    public static class AgeMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line;
            StringTokenizer itr = new StringTokenizer(value.toString());
            Path filePath = ((FileSplit) context.getInputSplit()).getPath();
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            line = value.toString();
            String[] entries = line.split(",");
            if (fileName.contains("customers.txt")) {
                int age = Integer.parseInt(entries[2]);
                if (age >= 20 && age <= 50) {
                    context.write(new Text(fileName), new Text(line));
                }
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(JobOne.class);
        job.setMapperClass(AgeMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("job_one_output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}