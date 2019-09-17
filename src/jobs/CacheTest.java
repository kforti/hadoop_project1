package jobs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CacheTest {

    public static class AgeMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public class MapClass extends Mapper {

            private Text word = new Text();

            protected void setup(Context context) throws IOException, InterruptedException {
                try {
                    BufferedReader bufferedReader = new BufferedReader(new FileReader("./customers.txt"));
                    String entry = null;
                    while ((entry = bufferedReader.readLine()) != null) {
                        String[] data = entry.split(",");
                        context.write(new Text(data[0].toString()), new Text(data[2]));
                    }
                } catch (IOException ex) {
                    System.err.println("Exception in mapper setup: " + ex.getMessage());
                }
            }

            /**
             * map function of Mapper parent class takes a line of text at a time
             * splits to tokens and passes to the context as word along with value as one
             */
            public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
                String line;
                StringTokenizer itr = new StringTokenizer(value.toString());
                Path filePath = ((FileSplit) context.getInputSplit()).getPath();
                String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
                line = value.toString();
                String[] entries = line.split(",");
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
        job.addCacheFile(new URI("cache/customers.txt"));
        job.setJarByClass(CacheTest.class);
        job.setMapperClass(AgeMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("cache_output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}