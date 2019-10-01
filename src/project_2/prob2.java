/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.p1;

import java.io.IOException;
import java.util.*;
import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.filecache.DistributedCache;


public class Proj2Prob2 {

    public static class KSort extends Mapper<Object, Text, Text, Text> {
        List<Float> kX = new ArrayList<Float>();
        List<Float> kY = new ArrayList<Float>();

        public void setup(Context context) throws IOException, InterruptedException {
            try {
                Configuration config = context.getConfiguration();
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(config);
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    kX.clear();
                    kY.clear();
                    BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                    while ((line = cacheReader.readLine()) != null) {
                        String[] tempVal = line.split(",");
                        kX.add(Float.parseFloat(tempVal[0]));
                        kY.add(Float.parseFloat(tempVal[1]));
                    }
                }

            } catch (IOException e) {
                System.err.println("Exception reading DistribtuedCache: " + e);
            }


        }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String keyO = "";
            String data = "";
            int x = -10000000;
            int y = -10000000;
            float dist = 0;
            float minDist = Float.MAX_VALUE;
            float minkX = 0;
            float minkY = 0;
            String line;
            Path filePath = ((FileSplit) context.getInputSplit()).getPath();
            line = value.toString();
            String[] entries = line.split(",");
            x = Integer.parseInt(entries[0]);
            y = Integer.parseInt(entries[1]);
            for (int i = 0; i < kX.size(); i++) {
                dist = (float) Math.sqrt(((kX.get(i) - x) * (kX.get(i) - x)) + ((kY.get(i) - y) * (kY.get(i) - y)));
                if (dist < minDist) {
                    minDist = dist;
                    minkX = kX.get(i);
                    minkY = kY.get(i);
                }
            }
            keyO = Float.toString(minkX) + "," + Float.toString(minkY);
            data = x + "," + y;
            context.write(new Text(keyO), new Text(data));
        }
    }


    public static class PartialSumCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum_totalx = 0;
            int sum_totaly = 0;
            int count = 0;
            String dataOut = "";
            int x = -1000000;
            int y = -1000000;

            for (Text value : values) {
                String v = value.toString();
                String[] data = v.split(",");
                x = Integer.parseInt(data[0]);
                y = Integer.parseInt(data[1]);

                sum_totalx += x;
                sum_totaly += y;
                count += 1;
            }
            dataOut = sum_totalx + "," + sum_totaly + "," + count;
            context.write(key, new Text(dataOut));
        }
    }


    public static class NewKPointsReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float newX = 0;
            float newY = 0;
            int totalx = 0;
            int totaly = 0;
            int totalCount = 0;
            int partTotalx = -100000;
            int partTotaly = -100000;
            int partTotalc = -100000;
            String keyOut = "";

            for (Text value : values) {
                String v = value.toString();
                String[] data = v.split(",");
                partTotalx = Integer.parseInt(data[0]);
                partTotaly = Integer.parseInt(data[1]);
                partTotalc = Integer.parseInt(data[2]);

                totalx += partTotalx;
                totaly += partTotaly;
                totalCount += partTotalc;
            }
            newX = (float) (totalx / totalCount);
            newY = (float) (totaly / totalCount);
            keyOut = Float.toString(newX) + "," + Float.toString(newY);
            context.write(new Text(keyOut), new Text(""));
        }
    }


    public static void main(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        String INITIAL_K_FILE = args[2];
        int itt = 0;
        boolean isDone = false;
        if (args.length != 3) {
            System.err.println("Invalid Or Missing Parameters: <HDFS input file> <HDFS output file> <HDFS Initial_K_Centroids file>");
            System.exit(2);
        }

        while (isDone == false) {
            Configuration conf = new Configuration();
            Path initKPath = new Path(INITIAL_K_FILE);
            DistributedCache.addCacheFile(initKPath.toUri(), conf);

            Job job = new Job(conf, "K-Means Job");
            job.setJarByClass(Proj2Prob2.class);
            job.setMapperClass(KSort.class);
            job.setCombinerClass(PartialSumCombiner.class);
            job.setReducerClass(NewKPointsReducer.class);
            job.setNumReduceTasks(1);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(job, input);
            FileOutputFormat.setOutputPath(job, new Path(output + itt));
            boolean finished = job.waitForCompletion(true);


            //Read in input file and get list of inital x, y cords
            List<Float> oldX = new ArrayList<Float>();
            List<Float> oldY = new ArrayList<Float>();
            List<Float> newX = new ArrayList<Float>();
            List<Float> newY = new ArrayList<Float>();

            Path ofile = new Path(INITIAL_K_FILE);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(ofile)));
            String line = br.readLine();
            while (line != null) {
                String[] sp = line.split(",");
                float oX = Float.parseFloat((sp[0]));
                float oY = Float.parseFloat((sp[1]));
                oldX.add(oX);
                oldY.add(oY);
                line = br.readLine();
            }
            br.close();

            Path prevfile = new Path(output + itt + "/part-r-00000");
            FileSystem fs1 = FileSystem.get(new Configuration());
            BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(prevfile)));
            String l = br1.readLine();
            while (l != null) {
                String[] sp1 = l.split(",");
                float nX = Float.parseFloat((sp1[0]));
                float nY = Float.parseFloat((sp1[1]));
                newX.add(nX);
                newY.add(nY);
                l = br1.readLine();
            }
            br1.close();

            Collections.sort(oldX);
            Collections.sort(oldY);
            Collections.sort(newX);
            Collections.sort(newY);
            boolean xSame = false;
            boolean ySame = false;

            for (int i = 0; i < newX.size(); i++) {
                if (Math.abs(oldX.get(i) - newX.get(i)) <= 0.1) {
                    xSame = true;
                } else {
                    xSame = false;
                    break;
                }
            }

            for (int j = 0; j < newX.size(); j++) {
                if (Math.abs(oldY.get(j) - newY.get(j)) <= 0.1) {
                    ySame = true;
                } else {
                    ySame = false;
                    break;
                }
            }

            if (xSame && ySame) {
                isDone = true;
            }

            INITIAL_K_FILE = output + itt + "/part-r-00000";
            itt += 1;
            if (itt == 6) {
                isDone = true;
            }
        }
    }
}