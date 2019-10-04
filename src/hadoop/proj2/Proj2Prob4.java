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

package hadoop.proj2;

import java.io.IOException;
import java.util.*;

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


public class Proj2Prob4 {

    public static class RegionalPartition extends Mapper<Object, Text, Text, Text> {
        String rTmp = "";

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            rTmp = config.get("rVal");
        }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int r = Integer.parseInt(rTmp);
            int bl = r * 2;
            int px = 0;
            int py = 0;
            int bx = 0;
            int by = 0;
            int bLx = 0;
            int bRx = 0;
            int bTy = 0;
            int bBy = 0;

            String line;
            Path filePath = ((FileSplit) context.getInputSplit()).getPath();
            line = value.toString();
            String[] entries = line.split(",");
            px = Integer.parseInt(entries[0]);
            py = Integer.parseInt(entries[1]);
            bx = px / bl;
            by = py / bl;
            bLx = bl * bx;
            bRx = (bl * bx) + bl;
            bTy = (bl * by);
            bBy = (bl * by) + bl;
            context.write(new Text(bx + "," + by), new Text(px + "," + py));

            //Check x box boundry conditions
            if (Math.abs(px - bLx) <= r) {
                if (bx > 0) {
                    context.write(new Text((bx - 1) + "," + by), new Text(px + "," + py));
                }
            } else if (Math.abs(bRx - px) <= r) {
                context.write(new Text((bx + 1) + "," + by), new Text(px + "," + py));
            }

            //Check y box boundry conditions
            if (Math.abs(py - bTy) <= r) {
                if (by > 0) {
                    context.write(new Text(bx + "," + (by - 1)), new Text(px + "," + py));
                }
            } else if (Math.abs(bBy - py) <= r) {
                context.write(new Text(bx + "," + (by + 1)), new Text(px + "," + py));
            }

            //Check diagonal box boundry conditions
            if ((Math.abs(px - bLx) <= r) && (Math.abs(py - bTy) <= r)) {
                if ((bx > 0) && (by > 0)) {
                    context.write(new Text((bx - 1) + "," + (by - 1)), new Text(px + "," + py));
                }
            } else if ((Math.abs(bRx - px) <= r) && (Math.abs(py - bTy) <= r)) {
                if (by > 0) {
                    context.write(new Text((bx + 1) + "," + (by - 1)), new Text(px + "," + py));
                }
            } else if ((Math.abs(bRx - px) <= r) && (Math.abs(bBy - py) <= r)) {
                context.write(new Text((bx + 1) + "," + (by + 1)), new Text(px + "," + py));
            } else if ((Math.abs(px - bLx) <= r) && (Math.abs(bBy - py) <= r)) {
                if (bx > 0) {
                    context.write(new Text((bx - 1) + "," + (by + 1)), new Text(px + "," + py));
                }
            }
        }
    }

    public static class OutlierDectation extends Reducer<Text, Text, Text, Text> {
        String rTmp = "";
        String kTmp = "";

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            rTmp = config.get("rVal");
            kTmp = config.get("kVal");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Integer> ibx = new ArrayList<Integer>();
            List<Integer> iby = new ArrayList<Integer>();
            List<Integer> alx = new ArrayList<Integer>();
            List<Integer> aly = new ArrayList<Integer>();
            int r = Integer.parseInt(rTmp);
            int k = Integer.parseInt(kTmp);
            int bl = r * 2;
            int bx = 0;
            int by = 0;
            int bLx = 0;
            int bRx = 0;
            int bTy = 0;
            int bBy = 0;
            int px = 0;
            int py = 0;

            String box = key.toString();
            String[] boxData = box.split(",");
            bx = Integer.parseInt(boxData[0]);
            by = Integer.parseInt(boxData[1]);
            bLx = bl * bx;
            bRx = (bl * bx) + bl;
            bTy = (bl * by);
            bBy = (bl * by) + bl;

            for (Text value : values) {
                String v = value.toString();
                String[] data = v.split(",");
                px = Integer.parseInt(data[0]);
                py = Integer.parseInt(data[1]);
                alx.add(px);
                aly.add(py);
                if ((px > bLx) && (px < bRx) && (py > bTy) && (py < bBy)) {
                    ibx.add(px);
                    iby.add(py);
                } else if (((px == bLx) || (px == bRx)) && ((py > bTy) && (py < bBy)) && ((px / bl) == bx)) {
                    ibx.add(px);
                    iby.add(py);
                } else if (((px > bLx) && (px < bRx)) && ((py == bTy) || (py == bBy)) && ((py / bl) == by)) {
                    ibx.add(px);
                    iby.add(py);
                } else if (((px == bLx) || (px == bRx)) && ((py == bTy) || (py == bBy)) && (((px / bl) == bx) && ((py / bl) == by))) {
                    ibx.add(px);
                    iby.add(py);
                }
            }

            for (int j = 0; j < ibx.size(); j++) {
                int c = -1; //copy of the same processing point will be in the all-points array as well so taking that into account start at -1 instead of 0
                for (int i = 0; i < alx.size(); i++) {
                    double dist = Math.sqrt(((ibx.get(j) - alx.get(i)) * (ibx.get(j) - alx.get(i))) + ((iby.get(j) - aly.get(i)) * (iby.get(j) - aly.get(i))));
                    if (dist <= r) {
                        c = c + 1;
                    }
                }
                if (c < k) {
                    context.write(new Text(ibx.get(j) + "," + iby.get(j)), new Text(""));
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Invalid Or Missing Parameters: <HDFS input file> <HDFS output file> <R Value> <K Value>");
            System.exit(2);
        }
        String input = args[0];
        String output = args[1];
        String r = args[2];
        String k = args[3];

        Configuration conf = new Configuration();
        conf.set("rVal", r);
        conf.set("kVal", k);

        Job job = new Job(conf, "Outlier Dectaction");
        job.setJarByClass(Proj2Prob4.class);
        job.setMapperClass(RegionalPartition.class);
        job.setReducerClass(OutlierDectation.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}