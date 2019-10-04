package hadoop.proj2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class pointsFileCreate {
    String pointsFilePath = "points.txt";
    String kCentroidsFilePath = "kCentroids.txt";

    public void genPointsData() {

        int x = 0;
        int y = 0;
        String content = "";

        BufferedWriter bw = null;
        FileWriter fw = null;

        try {

            fw = new FileWriter(pointsFilePath);
            bw = new BufferedWriter(fw);
            int loop = 11000000; //given number of points to create 100 MB file

            //loop through and create the given number of points and write it to file
            for(int i=0; i < loop; i++) {
                //randomly generate the x and y coordinates for each point
                x = getRandomNumberInRange(0, 10000);
                y = getRandomNumberInRange(0, 10000);
                if(i < (loop-1)) {
                    content = Integer.toString(x) + "," + Integer.toString(y) + "\n";
                }
                else{
                    content = Integer.toString(x) + "," + Integer.toString(y);
                }
                bw.write(content);
            }


        } catch (IOException e) {
        }
        finally {
            try {
                if (bw != null)
                    bw.close();

                if (fw != null)
                    fw.close();
            } catch (IOException ex) {
            }
        }
    }

    public void genKCentroidsData() {

        int x = 0;
        int y = 0;
        String content = "";

        BufferedWriter bw = null;
        FileWriter fw = null;

        try {

            fw = new FileWriter(kCentroidsFilePath);
            bw = new BufferedWriter(fw);
            int knum = getRandomNumberInRange(10,100);  //randomly generated number of centroids

            //for each centroid generate random coordinates and write them to file
            for(int i=0; i < knum; i++) {
                x = getRandomNumberInRange(0, 10000);
                y = getRandomNumberInRange(0, 10000);
                if(i < (knum-1)) {
                    content = Integer.toString(x) + "," + Integer.toString(y) + "\n";
                }
                else{
                    content = Integer.toString(x) + "," + Integer.toString(y);
                }
                bw.write(content);
            }


        } catch (IOException e) {
        }
        finally {
            try {
                if (bw != null)
                    bw.close();

                if (fw != null)
                    fw.close();
            } catch (IOException ex) {
            }
        }
    }

    private static int getRandomNumberInRange(int min, int max) {
        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;     //get a random number in range
    }

    public static void main(String[] args) {
        //create the input files
        pointsFileCreate p = new pointsFileCreate();
        p.genKCentroidsData();
        p.genPointsData();
    }
}