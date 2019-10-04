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
            int loop = 11000000;

            for(int i=0; i < loop; i++) {
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
            int knum = getRandomNumberInRange(10,100);

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
        return r.nextInt((max - min) + 1) + min;
    }

    public static void main(String[] args) {
        pointsFileCreate p = new pointsFileCreate();
        p.genKCentroidsData();
        p.genPointsData();
    }
}