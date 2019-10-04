package hadoop.proj2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class pointsFileCreate {
    String pointsFilePath = "Points.txt";

    public void genData() {

        int x = 0;
        int y = 0;
        String content = "";

        BufferedWriter bw = null;
        FileWriter fw = null;

        try {

            fw = new FileWriter(pointsFilePath);
            bw = new BufferedWriter(fw);

            for(int i=0; i < 11000000; i++) {
                x = getRandomNumberInRange(0, 10000);
                y = getRandomNumberInRange(0, 10000);
                if(i < (11000000-1)) {
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
        p.genData();
    }
}