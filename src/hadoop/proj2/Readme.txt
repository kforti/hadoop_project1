Milap Patel
Kevin

Project 2 Problems Build/Evacuation directions


Package is as follows: package hadoop.proj2.(java program file name)
Points file should be named: points.txt                 //parameter file for problems 1, 2, 4
K centroids file should be named: kCentroids.txt        //extra parameter file for problem 2

To run the problems in hadoop, please make the following directories in hdfs using following commends: (the "/user/hadoop" part can be different for you machine)
hadoop fs -mkdir /user/hadoop/inputPoints               //File directory where the points file will go
hadoop fs -mkdir /user/hadoop/inputKcentroids           //File directory where the K centroids file with go


Please upload the files to these directories using the following commends: (the "/user/hadoop" part can be different for you machine)
hadoop fs -put /home/hadoop/Desktop/Proj2/points.txt /user/hadoop/inputPoints
hadoop fs -put /home/hadoop/Desktop/Proj2/kCentroids.txt /user/hadoop/inputKcentroids


To run the program for problem 1, execute the following commends:
add directions to run prob1 here



To run the program for problem 2, execute the following commends:
cd to directory to the appropriate directory where the java files are located
mkdir prob2_classes                                                                             //make folder to compile java file
javac -classpath /usr/share/hadoop/hadoop-core-1.2.1.jar -d prob2_classes ./Proj2Prob2.java     //Compile java files
jar -cvf ./Proj2Prob2.jar -C prob2_classes/ .                                                   //Make jar file
hadoop jar ./Proj2Prob2.jar hadoop.proj2.Proj2Prob2 /user/hadoop/inputPoints /user/hadoop/outputProb2/output /user/hadoop/inputKcentroids/kCentroids.txt    //Run Job
output for the job will be located in the outputProb2 folder in HDFS


To run the program for problem 3, execute the following commends:
add directions to run prob3 here



To run the program for problem 4, execute the following commends:
cd to directory to the appropriate directory where the java files are located
mkdir prob4_classes                                                                             //make folder to compile java file
javac -classpath /usr/share/hadoop/hadoop-core-1.2.1.jar -d prob4_classes ./Proj2Prob4.java     //Compile java files
jar -cvf ./Proj2Prob4.jar -C prob4_classes/ .                                                   //Make jar file
hadoop jar ./Proj2Prob4.jar hadoop.proj2.Proj2Prob4 /user/hadoop/inputPoints /user/hadoop/outputProb4/output "Radius" "K#"      //Run Job
output for the job will be located in the outputProb4 folder in HDFS