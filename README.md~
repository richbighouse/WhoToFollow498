# WhoToFollow498
The goal is to write an algorithm that will recommend to user X a list of people to follow Fi based on the number of followers that X and Fi have in common.

Developed by Simon Houle 27194706 and Richard Grand'Maison 26145965.

https://github.com/richbighouse/WhoToFollow498

How to compile:

1. Have Hadoop, Java, HDFS and Yarn set up as per the Getting started with Hadoop Lab. The complete instruction can be found here https://github.com/glatard/big-data-analytics-course/releases/download/0.7.1/intro.pdf.

2. Run the following command in your terminal to add the commons-io-2.4jar to the CLASSPATH:
$ export CLASSPATH=$CLASSPATH:${HADOOP}/share/hadoop/common/lib/commons-io-2.4.jar

3. Compile the program:
$ javac WhoToFollow.java

4. Create a jar containing all the classes in WhoToFollow:
$ jar cvf whotofollow.jar WhoToFollow*.class

5. (Optional) Generate test input using the generate.py file. 
$ python generate.py n f > yourfile.txt
Where n is the number of users and f is the maximum number of followers per user.

6. Run the following commands to start dfs and yarn:
$ start-dfs.sh
$ start-yarn.sh

7. To run the program locally, run the following command:
$ hadoop jar whotofollow.jar WhoToFollow file:///your/path/to/input.txt file:///your/path/to/non/yet/existing/directory

8. To run the program from your hdfs, run the following command:
$ hadoop jar whotofollow.jar WhoToFollow input.txt output


	
