import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.function.Predicate;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WhoToFollow{

    /**
     * *****************
     */
    /**
     * Mapper      *
     */
    /**
     * *****************
     */
    public static class MapperOne extends Mapper<Object, Text, IntWritable, IntWritable>
    {
    	public void map(Object key, Text values, Context context) throws IOException, InterruptedException 
        {
        	//Holds all the values
            StringTokenizer st = new StringTokenizer(values.toString());
            IntWritable user = new IntWritable(Integer.parseInt(st.nextToken()));
            IntWritable friend1 = new IntWritable();
            
            while (st.hasMoreTokens()) 
            {
                Integer friend = Integer.parseInt(st.nextToken());
                friend1.set(friend);
                //emit all (friend, user) pair
                context.write(friend1,user);
            }
        }
    }

    public static class MapperTwo extends Mapper<Object, Text, IntWritable, IntWritable>
    {
    	public void map(Object key, Text values, Context context) throws IOException, InterruptedException 
        {
        	//Holds all the values
            StringTokenizer st = new StringTokenizer(values.toString());
            IntWritable user = new IntWritable(Integer.parseInt(st.nextToken()));
            IntWritable friend1 = new IntWritable();
            
            while (st.hasMoreTokens()) 
            {
                Integer friend = Integer.parseInt(st.nextToken());
                friend1.set(friend);
                //emit all (friend, user) pair
                context.write(friend1,user);
            }
        }
    }
    
    public static class ReducerOne extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        // A private class to describe a recommendation.
        // A recommendation has a friend id and a number of friends in common.
        private static class Recommendation {

            // Attributes
            private int friendId;
            private int nCommonFriends;

            // Constructor
            public Recommendation(int friendId) {
                this.friendId = friendId;
                // A recommendation must have at least 1 common friend
                this.nCommonFriends = 1;
            }

            // Getters
            public int getFriendId() {
                return friendId;
            }

            public int getNCommonFriends() {
                return nCommonFriends;
            }

            // Other methods
            // Increments the number of common friends
            public void addCommonFriend() {
                nCommonFriends++;
            }

            // String representation used in the reduce output            
            public String toString() {
                return friendId + "(" + nCommonFriends + ")";
            }

            // Finds a representation in an array
            public static Recommendation find(int friendId, ArrayList<Recommendation> recommendations) {
                for (Recommendation p : recommendations) {
                    if (p.getFriendId() == friendId) {
                        return p;
                    }
                }
                // Recommendation was not found!
                return null;
            }

        }

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
        	//Holds all the values
            ArrayList<Integer> users = new ArrayList<Integer>();
        	
            while (values.iterator().hasNext()) 
            {
                 int value = values.iterator().next().get();
                 users.add(value);
            }           
            Text emittedValues;
            IntWritable newKey = new IntWritable();
            for(Integer u : users)
            {
                String curValues = ((Integer)(-1*key.get())).toString();//values that will be emitted (starts with -key)
            	for (Integer u2 : users)
            	{
            		if(!u.equals(u2))
            		{
            			curValues += " " + u2.toString();
            		}
            	}
            	
            	newKey.set(u);
            	emittedValues = new Text(curValues);
            	context.write(newKey,emittedValues);
            	//Emitting all permutations of the values ex: for key = 3 values = [1 2] 
            	//It emits Key = 1 Values = [-3 2] and Key = 2 Values = [-3 1]
            }
        }   
    }

    public static class ReducerTwo extends Reducer<IntWritable, IntWritable, IntWritable, Text> 
    {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
        	//Holds all the values
            ArrayList<Integer> users = new ArrayList<Integer>();
        	
            while (values.iterator().hasNext()) 
            {
                 int value = values.iterator().next().get();
                 users.add(value);
            }           
            Text emittedValues;
            IntWritable newKey = new IntWritable();
            for(Integer u : users)
            {
                String curValues = ((Integer)(-1*key.get())).toString();//values that will be emitted (starts with -key)
            	for (Integer u2 : users)
            	{
            		if(!u.equals(u2))
            		{
            			curValues += " " + u2.toString();
            		}
            	}
            	
            	newKey.set(u);
            	emittedValues = new Text(curValues);
            	context.write(newKey,emittedValues);
            	//Emitting all permutations of the values ex: for key = 3 values = [1 2] 
            	//It emits Key = 1 Values = [-3 2] and Key = 2 Values = [-3 1]
            }
        }   

    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException 
    {
        //First map-reduce job
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Who to follow 1st run");
        job.setJarByClass(WhoToFollow.class);
        job.setMapperClass(MapperOne.class);
        job.setReducerClass(ReducerOne.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
                
        Path inputPath;//is the input path of the FILE
        Path finalOutputPath = null; //is the final output after all map-reduce jobs
        Path tempPath = null; //used to feed output of first map-reduce job to the 2nd
      
        if(args.length < 1)
        {
        	inputPath = new Path("file:///home//rich//dev//WhoToFollow498//input.txt"); 
            File tempFolder = new File("tempFolder");
            if(!tempFolder.exists()){
    			boolean result = false;
    			try{
    	    		result = tempFolder.mkdir();
    			} catch (SecurityException se){
    				System.out.println("OH NOES YOU CANT CREATE A FOLDER HERE!");
    			}
    			if(result){
    				System.out.println("dir created");
    			}
        	}
    		tempPath = new Path(tempFolder.getAbsolutePath());
    		FileUtils.deleteDirectory(tempFolder);    
    		
            File finalFolder = new File("finalFolder");
            if(!finalFolder.exists()){
    			boolean result = false;
    			try{
    	    		result = finalFolder.mkdir();
    			} catch (SecurityException se){
    				System.out.println("OH NOES YOU CANT CREATE A FOLDER HERE!");
    			}
    			if(result){
    				System.out.println("dir created");
    			}
        	}
    		finalOutputPath = new Path(finalFolder.getAbsolutePath());
    		FileUtils.deleteDirectory(finalFolder);    	
        }
        else
        {
        	inputPath = new Path(args[0]);
        	finalOutputPath = new Path(args[1]);
    		String tempFolderString = finalOutputPath.toString().substring(0,finalOutputPath.toString().lastIndexOf("//"));
    		File tempFolder = new File(tempFolderString);
    		if(!tempFolder.exists()){
    			boolean result = false;
    			try{
    	    		result = tempFolder.mkdir();
    			} catch (SecurityException se){
    				System.out.println("OH NOES YOU CANT CREATE A FOLDER HERE!");
    			}
    			if(result){
    				System.out.println("dir created");
    			}
        	}
    		tempPath = new Path(tempFolder.getAbsolutePath());
    		FileUtils.deleteDirectory(tempFolder);           	

        }
        FileInputFormat.addInputPath(job,inputPath);
        FileOutputFormat.setOutputPath(job,tempPath);

        job.waitForCompletion(true);

        //Second job of map-reduce
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(WhoToFollow.class);
        job2.setJobName("Who to follow pt2");
        job2.setMapperClass(MapperTwo.class);
        job2.setReducerClass(ReducerTwo.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2,tempPath);
        FileOutputFormat.setOutputPath(job2,finalOutputPath);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

	private static void setTempPathRunFromConsole(Path finalOutputPath,
			Path tempPath) {
		int lastIndexOfSlash = finalOutputPath.toString().lastIndexOf("//");
		String tempFolderString = finalOutputPath.toString().substring(0,finalOutputPath.toString().lastIndexOf("//"));
		File tempFolder = new File(tempFolderString);
	}
    

}
