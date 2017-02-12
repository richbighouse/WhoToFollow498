import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

/* The following code is written by Simon Houle 27194706 and Richard Grand'Maison 26145965
 * 
 * This program is a recommendation system that recommends possible users to follow based on the persons we follow
 * for example if user A and B follow the same persons, user A would be recommended to user B.
 */

public class WhoToFollow
{
	
	public static class MapperOne extends Mapper<Object, Text, IntWritable, IntWritable>
    {
		/* First mapper emits all (friend, user) pair
		 * Example:
		 * input: 1 3 4 5
		 * 		  2 1 3 5
		 * 
		 * output: (3,1), (4,1), (5,1)
		 * 		   (1,2), (3,2), (5,2)
		 * */
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
                context.write(friend1,user);
            }
        }
    }

	/* Second mapper class, for inputs (1, 2,-3), (2, 1, -3),(1, -4), (1, 2, -5), (2, 1, -5), (2, -1)
	 * will output: (1,2),(1,-3),(1,4),(1,2),(1,-5),(2,-1)
	 * It is mainly used because it will group it's outputs by keys for the 2nd reducer
	 */
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
                context.write(user,friend1);
            }
        }
    }
    
    public static class ReducerOne extends Reducer<IntWritable, IntWritable, IntWritable, Text> 
    {
    	/* First reducer task, for input 3 [1,2], 4[1], 5[1,2], 1[2]
    	 * It will output (1, 2,-3), (2, 1, -3),(1, -4), (1, 2, -5), (2, 1, -5), (2, -1)
    	 */
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
            	//multiplying by -1 because those as keys are already being followed by those as values
                String curValues = ((Integer)(-1*key.get())).toString();
            	for (Integer u2 : users)
            	{
            		if(!u.equals(u2)) //Appending all possibles persons for user "newKey" that newKey would be interested in following
            		{
            			curValues += " " + u2.toString();
            		}
            	}
            	
            	newKey.set(u);
            	emittedValues = new Text(curValues);
            	context.write(newKey,emittedValues);
            }
        }   
    }
    
	/* Final reducer for input: 1[2,-3,-4,-5,2], 2[1, -3, -5, 1, -1]
	 * will output: 1 [2(2)]
	 * 				2 []
     */
    public static class ReducerTwo extends Reducer<IntWritable, IntWritable, IntWritable, Text> 
    {
    	//Method used to keep track of the count of followers. It is appending the values to a fed Hashmap.
		public void countFollowers(int follower, HashMap<Integer, Integer> allFollowers)
		{
			//If the follower is negative, I add it to the Hashmap as its positive values(for the key) and I set 
			//the value to -1 because later I'm only recommending people with a count greater than 0.
			if(follower < 0)
			{
				allFollowers.put(follower*-1, -1); 
			}
			else
			{
				//If the follower exists in the Hashmap, it will append to it.
				//If the follower exists with value -1, it won't do anything.
				//If the follower doesn't exist, it adds it in the Hashmap.
				if(allFollowers.containsKey(follower))
				{
					int number = allFollowers.get(follower);
					if(number != -1)
					{
						allFollowers.put(follower, allFollowers.get(follower) + 1);
					}
				}
				else
				{
					allFollowers.put(follower, 1);    				
				}
			}
		}	
    	
		//Method took online used to sort a map by its values.
		//http://stackoverflow.com/questions/109383/sort-a-mapkey-value-by-values-java
		public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue( Map<K, V> map )
		{
		    List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
		    Collections.sort(list, new Comparator<Map.Entry<K, V>>()
		    {
		        @Override
		        public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
		        {
		            return ( o1.getValue() ).compareTo( o2.getValue() );
		        }
		    } );
	
		    Map<K, V> result = new LinkedHashMap<>();
		    
		    for (Map.Entry<K, V> entry : list)
		    {
		        result.put(entry.getKey(), entry.getValue());
		    }
		    return result;
		}
	
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
        	HashMap<Integer, Integer> allFollowers = new HashMap<Integer,Integer>();
        	ArrayList<Integer> temp = new ArrayList<>();
        	//Loops the values and adds them in the hashmap.
            while (values.iterator().hasNext()) 
            {
                 int value = values.iterator().next().get();
                 countFollowers(value,allFollowers);
                 temp.add(value);
            }             
            
            //Sorts the hashmap by its values.
            Map<Integer, Integer> sortedFollowers = sortByValue(allFollowers);
            
            StringBuilder sb = new StringBuilder();
            for(Map.Entry<Integer, Integer> follower : sortedFollowers.entrySet())
            {
            	if(follower.getValue()>0 && follower.getKey() != key.get())
            	{
            		//Create desired output "key [suggestion(number), ...]
            		sb.append(" "+follower.getKey()+"("+follower.getValue()+")");
            	}
            }
            Text output = new Text(sb.toString());
            context.write(key, output);
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
                
        Path inputPath;//Is the input path of the FILE
        Path finalOutputPath = null; //Is the final output after all map-reduce jobs
        Path tempPath = null; //Used to feed output of first map-reduce job to the 2nd
      
        //These are mainly used because we were testing in eclipse without the command line
        if(args.length < 1)
        {
        	inputPath = new Path("file:///home//rich//dev//WhoToFollow498//input.txt"); //Hard-coded path for testing purposes
            File tempFolder = new File("tempFolder");
            
            if(!tempFolder.exists())
            {
    			boolean result = false;
    			try
    			{
    	    		result = tempFolder.mkdir();
    			} catch (SecurityException se)
    			{
    				System.out.println("OH NOES YOU CANT CREATE A FOLDER HERE!");
    			}
    			if(result)
    			{
    				System.out.println("dir created");
    			}
        	}
            
    		tempPath = new Path(tempFolder.getAbsolutePath());
    		FileUtils.deleteDirectory(tempFolder);    
    		
            File finalFolder = new File("finalFolder");
            
            if(!finalFolder.exists())
            {
    			boolean result = false;
    			try
    			{
    	    		result = finalFolder.mkdir();
    			} catch (SecurityException se){
    				System.out.println("OH NOES YOU CANT CREATE A FOLDER HERE!");
    			}
    			if(result)
    			{
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
        	tempPath = new Path("file:///home/rich/dev/WhoToFollow498/bin/temp");
        	File temp = new File("temp");
        	if(temp.exists())
        	{
        		FileUtils.deleteDirectory(temp);
        	}
        	
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

	private static void setTempPathRunFromConsole(Path finalOutputPath, Path tempPath)
	{
		int lastIndexOfSlash = finalOutputPath.toString().lastIndexOf("//");
		String tempFolderString = finalOutputPath.toString().substring(0,finalOutputPath.toString().lastIndexOf("//"));
		File tempFolder = new File(tempFolderString);
	}
}
