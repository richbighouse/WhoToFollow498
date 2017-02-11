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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.GetConf;
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
                context.write(user,friend1);
            }
        }
    }
    
    public static class ReducerOne extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
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
		public void countFollowers(int follower, HashMap<Integer, Integer> allFollowers)
		{
			if(follower < 0)
			{
				allFollowers.put(follower*-1, -1);
			}
			else
			{
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
            while (values.iterator().hasNext()) 
            {
                 int value = values.iterator().next().get();
                 countFollowers(value,allFollowers);
                 temp.add(value);
            }             
            
            Map<Integer, Integer> sortedFollowers = sortByValue(allFollowers);
            
            StringBuilder sb = new StringBuilder();
            for(Map.Entry<Integer, Integer> follower : sortedFollowers.entrySet())
            {
            	if(follower.getValue()>0 && follower.getKey() != key.get())
            	{
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
                
        Path inputPath;//is the input path of the FILE
        Path finalOutputPath; //is the final output after all map-reduce jobs
        Path tempPath = new Path("file:///home//epar//Temp"); //used to feed output of first map-reduce job to the 2nd
        
        if(args.length < 1)
        {
        	//Has to be the absolute path of a file on your drive
        	inputPath = new Path("file:///home//epar//input//Test"); //Takes the Test file in the src if no args are specified
        	
        	//has to be the absolute path of a non-existing directory on your drive
        	finalOutputPath = new Path("file:///home//epar//FinalOutput");
        }
        else
        {
        	inputPath = new Path(args[0]);
        	if(args.length < 2)
        	{
        		finalOutputPath = new Path("file:///home//epar//FinalOutput");
        	}
        	else
        	{
        		finalOutputPath = new Path("file:///home//epar//FinalOutput");
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

}
