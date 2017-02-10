import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.function.Predicate;
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
    public static class AllPairsMapper extends Mapper<Object, Text, IntWritable, IntWritable>
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

    public static class CountReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

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
            	
            	key.set(u);
            	emittedValues = new Text(curValues);
            	context.write(key,emittedValues);
            	//Emitting all permutations of the values ex: for key = 3 values = [1 2] 
            	//It emits Key = 1 Values = [-3 2] and Key = 2 Values = [-3 1]
            }
        }   
        
        // The reduce method       
  /*      public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            IntWritable user = key;
            // 'existingFriends' will store the friends of user 'user'
            // (the negative values in 'values').
            ArrayList<Integer> existingFriends = new ArrayList();
            // 'recommendedUsers' will store the list of user ids recommended
            // to user 'user'
            ArrayList<Integer> recommendedUsers = new ArrayList<>();
            while (values.iterator().hasNext()) 
            {
                int value = values.iterator().next().get();
                if (value > 0) {
                    recommendedUsers.add(value);
                } else {
                    existingFriends.add(value);
                }
            }
            // 'recommendedUsers' now contains all the positive values in 'values'.
            // We need to remove from it every value -x where x is in existingFriends.
            // See javadoc on Predicate: https://docs.oracle.com/javase/8/docs/api/java/util/function/Predicate.html
            for (final Integer friend : existingFriends) {
                recommendedUsers.removeIf(new Predicate<Integer>() {
                    @Override
                    public boolean test(Integer t) {
                        return t.intValue() == -friend.intValue();
                    }
                });
            }
            ArrayList<Recommendation> recommendations = new ArrayList<>();
            // Builds the recommendation array
            for (Integer userId : recommendedUsers) {
                Recommendation p = Recommendation.find(userId, recommendations);
                if (p == null) {
                    recommendations.add(new Recommendation(userId));
                } else {
                    p.addCommonFriend();
                }
            }
            // Sorts the recommendation array
            // See javadoc on Comparator at https://docs.oracle.com/javase/8/docs/api/java/util/Comparator.html
            recommendations.sort(new Comparator<Recommendation>() {
                @Override
                public int compare(Recommendation t, Recommendation t1) {
                    return -Integer.compare(t.getNCommonFriends(), t1.getNCommonFriends());
                }
            });
            // Builds the output string that will be emitted
            StringBuffer sb = new StringBuffer(""); // Using a StringBuffer is more efficient than concatenating strings
            for (int i = 0; i < recommendations.size() && i < 10; i++) {
                Recommendation p = recommendations.get(i);
                sb.append(p.toString() + " ");
            }
            Text result = new Text(sb.toString());
            context.write(user, result);
        }
*/
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "people you may know");
        job.setJarByClass(WhoToFollow.class);
        job.setMapperClass(AllPairsMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
                
        Path inputPath;
        Path outputPath;
        if(args.length < 1)
        {
        	//Has to be the absolute path of a file on your drive
        	inputPath = new Path("file:///home//epar//input//Test"); //Takes the Test file in the src if no args are specified
        	//has to be the absolute path of a non-existing directory on your drive
        	outputPath = new Path("file:///home//epar//Test");
        }
        else
        {
        	inputPath = new Path(args[0]);
        	if(args.length < 2)
        	{
        		outputPath = new Path("file:///Output");
        	}
        	else
        	{
        		outputPath = new Path(args[1]);
        	}
        }
        FileInputFormat.addInputPath(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
