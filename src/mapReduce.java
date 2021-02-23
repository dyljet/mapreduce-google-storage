 
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class mapReduce {

  public static class TempMapper
       extends Mapper<Object, Text, Text, IntWritable>{  //sets the inputs and outputs of mapper

    private Text ld = new Text();     //setting the key ld(location+date) as a Text
    private IntWritable tempp = new IntWritable();  //setting the value as a IntWritable

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    String col = value.toString();       //puts all the values to string
    String [] colSplit = col.split(",");   //separates each value by ','
    String loc = colSplit[0];              //creates a string location which takes the whole location column at index 0
    String date = colSplit[1];         //creates date string, takes whole date column at index 1
    String temp = colSplit[3];        //creates temp string, takes whole temperature column at index 3
    String type = colSplit[2];        //creates type string takes whole type column at index 2
    
   if ( ((loc.equals("UK000056225")) && ( (type.contentEquals("TMIN")) || 
		   (type.contentEquals("TMAX")) )    ) ||  /*The first part of this massive if statement takes all cases 
		                                          where location is oxford and the type is either TMIN or TMAX*/
		   
		   ((loc.equals("UK000003377")) && ( (type.contentEquals("TMIN")) || 
				   (type.contentEquals("TMAX")) )   ) )     { //the second part does the similar thing but for waddinton
	   
	   tempp.set(Integer.parseInt(temp)); //the temp is turned into IntWritable so that it can be put into the value
	      ld.set(loc + "," + date);  /*this sets ld as the location and date concatenated when the if statement
	                                   is satisfied. this is so that is can be sent to the reducer in a shuffled
	                                   format, sorting by location and then the date*/
	      context.write(ld, tempp);	   //the key and value is set here to be sent to the reducer
   }
   }
    }

  public static class TempReducer
       extends Reducer<Text,IntWritable,Text,FloatWritable> {  //sets input and output of reducer

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	    float val1 = 0; //Defining the first value that is used in the if statement inside of the iterator
    	    float val2=0;  //defining the second value that the iterator reads
           float sum= 0; //defining the sum that is calculated
             for (IntWritable value : values) { /*the mapper gives the reducer the sorted values by location, 
            	                                 then date. Therefore we use an iterator that searches 
            	                                 the values for each date of each location*/
            	 
        		  
            float temperature = value.get();         /*gets the first value of that date*/
            	 
            	 if (val1 !=0) {                   /*this if statement initially gets ignored because val1 has
            	                                      not been set yet*/ 
            		 val2 = temperature;                 /*after val 1 has been set the second value the iterator
            		                                       reads is set to val 2*/
            		 sum = Math.abs(val1 - val2);        /*Math.abs takes into consideration that the first value 
            		                                       is not always tMax, and just finds the difference between the two numbers*/
            		 sum = sum/10;                        /*dividing each value by 10 to get it into degree celcius*/
            		 context.write(key=null,new FloatWritable(sum)); /*the result is only written if there is a second value
            		                                                 which leaves out any days with missing values.
            		                                                 the key is set to null so that only the value is outputted,
            		                                                 an alternate way was with nullwriteable but i found this way
            		                                                 to be easier*/
            		 }
            	  val1 = temperature;       /*the first value is set during the first run through,
            	                              it will still reach this on the second run through,
            	                              however it does not matter because the iterator stops 
            	                              here and the sum has already been written to context */
            	  
        	   }
    }
}
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "map reduce");
    job.setJarByClass(mapReduce.class);    
    job.setMapperClass(TempMapper.class);     //sets which class is the mapper
    job.setReducerClass(TempReducer.class);    //sets which class is the reducer
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);      //sets which type the key is outputted in
    job.setOutputValueClass(FloatWritable.class);    //sets which type the value is outputted in
    job.setNumReduceTasks(1); /*the number of reduce tasks is set to 1
                               because when run on google cloud, by default
                               it produces 7 files. by setting number of reduce
                               tasks to 1, it outputs everything into 1 file*/

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
