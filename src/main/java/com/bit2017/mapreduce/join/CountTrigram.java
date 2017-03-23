package com.bit2017.mapreduce.join;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.bit2017.mapreduce.topn.TopN;

public class CountTrigram {

	private static Log log = LogFactory.getLog(CountTrigram.class);

	public static class MyMapper extends Mapper<Text, Text, Text, LongWritable>{

		@Override
		protected void map(Text key, Text value,
				Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			
			Text result = new Text();
			String str = value.toString();
			String[] word = new String[3];
			int count = 0;
			
			StringTokenizer tokens = new StringTokenizer(str, "\r\n\t,./|()<>{} '\"");
			while(tokens.hasMoreTokens()){
				word[count] = tokens.nextToken().toString();
				if( count < 2 ) {
					count++;
				} else if( count == 2 ) {
					result.set(word[0] + " " + word[1] + " " + word[2]);
					context.write(result, new LongWritable(1L));
					word[0] = word[1];
					word[1] = word[2];
				}
			}

		}

	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum = 0;

			for (LongWritable value : values) {
				sum += value.get();
			}

			context.write(key, new LongWritable(sum));
		}

	}
	
	public static void main (String args[]) throws Exception{
		Configuration conf = new Configuration();
	    Job job = new Job( conf, "Count Trigram" );
	   
	    job.setJarByClass( CountTrigram.class ); 
	    job.setOutputKeyClass( Text.class );
	    job.setOutputValueClass( LongWritable.class );
	        
	    job.setMapperClass( MyMapper.class );
	    job.setReducerClass( MyReducer.class );

	    job.setInputFormatClass( KeyValueTextInputFormat.class );
	    job.setOutputFormatClass( TextOutputFormat.class );
	        
	    FileInputFormat.addInputPath( job, new Path( args[0] ) );
	    FileOutputFormat.setOutputPath( job, new Path( args[1] ) );
	        
	    if ( job.waitForCompletion( true ) == false ) {
	        return;
	    }

	    Configuration conf2 = new Configuration();
	    Job job2 = new Job(conf2, "Top N");

	    job2.setJarByClass( TopN.class );
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass( LongWritable.class );

	    job2.setMapperClass( TopN.MyMapper.class );
	    job2.setReducerClass( TopN.MyReducer.class );

	    job2.setInputFormatClass(KeyValueTextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);

	    // input of Job2 is output of Job
	    FileInputFormat.addInputPath(job2, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job2, new Path( args[1] + "/topN" ));
	    job2.getConfiguration().setInt( "topN", 10 );

	    job2.waitForCompletion(true);

	}
	

}
