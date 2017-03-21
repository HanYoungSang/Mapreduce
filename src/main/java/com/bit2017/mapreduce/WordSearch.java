package com.bit2017.mapreduce;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.bit2017.mapreduce.io.NumberWritable;

public class WordSearch {

	private static Log log = LogFactory.getLog(WordCount.class);
	public static String searchText;
	
	
	public static class MyMapper extends Mapper<LongWritable, Text, StringWritable, NumberWritable> {

		private StringWritable word = new StringWritable();
		private static NumberWritable one = new NumberWritable(1L); //내용이 변하지 않으므로
		
		@Override
		protected void setup(
				Mapper<LongWritable, Text, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			log.info("--------------->>>> Mapper setup() called");
			super.setup(context);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			
//			log.info("============= map() search text is " + searchText);
			
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\r\n\t,./|()<>{} '\"");
			while( tokenizer.hasMoreTokens() ) {
				
				String word_ori = tokenizer.nextToken();
//				log.info("============= map() word_ori.contains(searchText ) is " + word_ori.contains(searchText ));
				log.info("============= map() word_ori is " + word_ori);
				
				if ( word_ori.contains("Hadoop" ) ) {
					
					word.set(word_ori);
					context.write(word, one);	
				}
				
			}
		}

		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			log.info("--------------->>>> Mapper cleanup() called");
			super.cleanup(context);
		}
	}

	public static class MyReducer extends Reducer<StringWritable, NumberWritable, StringWritable, NumberWritable> {

		private NumberWritable sumWritable = new NumberWritable(); 

		@Override
		protected void setup(
				Reducer<StringWritable, NumberWritable, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			log.info("--------------->>>> Reducer setup() called");
			super.setup(context);
		}
		
		@Override
		protected void reduce(StringWritable key, Iterable<NumberWritable> values, Reducer<StringWritable, NumberWritable, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			long sum = 0;

			for(NumberWritable value : values) {
				sum += value.get();
			}
			
//			for (Iterator<NumberWritable> iterator = values.iterator(); iterator.hasNext();) {
//				distinctSum+= iterator.next()..get();
//	         }
			
			
			sumWritable.set(sum);
			
			context.getCounter("Word Status", "Count of all Words").increment( sum );
			
			context.getCounter("Word Status", "Count of distinct Words").increment( 1L );
			
			context.write(key, sumWritable);
		}
		
		
// 		run은 보통 Override 하지 않는다.		
//		@Override
//		public void run(
//				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
//				throws IOException, InterruptedException {
//			// TODO Auto-generated method stub
//			log.info("--------------->>>> Reducer run() called");
//			super.run(context);
//		}
		
		@Override
		protected void cleanup(
				Reducer<StringWritable, NumberWritable, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			log.info("--------------->>>> Reducer cleanup() called");
			super.cleanup(context);
		}


		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job( conf, "WordCount" );
		
		searchText = args[2];
		log.info("============= search text is " + searchText);
		
		
		// 1. Job Instance를 가지고 초기화 작업
		job.setJarByClass( WordCount.class );
		
		// 2. 맵 클래스 지정
		job.setMapperClass( MyMapper.class );

		// 3. 리듀스 클래스 지정
		job.setReducerClass( Reducer.class);
		
		// 리듀스 태스크 수 
		job.setNumReduceTasks(2);

		
		// 4. 출력 키 타입
		job.setMapOutputKeyClass( StringWritable.class );
		
		// 5. 출력 밸류 타입
		job.setMapOutputValueClass( NumberWritable.class );
		
		// 6. 입력 파일 포멧 지정 ( 생략 가능 )
		job.setInputFormatClass( TextInputFormat.class );
		// 7. 출력 파일 포멧 지정 ( 생략 가능 )
		job.setOutputFormatClass( TextOutputFormat.class );
		
		// 8. 입력 파일 위치 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		// 9. 출력 파일 위치 지정
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 10. 실행
		job.waitForCompletion(true);

		
	}
}
