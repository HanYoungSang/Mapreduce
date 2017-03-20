package com.bit2017.MapReduceEx;

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

public class WordCount {

	private static Log log = LogFactory.getLog(WordCount.class);
	public static class MyMapper extends Mapper<LongWritable, Text, Text, NumberWritable> {

		private Text word = new Text();
		private static NumberWritable one = new NumberWritable(1L); //내용이 변하지 않으므로
		
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			log.info("--------------->>>> Mapper setup() called");
			super.setup(context);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\r\n\t,./|()<>{} '\"");
			while( tokenizer.hasMoreTokens() ) {
				
				String word_ori = tokenizer.nextToken();
				word.set(word_ori);
				context.write(word, one);
			}
		}

		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			log.info("--------------->>>> Mapper cleanup() called");
			super.cleanup(context);
		}
	}

	public static class MyReducer extends Reducer<Text, NumberWritable, Text, LongWritable> {

		private LongWritable sumWritable = new LongWritable(); 

		@Override
		protected void setup(
				Reducer<Text, NumberWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			log.info("--------------->>>> Reducer setup() called");
			super.setup(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<NumberWritable> values, Reducer<Text, NumberWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			long sum = 0;
			for(NumberWritable value : values) {
				sum += value.get();
			}
			
			sumWritable.set(sum);
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
				Reducer<Text, NumberWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			log.info("--------------->>>> Reducer cleanup() called");
			super.cleanup(context);
		}

		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job( conf, "WordCount" );
		
		// 1. Job Instance를 가지고 초기화 작업
		job.setJarByClass( WordCount.class );
		
		// 2. 맵 클래스 지정
		job.setMapperClass( MyMapper.class );

		// 3. 리듀스 클래스 지정
		job.setReducerClass( MyReducer.class);
		
		// 4. 출력 키 타입
		job.setMapOutputKeyClass( Text.class );
		
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
