package com.bit2017.mapreduce.sorting;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class StringSort {

	private static Log log = LogFactory.getLog(StringSort.class);

	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		private Text word = new Text();
		private static LongWritable one = new LongWritable(1L); //내용이 변하지 않으므로

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			
			log.info("--------------->>>> MyMapper map() called");
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\r\n\t,./|()<>{} '\"");
			while( tokenizer.hasMoreTokens() ) {
				
				String word_ori = tokenizer.nextToken();

				word.set(word_ori);
				context.write(word, one);

			}
		}
	}

	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		private LongWritable sumWritable = new LongWritable(); 
	
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			
			for(LongWritable value : values) {
				sum += value.get();
			}
			
//			for (Iterator<LongWritable> iterator = values.iterator(); iterator.hasNext();) {
//				distinctSum+= iterator.next().get();
//	         }
			
			sumWritable.set(sum);
			context.write(key, sumWritable);
		}

	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job( conf, "String Sort" );

		// Job Instance를 가지고 초기화 작업
		job.setJarByClass( StringSort.class );
		
		// 맵 클래스 지정
		job.setMapperClass( MyMapper.class );

		// 리듀스 클래스 지정
		job.setReducerClass( MyReducer.class);
		
		// 맵퍼 출력 키 타입
		job.setMapOutputKeyClass( Text.class );
		
		// 맵퍼 출력 밸류 타입
		job.setMapOutputValueClass( Text.class );
		
		// 처리 결과 출력 키 타입(리듀스)
		job.setOutputKeyClass( Text.class );
		
		//처리 결과 출력 밸류 타입(리듀스)
		job.setOutputValueClass( Text.class );
				
		
		// 입력 파일 포멧 지정 ( 생략 가능 )
		job.setInputFormatClass( KeyValueTextInputFormat.class );
		// 출력 파일 포멧 지정 ( 생략 가능 )
		job.setOutputFormatClass( SequenceFileOutputFormat.class );
		
		// 입력 파일 위치 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		// 출력 파일 위치 지정
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		
		
		// 실행
		job.waitForCompletion(true);

		
	}
}
