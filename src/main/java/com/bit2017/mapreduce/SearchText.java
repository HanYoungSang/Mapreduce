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

public class SearchText {

	private static Log log = LogFactory.getLog(WordCount.class);
	private static String searchText = "";
	
	public static class MyMapper extends Mapper<LongWritable, Text, StringWritable, NumberWritable> {

		private StringWritable word = new StringWritable();
		private static NumberWritable one = new NumberWritable(1L); //내용이 변하지 않으므로
		private static CharSequence charSearchText = searchText;

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			
			log.info("============= map() search text is " + charSearchText.toString());
			
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\n");
			while( tokenizer.hasMoreTokens() ) {
				
				String word_ori = tokenizer.nextToken();
//				log.info("============= map() word_ori.contains(searchText ) is " + word_ori.contains(searchText ));
				log.info("============= map() word_ori is " + word_ori);
				
				
				if ( word_ori.contains(charSearchText ) ) {
					
					word.set(word_ori);
					context.write(word, one);	
				}
				
			}
		}

	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job( conf, "WordCount" );

		
//		searchText = new String(args[2]);
//		System.err.println(args[2]);
//		log.info("============= search text is " + searchText);
		
		
		// 1. Job Instance를 가지고 초기화 작업
		job.setJarByClass( SearchText.class );
		
		// 2. 맵 클래스 지정
		job.setMapperClass( MyMapper.class );

		log.info("============= search text is " + args[2]);
		MyMapper.charSearchText = args[2];

		
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
