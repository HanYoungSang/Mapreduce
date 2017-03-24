package com.bit2017.mapreduce.index;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InventedIndex1 {

//	private static Log log = LogFactory.getLog(InventedIndex1.class);

	public static class MyMapper extends Mapper<Text, Text, Text, Text> {

		private Text word = new Text();
		
		@Override
		protected void map(Text docId, Text contents, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String line = contents.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\r\n\t,/|()<>{} '\"");
			while( tokenizer.hasMoreTokens() ) {
				word.set(tokenizer.nextToken().toLowerCase());
				context.write(word, docId);

			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(Text word, Iterable<Text> docIds, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuilder s = new StringBuilder();
			boolean isFirst = true;
			for(Text docId : docIds) {
				if ( isFirst == false ){
					s.append(",");
				} else {
					isFirst = false;
				}
				s.append(docId.toString());
			}

			context.write(word, new Text(s.toString()));
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job( conf, "WordCount" );

		//  Job Instance를 가지고 초기화 작업
		job.setJarByClass( InventedIndex1.class );

		//  맵 클래스 지정
		job.setMapperClass( MyMapper.class );

		//  리듀스 클래스 지정
		job.setReducerClass( MyReducer.class);

		// 리듀스 개수 지정
		job.setNumReduceTasks( 10 );
		
		//  맵 출력 키 타입
		job.setMapOutputKeyClass( Text.class );

		//  맵 출력 밸류 타입
		job.setMapOutputValueClass( Text.class );

		//  리듀스 출력 키 타입
		job.setOutputKeyClass( Text.class );

		//  리듀스 출력 밸류 타입
		job.setOutputValueClass( Text.class );

		
		//  입력 파일 포멧 지정 ( 생략 가능 )
		job.setInputFormatClass( KeyValueTextInputFormat.class );
		//  출력 파일 포멧 지정 ( 생략 가능 )
		job.setOutputFormatClass( TextOutputFormat.class );

		//  입력 파일 위치 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));

		//  출력 파일 위치 지정
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//  실행
		job.waitForCompletion(true);

	}
}
