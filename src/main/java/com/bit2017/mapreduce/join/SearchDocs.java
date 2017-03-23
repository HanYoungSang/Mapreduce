package com.bit2017.mapreduce.join;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.bit2017.mapreduce.topn.ItemFreq;

public class SearchDocs {

	private static Log log = LogFactory.getLog(SearchDocs.class);

	public static class DocIdContentsMapper
			extends
				Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value,
				Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String str = value.toString();
			String findStr = context.getConfiguration().get("SearchWord");
			
			//TOP N 용
			int strCount = context.getConfiguration().getInt("Count", 10);
			
			int idx = 0;
			int count = 0;
			while( idx != -1) {
				idx = str.indexOf(findStr, idx);
				if (idx != -1){
					idx += findStr.length();
					count ++;
					}
			}
			// 검색어가 몇개 있는지를 구해서 <ID, 갯수>로 넘겨준다.
			context.write(key, new Text( count + "\t" + 1 ));

		}

	}
	public static class TitleDocIdMapper
			extends
				Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value,
				Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			//Title ID 순서로 들어가 있는 것을 <ID, Title>로 들어가도록 바꿔 넘겨준다.
			context.write(value, new Text( key.toString() + "\t" + 2 ));	

		}

	}

	public static class JobTitleIdTopNReducer
			extends
				Reducer<Text, Text, Text, LongWritable> {
		
		private int topN = 10;
		private PriorityQueue<ItemFreq> pq = null;
		
//		@Override
//		protected void setup(
//				Reducer<Text, Text, Text, LongWritable>.Context context)
//				throws IOException, InterruptedException {
//			topN = context.getConfiguration().getInt("topN", 10);
//			pq = new PriorityQueue<ItemFreq>(10, new ItemFreqComparator());
//		}
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			
			int tokenCount = 0;
			
			// 출력용 Text 변수
			Text k = new Text();
			LongWritable v = new LongWritable();
			
			
			for (Text value : values) {
				String info = value.toString();
				String[] tokens = info.split("\t");
				if(tokens.length != 2) {
					log.info("============== tokens.length != 2 " );
//					break;
				}
				
				//Doc ID와 검색어 카운트
				if("1".equals( tokens[1] ) ) {
					v.set(Long.valueOf( tokens[0].toString()) );
					
				// Doc ID와 제목
				} else if ("2".equals( tokens[1] ) ){
					k.set( tokens[0] + "[" + key.toString() + "]");
					
				} else {
					continue;
				}
				
				tokenCount++;
			}
			
			// 출력
			if( tokenCount != 2 ) {
				return;
			}
			context.write(k, v);
		}

		@Override
		protected void cleanup(
				Reducer<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		

	}

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "SearchDocs");

		job.setJarByClass(SearchDocs.class);

		// 파라미터 저장
		final String TITLE_DOCID    = "/input/2M.TITLE.ID";
		final String DOCID_CONTENTS = "/input/10K.ID.CONTENTS";
		final String OUTPUT_DIR     = "/output/searchdocs/"+args[0];

		job.getConfiguration().setStrings("SearchWord", args[0]);
		job.getConfiguration().setInt("TopN", Integer.valueOf(args[1]) );
		
		//////////////////////////////
		/* 입력 */
		//////////////////////////////

		MultipleInputs.addInputPath(job, new Path(TITLE_DOCID),
				KeyValueTextInputFormat.class, TitleDocIdMapper.class);
		MultipleInputs.addInputPath(job, new Path(DOCID_CONTENTS),
				KeyValueTextInputFormat.class, DocIdContentsMapper.class);

		//////////////////////////////
		/* 출력 */
		//////////////////////////////

		// 리듀스 클래스 지정
		job.setReducerClass(JobTitleIdTopNReducer.class);
		// 출력 키 타입
		job.setMapOutputKeyClass(Text.class);
		// 출력 밸류 타입
		job.setMapOutputValueClass(Text.class);
		// 출력 파일 포멧 지정 ( 생략 가능 )
		job.setOutputFormatClass(TextOutputFormat.class);
		// 출력 파일 위치 지정
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR));

		//////////////////////////////
		/* 실행 */
		//////////////////////////////
		job.waitForCompletion(true);

	}
}
