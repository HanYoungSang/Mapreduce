package bit2017.com.MapReduceEx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	}

	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job( conf, "WordCount" );
		
		// 1. Job Instance를 가지고 초기화 작업
		
		// 2. 맵 클래스 지정
		// 3. 리듀스 클래스 지정
		
		// 4. 입력 파일 위치 지정
		// 5. 풀력 파일 위치 지정
		
		
		// 6. 실행
		job.waitForCompletion(true);

		
	}
}
