

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> 
{ 
	private IntWritable result = new IntWritable(); 
	static int i = 10;
	@Override public void reduce(Text key, Iterable<IntWritable> values, Context context ) 
			throws IOException, InterruptedException{
		
		if(i>0) {
			
			HashMap<Integer, Integer> wordMap = new HashMap<Integer, Integer>(); 
			for(IntWritable chap : values) {
				
				if(wordMap.get(chap.get())==null)
					wordMap.put(chap.get(), 1);
				
				else wordMap.put(chap.get(), wordMap.get(chap.get())+1);
				
			}
			
			if(wordMap.size() == 12) {
				boolean flag = true; 
				for(int count : wordMap.values())
				{
					if(count < 3) {
						
						flag = false; break; 
					} 
				}
				
				if(flag){
					i--;
					for(Entry<Integer, Integer> ChapterNumber : wordMap.entrySet()) {
						
						StringBuffer sb = new StringBuffer(key.toString());
						sb.append(" => Chapter Number "+ChapterNumber.getKey());
						result.set(ChapterNumber.getValue()); 
						context.write(new Text(sb.toString()), result); 
					}
				}
				/*
				int total = 0;
				for(int count : wordMap.values())
				{
					total +=count;
				}
				if(total >50)
				{
					result.set(total);
					context.write(key, result);
				}*/
			}
			wordMap.clear();
		}
		//System.out.println("Current i = "+ i);
	}
}