

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException; 
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.util.ArrayList;
import java.util.Scanner; 

@SuppressWarnings("unused")
public class Map extends Mapper<Object, Text, Text, IntWritable>
{
	private static int ChapterNumber = 0; 
	String curr;
	private Text word = new Text();
	private static boolean flag = false; 
	
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException { 
		
		if(value.toString().contains("CHAPTER")){ 
			flag = true; 
			ChapterNumber++; 
		} 
		
		else if(value.toString().contains("THE")) { 
			flag = false; 
		}
		
		if(flag) { 
			curr = value.toString(); 
			rmPunt(); 
			rmStopWords();
			StringTokenizer itr = new StringTokenizer(curr);
			
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, new IntWritable(ChapterNumber)); 		
			} 	
		}
	}
		
	public void rmPunt() {
			
		curr = curr.replaceAll("[^a-zA-Z\\s+]", " ").toLowerCase();
		curr = curr.trim();
			
		}
	
	public void rmStopWords() throws FileNotFoundException { 
		
		ArrayList<String> stopWords = new ArrayList<String>();
		Scanner inputfile;
		
		File txt_file = new File("/home/biadmin/hw1_wordCount/stopwords.txt"); 
		inputfile = new Scanner (txt_file); 
		
		while(inputfile.hasNext()) {
			String line = inputfile.next(); 
			stopWords.add(line); 
		} 
		
		inputfile.close();
		
		String formattedStr = "";
		for(String word: curr.split("[\\s\\n\\t]")) { 
			
			if(!stopWords.contains(word)) { 
				formattedStr = formattedStr+" "+word; } 
			} 
		curr = formattedStr.trim(); 
	} 
	
	
	
}