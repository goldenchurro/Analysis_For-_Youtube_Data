import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Youtubetopuploader {
   public static class Map extends Mapper<LongWritable,Text,Text,InWritable> {
     private Text uploader = new Text();
     private final static IntWritable occurance = new InWritable(1);
     publlic void map(LongWritable key, Text value,
                      Context context) throws IOException, InterruptedException {
        
public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> 
{
   
   public void reduce()
      
   }
        
 public static void main()
       
     
