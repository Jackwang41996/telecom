package Personas;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Tools.MProperties;

public class UserGoup {
    public static class mapper extends Mapper<LongWritable, Text, Text, Text>{
         //513047538993,001282000000000000,0,0,http://img.users.51.la/19155999.asp
    	//用户id 行为id 是否产品标识 预购类型 网址
    	Text k=new Text();
    	Text v=new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] line=value.toString().split(MProperties.getValue("outsplit"));
			if(!line[3].equals("0")){
			  k.set(line[0]);
			  v.set(line[3]);
			  context.write(k, v);
			}
		}  	
    }
	public static class reducer extends Reducer< Text, Text, NullWritable, Text>{
      //id  预购类型
		//id  预购类型 次数 
		Text v=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int n1=0;
			int n2=0;
			for (Text s:values){
			  int x=Integer.parseInt(s.toString());
			  if(x==1){
				n1++;
			  }
			  if(x==2){
				n2++;
			 }				
			}
			if(n1==0){
				v.set(key+"|"+2+":"+n2);
			}else if(n2==0)	{
				v.set(key+"|"+1+":"+n1);
			}else{
				v.set(key+"|"+1+":"+n1+","+2+":"+n2);
			}
			context.write(NullWritable.get(), v);
		}
		
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(Personas.class);
		job.setMapperClass(mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(reducer.class);
		//输出格式
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);							
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.245.20:9000/telecom/data2"));
		Path out=new Path("hdfs://192.168.245.20:9000/telecom/data10");			
		FileOutputFormat.setOutputPath(job, out);
		FileSystem fileSystem = out.getFileSystem(conf);       
		//getFileSystem()函数功能  Return the FileSystem that owns this Path.   
		    if (fileSystem.exists(out)) {  
		        fileSystem.delete(out,true);  
		    }  
		boolean res=job.waitForCompletion(true);

	}

}
