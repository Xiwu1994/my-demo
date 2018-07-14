package indi.bigdata.demo;
/**
 * Created by xiwu on 18/6/12
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountMRDemo extends Configured {
    //在hadoop中，普通的java类不适合做网络序列化传输，hadoop对java的类型进行了封装，以便于利用hadoop的序列化框架进行序列化传输
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        /**
         * map方法是每读一行调用一次
         */
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {

            //拿到一行的内容
            String line = value.toString();
            //切分出一行中所有的单词
            String[] words = line.split(" ");
            //输出<word,1>这种KV对
            for(String word:words){
                //遍历单词数组，一对一对地输出<hello,1>  <tom,1> .......
                context.write(new Text(word), new LongWritable(1));
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        /**
         * reduce方法是每获得一个<key,valueList>,执行一次
         */
        //key ： 某一个单词 ，比如  hello
        //values：  这个单词的所有v，  封装在一个迭代器中，可以理解为一个list{1,1,1,1.....}
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,Context context)
                throws IOException, InterruptedException {
            long count = 0;
            //遍历该key的valuelist，将所有value累加到计数器中去
            for(LongWritable value:values){
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //先构造一个用来提交我们的业务程序的一个信息封装对象
        Job job = Job.getInstance(conf);
        //指定本job所采用的mapper类
        job.setMapperClass(WordCountMapper.class);
        //指定本job所采用的reducer类
        job.setReducerClass(WordCountReducer.class);
        //指定我们的mapper类输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //指定我们的reducer类输出的kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //指定我们要处理的文件所在的路径
        FileInputFormat.setInputPaths(job, new Path("/Users/liebaomac/demo_wordcount/input/"));
        //指定我们的输出结果文件所存放的路径
        FileOutputFormat.setOutputPath(job, new Path("/Users/liebaomac/demo_wordcount/output/"));
        int res = job.waitForCompletion(true)? 0:1;
        System.exit(res);
    }
}
