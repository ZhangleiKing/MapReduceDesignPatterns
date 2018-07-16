package com.mapreduce.ch3;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Filtering基于某种条件评估每一条记录，决定它的去留
 * Filtering是MapReduce中唯一不需要reduce的
 *
 * DistributedGrep用于在大数据上进行分布式正则匹配，找到相应的符合条件的数据
 * 所有输出记录被写到本地文件系统
 *
 * @author zhanglei
 * 2018.06.14
 */
public class DistributedGrep {

  public static class DistributedGrepMapper extends Mapper<Object, Text, NullWritable, Text> {

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String text = value.toString();
      String mapRegex = context.getConfiguration().get("mapregex");

      if(text.matches(mapRegex)) {
        context.write(NullWritable.get(), value);
      }
    }
  }

  public static void main(String[] args) {

  }

}
