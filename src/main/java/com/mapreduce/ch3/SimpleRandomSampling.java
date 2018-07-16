package com.mapreduce.ch3;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimpleRandomSampling {

  public static class SimpleRandomSamplingMapper extends Mapper<Object, Text, NullWritable, Text> {

    private Random rands = new Random();

    private double percentage;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      String strPercentage = context.getConfiguration().get("filter_percentage");
      percentage = Double.parseDouble(strPercentage) / 100.0;
    }

    public void map(Object key, Text value ,Context context)
        throws IOException, InterruptedException {
      if(rands.nextDouble() < percentage) {
        context.write(NullWritable.get(), value);
      }
    }
  }

  public static void main(String[] args) {

  }
}
