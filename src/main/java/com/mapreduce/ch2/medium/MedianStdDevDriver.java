package com.mapreduce.ch2.medium;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * get the median number of many numbers
 *
 * @author zhanglei
 * 2018.06.12
 */
public class MedianStdDevDriver {

    public static class MedianStdMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable outHour = new IntWritable();
        private IntWritable outCommentLen = new IntWritable();

        private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd'T'HH:mm:ss.SSS");

        @SuppressWarnings("deprecation")
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = new HashMap<String, String>();

            String strDate = parsed.get("CreationDate");
            String text = parsed.get("Text");

            if(strDate == null || text == null) {
                return;
            }

            try {
                Date creationDate = sdf.parse(strDate);
                outHour.set(creationDate.getHours());
                outCommentLen.set(text.length());

                context.write(outHour, outCommentLen);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class MedianStdReducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevTuple> {
        private MedianStdDevTuple ret = new MedianStdDevTuple();
        private ArrayList<Float> commentLengths = new ArrayList<Float>();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            float cnt = 0;
            float sum = 0;
            for(IntWritable val : values) {
                float tmp = val.get();
                commentLengths.add(tmp);
                cnt++;
                sum += tmp;
            }
            Collections.sort(commentLengths);

            int mid = (int) (cnt/2);
            if(cnt % 2 == 0) {
                ret.setMedian((commentLengths.get(mid)+commentLengths.get(mid-1))/2);
            } else {
                ret.setMedian(commentLengths.get(mid));
            }

            //get average value
            float average = sum / cnt;
            float sumOfQuares = 0; //样本方差
            for(Float num : commentLengths) {
                sumOfQuares += (num-average)*(num-average);
            }
            sumOfQuares /= (cnt-1);
            ret.setStdDev((float) Math.sqrt(sumOfQuares));

            context.write(key, ret);
        }
    }

    public static class MedianStdDevTuple implements Writable{

        private float median = 0;//中间值
        private float stdDev = 0f;//标准差

        public float getMedian() {
            return median;
        }

        public void setMedian(float median) {
            this.median = median;
        }

        public float getStdDev() {
            return stdDev;
        }

        public void setStdDev(float stdDev) {
            this.stdDev = stdDev;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeFloat(median);
            dataOutput.writeFloat(stdDev);
        }

        public void readFields(DataInput dataInput) throws IOException {
            median = dataInput.readFloat();
            stdDev = dataInput.readFloat();
        }
    }
}
