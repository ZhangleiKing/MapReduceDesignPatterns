package com.mapreduce.ch2.simple;

import com.mapreduce.utils.MRDPUtils;
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
import java.util.Date;
import java.util.Map;

/**
 * get the corresponding average value
 *
 * @author zhanglei
 * 2018.06.12
 */
public class AverageDriver {

    public static class AverageDriverMapper extends Mapper<Object, Text, IntWritable, CountAverageTuple> {
        private IntWritable outHour = new IntWritable();
        private CountAverageTuple outTuple = new CountAverageTuple();

        private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd'T'HH:mm:ss.SSS");

        @SuppressWarnings("deprecation")
        @Override
        public void map(Object key, Text value, Context context) {
            Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

            String strDate = parsed.get("CreationDate");

            String text = parsed.get("Text");

            if(strDate == null || text == null) {
                return;
            }

            try {
                Date creationDate = sdf.parse(strDate);
                outHour.set(creationDate.getHours());

                outTuple.setCount(1);
                outTuple.setAverageLen(text.length());

                context.write(outHour, outTuple);
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class AverageDriverReducer extends Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple> {
        private CountAverageTuple ret = new CountAverageTuple();

        public void reduce(IntWritable key, Iterable<CountAverageTuple> values, Context context) throws IOException, InterruptedException {
            float sumLen = 0;
            float sumCnt = 0;
            for(CountAverageTuple tuple : values) {
                sumCnt += tuple.getCount();
                sumLen += tuple.getAverageLen()*tuple.getCount();
            }
            ret.setCount(sumCnt);
            ret.setAverageLen(sumLen/sumCnt);
            context.write(key, ret);
        }
    }

    public static void main(String[] args) {

    }

    public static class CountAverageTuple implements Writable {

        private float count = 0f;
        private float averageLen = 0f;

        public float getCount() {
            return count;
        }

        public void setCount(float count) {
            this.count = count;
        }

        public float getAverageLen() {
            return averageLen;
        }

        public void setAverageLen(float averageLen) {
            this.averageLen = averageLen;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeFloat(count);
            dataOutput.writeFloat(averageLen);
        }

        public void readFields(DataInput dataInput) throws IOException {
            count = dataInput.readFloat();
            averageLen = dataInput.readFloat();
        }

        @Override
        public String toString() {
            return count + "\t" + averageLen;
        }
    }
}
