package com.mapreduce.ch2.simple;

import com.mapreduce.utils.MRDPUtils;
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
 * get the corresponding min and max value
 *
 * @author zhanglei
 * 2018.06.12
 */
public class MinMaxCount {

    public static class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {
        //output
        private Text outUserId = new Text();
        private MinMaxCountTuple outTuple = new MinMaxCountTuple();

        private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd'T'HH:mm:ss.SSS");

        public void map(Object key, Text value, Context context) {
            Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

            String strDate = parsed.get("CreationDate");

            String userId = parsed.get("UserId");

            if(strDate == null || userId == null) {
                return;
            }

            try {
                Date creationDate = sdf.parse(strDate);

                outTuple.setMin(creationDate);
                outTuple.setMax(creationDate);
                outTuple.setCount(1);

                outUserId.set(userId);

                context.write(outUserId, outTuple);
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
        private MinMaxCountTuple ret = new MinMaxCountTuple();

        public void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {
            ret.setMin(null);
            ret.setMax(null);
            int sum = 0;

            for(MinMaxCountTuple tuple : values) {
                if(ret.getMin() == null || tuple.getMin().compareTo(ret.getMin()) < 0) {
                    ret.setMin(tuple.getMin());
                }

                if(ret.getMax() == null || tuple.getMax().compareTo(ret.getMax()) > 0) {
                    ret.setMax(tuple.getMax());
                }

                sum += tuple.getCount();
            }
            ret.setCount(sum);
            context.write(key, ret);
        }
    }

    public static void main(String[] args) {

    }

    public static class MinMaxCountTuple implements Writable {
        private Date min = new Date();
        private Date max = new Date();
        private long count = 0;

        private final static SimpleDateFormat sdf= new SimpleDateFormat("yyyy-mm-dd'T'HH:mm:ss.SSS");

        public Date getMin() {
            return min;
        }

        public Date getMax() {
            return max;
        }

        public long getCount() {
            return count;
        }

        public void setMin(Date min) {
            this.min = min;
        }

        public void setMax(Date max) {
            this.max = max;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(min.getTime());
            dataOutput.writeLong(max.getTime());
            dataOutput.writeLong(count);
        }

        public void readFields(DataInput dataInput) throws IOException {
            min = new Date(dataInput.readLong());
            max = new Date(dataInput.readLong());
            count = dataInput.readLong();
        }

        @Override
        public String toString() {
            return sdf.format(min) + "\t" + sdf.format(max) + "\t" + count;
        }
    }
}
