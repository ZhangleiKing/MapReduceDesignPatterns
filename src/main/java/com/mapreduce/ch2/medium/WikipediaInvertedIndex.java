package com.mapreduce.ch2.medium;

import com.mapreduce.utils.MRDPUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

/**
 * Get the list of ids which contain the key(url)
 *
 * @author zhanglei
 * 2018.06.13
 */
public class WikipediaInvertedIndex {

    public static class WikipediaExtractor extends Mapper<Object, Text, Text, Text> {
        private Text link = new Text();
        private Text outId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

            String text = parsed.get("Body");
            String postType = parsed.get("PostTypeId");
            String rowId = parsed.get("id");

            if(text == null || (postType != null && postType.equals("1"))) {
                return;
            }

            text = StringEscapeUtils.unescapeHtml(text.toLowerCase());
            link.set(getWikipediaUrl(text));
            outId.set(rowId);
            context.write(link, outId);
        }
    }

    public static class Concatenator extends Reducer<Text, Text, Text, Text>{
        private Text ret = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text val : values) {
                sb.append(val).append(" ");
            }
            ret.set(sb.substring(0, sb.length()-1));
            context.write(key, ret);
        }
    }

    private static String getWikipediaUrl(String text) {
        int idx = text.indexOf("\"http://en.wikipedia.org");
        if(idx == -1) {
            return null;
        }
        int idx_end = text.indexOf("\"", idx+1);
        if(idx_end == -1) {
            return null;
        }
        int idx_hash = text.indexOf('#', idx+1);
        if(idx_hash != -1 && idx_hash < idx_end) {
            return text.substring(idx+1, idx_hash);
        } else {
            return text.substring(idx+1, idx_end);
        }
    }
}
