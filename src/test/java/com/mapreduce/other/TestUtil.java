package com.mapreduce.other;

import org.apache.commons.lang.StringEscapeUtils;
import org.junit.Test;

public class TestUtil {

    @Test
    public void testStringEscapeUtils() {
        String s = "<pre class=\"brush: java;\">";
        assert StringEscapeUtils.escapeHtml(s).equals("&lt;pre class=&quot;brush: java;&quot;&gt;");
        System.out.println("testStringEscapeUtils passed!");
    }
}
