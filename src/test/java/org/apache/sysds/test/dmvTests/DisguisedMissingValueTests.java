package org.apache.sysds.test.dmvTests;

import org.apache.commons.collections.map.HashedMap;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.sysds.runtime.util.UtilFunctions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class DisguisedMissingValueTests {

    // Just to test our functions directly

    @Test
    public void TestUpperLowerFunction() {
        String s = UtilFunctions.removeUpperLowerCase("u3l5d5");
        Assert.assertEquals(s, "a3a5d5");
    }

    @Test
    public void TestCalculationRatioAndDominantPatternFunction1() {

        Map<String, Integer> pattern_hist = new HashedMap();
        pattern_hist.put("d4u3", 40);
        pattern_hist.put("u3", 10);
        pattern_hist.put("l1sd2", 50);
        Map<String, Double> dominant_patterns = UtilFunctions.calculatePatternsRatio(pattern_hist, 100);

        Double value1 = dominant_patterns.get("d4u3");
        Assert.assertEquals(value1, (Double)0.4);
        Double value2 = dominant_patterns.get("u3");
        Assert.assertEquals(value2, (Double)0.1);
        Double value3 = dominant_patterns.get("l1sd2");
        Assert.assertEquals(value3, (Double)0.5);

        String s = UtilFunctions.findDominantPattern(dominant_patterns, 0.7);
        Assert.assertEquals(s, null); // must be 0 because no dominant pattern found
    }

    @Test
    public void TestCalculationRatioAndDominantPatternFunction2() {

        Map<String, Integer> pattern_hist = new HashedMap();
        pattern_hist.put("d3", 15);
        pattern_hist.put("l3d1", 3);
        pattern_hist.put("l2d1", 2);
        pattern_hist.put("l1d1", 4);
        pattern_hist.put("d4", 971);
        pattern_hist.put("d1l1", 3);
        pattern_hist.put("d1l2", 2);
        Map<String, Double> dominant_patterns = UtilFunctions.calculatePatternsRatio(pattern_hist, 1000);

        Double value1 = dominant_patterns.get("d4");
        Assert.assertEquals((Double)0.971, value1);
        Double value2 = dominant_patterns.get("d3");
        Assert.assertEquals((Double)0.015, value2);
        Double value3 = dominant_patterns.get("l3d1");
        Assert.assertEquals((Double)0.003, value3);
        Double value4 = dominant_patterns.get("l2d1");
        Assert.assertEquals((Double)0.002, value4);
        Double value5 = dominant_patterns.get("l1d1");
        Assert.assertEquals((Double)0.004, value5);
        Double value6 = dominant_patterns.get("d1l1");
        Assert.assertEquals((Double)0.003, value6);
        Double value7 = dominant_patterns.get("d1l2");
        Assert.assertEquals((Double)0.002, value7);

        String s = UtilFunctions.findDominantPattern(dominant_patterns,  0.7);
        Assert.assertEquals(s, "d4");
    }

    //-----------------------------------------------------------------------------------------------
    // TESTING LEVELS
    @Test
    public void TestLevel1Function() {

        Map<String, Integer> values = new HashedMap();
        values.put("kerschbaumer", 15);
        values.put("edelsbrunner", 3);
        values.put("27", 2);
        values.put("L.44", 4);
        values.put("3.7", 971);
        values.put("456.David", 77);
        values.put("Patrick Lovric", 64);

        Map<String, Integer> l1_pattern_hist = UtilFunctions.LevelsExecutor(values, UtilFunctions.LEVEL_ENUM.LEVEL1);

        Integer value1 = l1_pattern_hist.get("l12");
        Assert.assertEquals((Integer)(15 + 3), value1);
        Integer value2 = l1_pattern_hist.get("d2");
        Assert.assertEquals((Integer)(2), value2);
        Integer value3 = l1_pattern_hist.get("u1t1d2");
        Assert.assertEquals((Integer)(4), value3);

        Integer value4 = l1_pattern_hist.get("d1t1d1");
        Assert.assertEquals((Integer)(971), value4);
        Integer value5 = l1_pattern_hist.get("d3t1u1l4");
        Assert.assertEquals((Integer)(77), value5);
        Integer value6 = l1_pattern_hist.get("u1l6s1u1l5");
        Assert.assertEquals((Integer)(64), value6);


    }

    @Test
    public void TestLevel2Function() {
        Map<String, Integer> pattern_hist = new HashedMap();
        pattern_hist.put("d3", 15);
        pattern_hist.put("l3d1", 3);
        pattern_hist.put("l2d1", 2);
        pattern_hist.put("l1d1", 4);
        pattern_hist.put("d4", 971);
        pattern_hist.put("d1l1", 3);
        pattern_hist.put("d1l2", 2);

        Map<String, Integer> l2_pattern_hist = UtilFunctions.LevelsExecutor(pattern_hist, UtilFunctions.LEVEL_ENUM.LEVEL2);

        Integer value1 = l2_pattern_hist.get("d+");
        Assert.assertEquals((Integer)(15 + 971), value1);
        Integer value2 = l2_pattern_hist.get("l+d+");
        Assert.assertEquals((Integer)(3 + 2 + 4), value2);
        Integer value3 = l2_pattern_hist.get("d+l+");
        Assert.assertEquals((Integer)(3 + 2), value3);

    }

    @Test
    public void TestLevel3Function() {
        Map<String, Integer> pattern_hist = new HashedMap();
        pattern_hist.put("d+", 15 + 971);
        pattern_hist.put("l+d+", 3 + 2 + 4);
        pattern_hist.put("d+l+", 2);
        pattern_hist.put("d+u+l+", 1);
        pattern_hist.put("d+su+l+tu+", 2);

        Map<String, Integer> l3_pattern_hist = UtilFunctions.LevelsExecutor(pattern_hist, UtilFunctions.LEVEL_ENUM.LEVEL3);

        Integer value1 = l3_pattern_hist.get("d+");
        Assert.assertEquals((Integer)(15 + 971), value1);
        Integer value2 = l3_pattern_hist.get("a+d+");
        Assert.assertEquals((Integer)(3 + 2 + 4), value2);
        Integer value3 = l3_pattern_hist.get("d+a+");
        Assert.assertEquals((Integer)(2 + 1), value3);
        Integer value4 = l3_pattern_hist.get("d+sa+ta+");
        Assert.assertEquals((Integer)(2), value4);
    }

    @Test
    public void TestLevel4Function() {
        Map<String, Integer> pattern_hist = new HashedMap();
        pattern_hist.put("d+", 15);
        pattern_hist.put("d+td+", 30);

        pattern_hist.put("a+d+", 20);
        pattern_hist.put("a+d+td+", 35);

        pattern_hist.put("d+a+", 25);
        pattern_hist.put("d+td+a+", 40);

        Map<String, Integer> l4_pattern_hist = UtilFunctions.LevelsExecutor(pattern_hist, UtilFunctions.LEVEL_ENUM.LEVEL4);


        Integer value1 = l4_pattern_hist.get("d+");
        Assert.assertEquals((Integer)(15 + 30), value1);

        Integer value2 = l4_pattern_hist.get("a+d+");
        Assert.assertEquals((Integer)(20 + 35), value2);

        Integer value5 = l4_pattern_hist.get("d+a+");
        Assert.assertEquals((Integer)(40 + 25), value5);

    }

    @Test
    public void TestLevel5Function() {
        Map<String, Integer> pattern_hist = new HashedMap();
        pattern_hist.put("d+", 15);
        pattern_hist.put("d+s", 30);
        pattern_hist.put("sd+", 22);

        pattern_hist.put("sa+", 20);
        pattern_hist.put("a+s", 35);
        pattern_hist.put("a+sa+", 99);

        pattern_hist.put("d+sa+", 25);
        pattern_hist.put("sd+a+", 40);
        pattern_hist.put("d+a+s", 76);

        Map<String, Integer> l5_pattern_hist = UtilFunctions.LevelsExecutor(pattern_hist, UtilFunctions.LEVEL_ENUM.LEVEL5);

        Integer value1 = l5_pattern_hist.get("d+");
        Assert.assertEquals((Integer)(15 + 30 + 22), value1);

        Integer value2 = l5_pattern_hist.get("a+");
        Assert.assertEquals((Integer)(20 + 35 + 99), value2);

        Integer value5 = l5_pattern_hist.get("d+a+");
        Assert.assertEquals((Integer)(40 + 25 + 76), value5);

    }

    @Test
    public void TestLevel6Function() {
        Map<String, Integer> pattern_hist = new HashedMap();
        pattern_hist.put("-d+", 15);
        pattern_hist.put("d+", 12);

        pattern_hist.put("a+", 20);

        pattern_hist.put("-d+a+", 25);
        pattern_hist.put("d+a+", 17);

        pattern_hist.put("a+-d+", 21);
        pattern_hist.put("a+d+", 5);


        Map<String, Integer> l6_pattern_hist = UtilFunctions.LevelsExecutor(pattern_hist, UtilFunctions.LEVEL_ENUM.LEVEL6);

        Integer value1 = l6_pattern_hist.get("d+");
        Assert.assertEquals((Integer)(15 + 12), value1);

        Integer value2 = l6_pattern_hist.get("a+");
        Assert.assertEquals((Integer)(20), value2);

        Integer value3 = l6_pattern_hist.get("d+a+");
        Assert.assertEquals((Integer)(25 + 17), value3);

        Integer value4 = l6_pattern_hist.get("a+d+");
        Assert.assertEquals((Integer)(21 + 5), value4);
    }

    @Test
    public void TestAllLevels1() {

        String[] testarray0 = new String[]{"77","77","55","89","43", "Patrick-Lovric-Weg-666", "46"}; // detect Weg
        String[] testarray1 = new String[]{"8010","?","8456","4565","89655", "86542", "45624"}; // detect ?
        String[] testarray2 = new String[]{"David K","Valentin E","Patrick L","45","DK", "VE", "PL"}; // detect 45
        String[] testarray3 = new String[]{"3.42","45","0.456",".45","4589.245", "97", "ka"}; // detect ka
        String[] testarray4 = new String[]{"9999","123","158","146","158", "174", "201"}; // detect 9999
        String[] testarray5 = new String[]{"Kerschbaumer","123","258d","80105","Valentin-E-Weg 23c", ".035", "helloWorld"}; // detect nothing
        String[] testarray6 = new String[]{"77","-23","78","-21","43", "-44", "43"}; // detect nothing


        FrameBlock f = new FrameBlock();
        f.appendColumn(testarray0.clone());
        f.appendColumn(testarray1.clone());
        f.appendColumn(testarray2.clone());
        f.appendColumn(testarray3.clone());
        f.appendColumn(testarray4.clone());
        f.appendColumn(testarray5.clone());
        f.appendColumn(testarray6.clone());

        FrameBlock new_frame = UtilFunctions.syntacticalPatternDiscovery(f, 0.7, "NA");

        // testarray0
        Object c = new_frame.getColumnData(0);
        String[] column_s = (String[]) c;
        for(int i = 0; i < testarray0.length; i++) {
            if(testarray0[i].equals("Patrick-Lovric-Weg-666"))
                Assert.assertEquals(UtilFunctions.DISGUISED_VAL, column_s[i]);
            else
                Assert.assertEquals(testarray0[i], column_s[i]);
        }

        // testarray1
        c = new_frame.getColumnData(1);
        column_s = (String[]) c;
        for(int i = 0; i < testarray1.length; i++) {
            if(testarray1[i].equals("?"))
                Assert.assertEquals(UtilFunctions.DISGUISED_VAL, column_s[i]);
            else
                Assert.assertEquals(testarray1[i], column_s[i]);
        }

        // testarray2
        c = new_frame.getColumnData(2);
        column_s = (String[]) c;
        for(int i = 0; i < testarray2.length; i++) {
            if(testarray2[i].equals("45"))
                Assert.assertEquals(UtilFunctions.DISGUISED_VAL, column_s[i]);
            else
                Assert.assertEquals(testarray2[i], column_s[i]);
        }

        // testarray3
        c = new_frame.getColumnData(3);
        column_s = (String[]) c;
        for(int i = 0; i < testarray3.length; i++) {
            if(testarray3[i].equals("ka"))
                Assert.assertEquals(UtilFunctions.DISGUISED_VAL, column_s[i]);
            else
                Assert.assertEquals(testarray3[i], column_s[i]);
        }

        // testarray4
        c = new_frame.getColumnData(4);
        column_s = (String[]) c;
        for(int i = 0; i < testarray4.length; i++) {
            if(testarray4[i].equals("9999"))
                Assert.assertEquals(UtilFunctions.DISGUISED_VAL, column_s[i]);
            else
                Assert.assertEquals(testarray4[i], column_s[i]);
        }

        // testarray5 detect nothing
        c = new_frame.getColumnData(5);
        column_s = (String[]) c;
        for(int i = 0; i < testarray5.length; i++) {
                Assert.assertEquals(testarray5[i], column_s[i]);
        }

        // testarray6
        c = new_frame.getColumnData(6);
        column_s = (String[]) c;
        for(int i = 0; i < testarray6.length; i++) {
            Assert.assertEquals(testarray6[i], column_s[i]);
        }

    }

    //-----------------------------------------------------------------------------------------------
    // TESTING FUNCTIONS

    @Test
    public void TestNegativNumbers() {

        String res = UtilFunctions.acceptNegativeNumbersAsDigits("-d+");
        Assert.assertEquals("d+", res);
        res = UtilFunctions.acceptNegativeNumbersAsDigits("d+");
        Assert.assertEquals("d+", res);
        res = UtilFunctions.acceptNegativeNumbersAsDigits("a+-d+");
        Assert.assertEquals("a+d+", res);
        res = UtilFunctions.acceptNegativeNumbersAsDigits("-d+a+");
        Assert.assertEquals("d+a+", res);
        res = UtilFunctions.acceptNegativeNumbersAsDigits("a+-d+a+");
        Assert.assertEquals("a+d+a+", res);
        res = UtilFunctions.acceptNegativeNumbersAsDigits("-d+a+-d+");
        Assert.assertEquals("d+a+d+", res);
    }


    @Test
    public void TestRemoveSpacesFunktion() {

        String res = UtilFunctions.removeInnerCharacterInPattern("a+s+", UtilFunctions.ALPHA, UtilFunctions.SPACE);
        Assert.assertEquals("a+", res);
        res = UtilFunctions.removeInnerCharacterInPattern("d+s+", UtilFunctions.ALPHA, UtilFunctions.SPACE);
        Assert.assertEquals("d+", res);
        res = UtilFunctions.removeInnerCharacterInPattern("s+d+", UtilFunctions.ALPHA, UtilFunctions.SPACE);
        Assert.assertEquals("d+", res);
        res = UtilFunctions.removeInnerCharacterInPattern("s+a+", UtilFunctions.ALPHA, UtilFunctions.SPACE);
        Assert.assertEquals("a+", res);
        res = UtilFunctions.removeInnerCharacterInPattern("a+s+a+", UtilFunctions.ALPHA, UtilFunctions.SPACE);
        Assert.assertEquals("a+", res);
        res = UtilFunctions.removeInnerCharacterInPattern("d+s+a+", UtilFunctions.ALPHA, UtilFunctions.SPACE);
        Assert.assertEquals("d+a+", res);
        res = UtilFunctions.removeInnerCharacterInPattern("a+s+d+", UtilFunctions.ALPHA, UtilFunctions.SPACE);
        Assert.assertEquals("a+d+", res);

    }

    @Test
    public void TestRemoveUpperLowerCase() {
        String res = UtilFunctions.removeUpperLowerCase("u+d+u+");
        Assert.assertEquals("a+d+a+", res);
        res = UtilFunctions.removeUpperLowerCase("u+l+");
        Assert.assertEquals("a+", res);
        res = UtilFunctions.removeUpperLowerCase("u+l+d+");
        Assert.assertEquals("a+d+", res);
        res = UtilFunctions.removeUpperLowerCase("l+d+l+u+d+");
        Assert.assertEquals("a+d+a+d+", res);
        res = UtilFunctions.removeUpperLowerCase("l+u+su+l+sd+sl+tu+l+d+");
        Assert.assertEquals("a+sa+sd+sa+ta+d+", res);
    }

    @Test
    public void TestFrequencyOfEachConsecutiveChar() {

        String s = UtilFunctions.getFrequencyOfEachConsecutiveChar("ddduu");
        Assert.assertEquals("d3u2", s);

        s = UtilFunctions.getFrequencyOfEachConsecutiveChar("duu");
        Assert.assertEquals("d1u2", s);
    }

    @Test
    public void TestFloatToDigitsFunction() {

        String s = UtilFunctions.removeInnerCharacterInPattern("d+t+d+", UtilFunctions.DIGIT, UtilFunctions.DOT);
        Assert.assertEquals("d+", s);
        s = UtilFunctions.removeInnerCharacterInPattern("d+", UtilFunctions.DIGIT, UtilFunctions.DOT);
        Assert.assertEquals("d+", s);
        s = UtilFunctions.removeInnerCharacterInPattern("t+d+", UtilFunctions.DIGIT, UtilFunctions.DOT);
        Assert.assertEquals("d+", s);
        s = UtilFunctions.removeInnerCharacterInPattern("a+t+", UtilFunctions.DIGIT, UtilFunctions.DOT);
        Assert.assertEquals("a+", s);
        s = UtilFunctions.removeInnerCharacterInPattern("a+d+t+d+", UtilFunctions.DIGIT, UtilFunctions.DOT);
        Assert.assertEquals("a+d+", s);
        s = UtilFunctions.removeInnerCharacterInPattern("d+t+a+", UtilFunctions.DIGIT, UtilFunctions.DOT);
        Assert.assertEquals("d+a+", s);
        s = UtilFunctions.removeInnerCharacterInPattern("d+t+d+a+", UtilFunctions.DIGIT, UtilFunctions.DOT);
        Assert.assertEquals("d+a+", s);
        s = UtilFunctions.removeInnerCharacterInPattern("a+d+t+d+", UtilFunctions.DIGIT, UtilFunctions.DOT);
        Assert.assertEquals("a+d+", s);
    }

}
