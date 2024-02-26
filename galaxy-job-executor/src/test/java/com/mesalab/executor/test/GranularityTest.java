package com.mesalab.executor.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mesalab.executor.druid.granularity.*;
import org.junit.Test;

public class GranularityTest {

    @Test
    public void testGranularitiesParseAndCompare() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        GranularityModule granularityModule = new GranularityModule();
        JodaStuff.register(granularityModule);
        mapper.registerModule(granularityModule);
        String[] strs = new String[]{"SECOND", "{\"type\":\"period\",\"period\":\"PT60S\",\"timeZone\":\"UTC\",\"origin\":null}", "MINUTE"};
        for (String str : strs) {
            Granularity granularity;
            if(str.trim().startsWith("{")){
                granularity = mapper.readValue(str, PeriodGranularity.class);
            }else{
                granularity = Granularity.fromString(str);
            }

            System.out.println(granularity);
            System.out.println("equals SECOND:" + granularity.equals(Granularities.SECOND));
            System.out.println("equals MINUTE:" + granularity.equals(Granularities.MINUTE));
            System.out.println("compare SECOND:" + Granularity.IS_FINER_THAN.compare(granularity, Granularities.SECOND));
            System.out.println("compare MINUTE:" + Granularity.IS_FINER_THAN.compare(granularity, Granularities.MINUTE));
        }
    }

}
