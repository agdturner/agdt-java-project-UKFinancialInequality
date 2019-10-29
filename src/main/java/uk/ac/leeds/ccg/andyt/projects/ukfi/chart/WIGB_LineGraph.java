/**
 * Copyright 2018 Andy Turner, The University of Leeds, UK.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package uk.ac.leeds.ccg.andyt.projects.ukfi.chart;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import uk.ac.leeds.ccg.andyt.chart.examples.Chart_Line;
import uk.ac.leeds.ccg.andyt.generic.core.Generic_Environment;
import uk.ac.leeds.ccg.andyt.generic.util.Generic_Collections;

/**
 * An implementation of <code>Generic_AbstractLineGraph</code> to generate a
 * Line Chart Visualization of some default data and write it out to file as a
 * PNG.
 */
public class WIGB_LineGraph extends Chart_Line {

    /**
     *
     * @param es
     * @param file
     * @param format
     * @param title
     * @param dataWidth
     * @param dataHeight
     * @param xAxisLabel
     * @param yAxisLabel
     * @param yMax
     * @param yPin
     * @param yIncrement
     * @param numberOfYAxisTicks
     * @param decimalPlacePrecisionForCalculations
     * @param decimalPlacePrecisionForDisplay
     * @param r
     */
    public WIGB_LineGraph(Generic_Environment env, 
            ExecutorService es, File file, String format, String title,
            int dataWidth, int dataHeight,
            String xAxisLabel, String yAxisLabel,
            BigDecimal yMax,
            ArrayList<BigDecimal> yPin,
            BigDecimal yIncrement,
            int numberOfYAxisTicks, boolean drawYZero,
            int decimalPlacePrecisionForCalculations,
            int decimalPlacePrecisionForDisplay,
            RoundingMode r) {
        super(env, es, file, format, title, dataWidth, dataHeight, xAxisLabel, 
                yAxisLabel, yMax, yPin, yIncrement, numberOfYAxisTicks, 
                drawYZero, dataWidth, dataWidth, r);
//        this.yMax = yMax;
//        this.yPin = yPin;
//        this.yIncrement = yIncrement;
//        this.numberOfYAxisTicks = numberOfYAxisTicks;
//        init(es, file, format, title, dataWidth, dataHeight, xAxisLabel,
//                yAxisLabel, false, decimalPlacePrecisionForCalculations,
//                decimalPlacePrecisionForDisplay, r);
    }

    public void setData(
            String variableName, ArrayList<Byte> gors,
            TreeMap<Byte, String> GORNameLookup,
            TreeMap<Byte, Double> changeHPROPWSubset,
            TreeMap<Byte, Double> changeHPROPWAll) {
        data = new Object[7];

        TreeMap<String, TreeMap<BigDecimal, BigDecimal>> maps;
        maps = new TreeMap<>();

        TreeMap<BigDecimal, BigDecimal> map;
        map = new TreeMap<>();
        int x;
        x = 1;
        Iterator<Byte> ite;
        ite = gors.iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            map.put(new BigDecimal(x), new BigDecimal(changeHPROPWSubset.get(gor)));
            maps.put("Change in " + variableName + " Subset", map);
            x++;
        }
        TreeMap<BigDecimal, BigDecimal> map2;
        map2 = new TreeMap<>();
        x = 1;
        ite = gors.iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            map2.put(new BigDecimal(x), new BigDecimal(changeHPROPWAll.get(gor)));
            maps.put("Change in " + variableName + " All", map2);
            x++;
        }
        BigDecimal[] minMaxBigDecimal;
        minMaxBigDecimal = Generic_Collections.getMinMaxBigDecimal(map);
        minY = minMaxBigDecimal[0];
         maxY = minMaxBigDecimal[1];
         minX = map.firstKey();
         maxX = map.lastKey();
        minMaxBigDecimal = Generic_Collections.getMinMaxBigDecimal(map2);
        if (minY.compareTo(minMaxBigDecimal[0]) == 1) {
            minY = minMaxBigDecimal[0];
        }
        if (maxY.compareTo(minMaxBigDecimal[1]) == -1) {
            maxY = minMaxBigDecimal[1];
        }
        if (minX.compareTo(map2.firstKey()) == 1) {
            minX = map2.firstKey();
        }
        if (maxX.compareTo(map2.lastKey()) == -1) {
            maxX = map2.lastKey();
        }
        data[0] = maps;
        data[1] = minY;
        data[2] = maxY;
        data[3] = minX;
        data[4] = maxX;
        ArrayList<String> labels;
        labels = new ArrayList<>();
        labels.addAll(maps.keySet());
        data[5] = labels;

        // Comment out the following section to have a normal axis instead of labels.
        TreeMap<BigDecimal, String> xAxisLabels;
        xAxisLabels = new TreeMap<>();
        x = 1;
        ite = gors.iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            xAxisLabels.put(new BigDecimal(x), GORNameLookup.get(gor));
            x++;
        }
        data[6] = xAxisLabels;
        setData(data);
    }
}
