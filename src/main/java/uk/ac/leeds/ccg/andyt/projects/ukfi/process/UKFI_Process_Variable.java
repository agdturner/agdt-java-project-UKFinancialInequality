/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leeds.ccg.andyt.projects.ukfi.process;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.TreeMap;

/**
 *
 * @author Andy Turner
 */
public class UKFI_Process_Variable extends UKFI_Main_Process {

    public UKFI_Process_Variable(UKFI_Main_Process p) {
        super(p);
    }

    /**
     * BigDecimal yIncrement = new BigDecimal("20000");
     */
    public void createGraph(BigDecimal yIncrement, String variableName) {

        /**
         * Get Variable for each wave in the subsets.
         */
        TreeMap<Byte, Double> changeVariableSubset = hh.getChangeVariableSubset(
                variableName, gors, GORSubsets, GORLookups, GORNameLookup, data,
                subset);
//        changeVariableSubset = new TreeMap<>();
//HVALUE
//        changeVariableSubset.put((byte) 1, 18783.080794344292);
//        changeVariableSubset.put((byte) 2, 31422.89858602066);
//        changeVariableSubset.put((byte) 4, 37033.93366888953);
//        changeVariableSubset.put((byte) 5, 31309.95230665049);
//        changeVariableSubset.put((byte) 6, 23092.340505592496);
//        changeVariableSubset.put((byte) 7, 40828.75264348736);
//        changeVariableSubset.put((byte) 8, 199981.6611156352);
//        changeVariableSubset.put((byte) 9, 106084.6388429752);
//        changeVariableSubset.put((byte) 10, 43699.888348443725);
//        changeVariableSubset.put((byte) 11, 7046.695521390357);
//        changeVariableSubset.put((byte) 12, 47581.911239563255);
//Variable
//        changeVariableSubset.put((byte) 1, 15543.71633729733);
//        changeVariableSubset.put((byte) 2, 25043.078226624522);
//        changeVariableSubset.put((byte) 4, 52316.468204716686);
//        changeVariableSubset.put((byte) 5, 25161.244726051722);
//        changeVariableSubset.put((byte) 6, 28463.983883376408);
//        changeVariableSubset.put((byte) 7, 43457.62488630164);
//        changeVariableSubset.put((byte) 8, 217870.42118621065);
//        changeVariableSubset.put((byte) 9, 106894.89840824757);
//        changeVariableSubset.put((byte) 10, 45446.714779741014);
//        changeVariableSubset.put((byte) 11, 12773.929645721946);
//        changeVariableSubset.put((byte) 12, 47155.80562571576);
        /**
         * Get Variable Total Household Property Wealth for each wave for all
         * records.
         */
        TreeMap<Byte, Double> changeVariableAll = hh.getChangeVariableAll(
                variableName, gors, GORNameLookup);
//        changeVariableAll = new TreeMap<>();
//HVALUE
//        changeVariableAll.put((byte) 1, 8205.087170241168);
//        changeVariableAll.put((byte) 2, 19961.039231688454);
//        changeVariableAll.put((byte) 4, 30446.793232431926);
//        changeVariableAll.put((byte) 5, 27533.2284276828);
//        changeVariableAll.put((byte) 6, 28296.789723345108);
//        changeVariableAll.put((byte) 7, 54166.46835776328);
//        changeVariableAll.put((byte) 8, 158700.90905907305);
//        changeVariableAll.put((byte) 9, 87343.95985063427);
//        changeVariableAll.put((byte) 10, 53206.957541575975);
//        changeVariableAll.put((byte) 11, 26843.193725923193);
//        changeVariableAll.put((byte) 12, 39896.735828757795);
//Variable
//        changeVariableAll.put((byte) 1, 1356.6025079350366);
//        changeVariableAll.put((byte) 2, 21243.045817762642);
//        changeVariableAll.put((byte) 4, 31114.669464522856);
//        changeVariableAll.put((byte) 5, 34598.05717948475);
//        changeVariableAll.put((byte) 6, 17621.49991544307);
//        changeVariableAll.put((byte) 7, 62317.38477178279);
//        changeVariableAll.put((byte) 8, 188232.2675591018);
//        changeVariableAll.put((byte) 9, 90111.07547487883);
//        changeVariableAll.put((byte) 10, 68052.50996744036);
//        changeVariableAll.put((byte) 11, 33259.53730763032);
//        changeVariableAll.put((byte) 12, 38072.805181146614);
        // Data to graph.
        env.log("Data to graph");
        env.log("GOR,GORName,change" + variableName + "Subset,change" + variableName + "All");
        Iterator<Byte> ite = gors.iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            if (gor != 3) {
                env.log("" + gor + "," + GORNameLookup.get(gor) + ","
                        + changeVariableSubset.get(gor) + ","
                        + changeVariableAll.get(gor));
            }
        }

        // Graph data
        String title = "Average change in " + variableName + " (Wave 5 minus "
                + "Wave 1)";
        String xAxisLabel = "Government Office Region";
        String yAxisLabel = "£";
        int numberOfYAxisTicks = 10;
        createLineGraph(title, xAxisLabel, yAxisLabel, variableName,
                changeVariableSubset, changeVariableAll, numberOfYAxisTicks,
                yIncrement);
    }
}
