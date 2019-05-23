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
     * @param name
     * @param yIncrement The increment to use on the y axis for ticks.
     * @param vName Variable name
     */
    public void createGraph(String name, BigDecimal yIncrement, String vName) {

        /**
         * Get Variable for each wave in the subsets.
         */
        TreeMap<Byte, Double> changeSubset = hh.getChangeVariableSubset(
                vName, gors, GORSubsetsAndLookups, GORNameLookup, data,
                subset);
//        changeSubset = new TreeMap<>();
//HVALUE
//        changeSubset.put((byte) 1, 18783.080794344292);
//        changeSubset.put((byte) 2, 31422.89858602066);
//        changeSubset.put((byte) 4, 37033.93366888953);
//        changeSubset.put((byte) 5, 31309.95230665049);
//        changeSubset.put((byte) 6, 23092.340505592496);
//        changeSubset.put((byte) 7, 40828.75264348736);
//        changeSubset.put((byte) 8, 199981.6611156352);
//        changeSubset.put((byte) 9, 106084.6388429752);
//        changeSubset.put((byte) 10, 43699.888348443725);
//        changeSubset.put((byte) 11, 7046.695521390357);
//        changeSubset.put((byte) 12, 47581.911239563255);
//HPROPW
//        changeSubset.put((byte) 1, 15543.71633729733);
//        changeSubset.put((byte) 2, 25043.078226624522);
//        changeSubset.put((byte) 4, 52316.468204716686);
//        changeSubset.put((byte) 5, 25161.244726051722);
//        changeSubset.put((byte) 6, 28463.983883376408);
//        changeSubset.put((byte) 7, 43457.62488630164);
//        changeSubset.put((byte) 8, 217870.42118621065);
//        changeSubset.put((byte) 9, 106894.89840824757);
//        changeSubset.put((byte) 10, 45446.714779741014);
//        changeSubset.put((byte) 11, 12773.929645721946);
//        changeSubset.put((byte) 12, 47155.80562571576);
        /**
         * Get Variable Total Household Property Wealth for each wave for all
         * records.
         */
        TreeMap<Byte, Double> changeAll = hh.getChangeVariableAll(vName, gors, 
                GORNameLookup);
//        changeAll = new TreeMap<>();
//HVALUE
//        changeAll.put((byte) 1, 8205.087170241168);
//        changeAll.put((byte) 2, 19961.039231688454);
//        changeAll.put((byte) 4, 30446.793232431926);
//        changeAll.put((byte) 5, 27533.2284276828);
//        changeAll.put((byte) 6, 28296.789723345108);
//        changeAll.put((byte) 7, 54166.46835776328);
//        changeAll.put((byte) 8, 158700.90905907305);
//        changeAll.put((byte) 9, 87343.95985063427);
//        changeAll.put((byte) 10, 53206.957541575975);
//        changeAll.put((byte) 11, 26843.193725923193);
//        changeAll.put((byte) 12, 39896.735828757795);
//HPROPW
//        changeAll.put((byte) 1, 1356.6025079350366);
//        changeAll.put((byte) 2, 21243.045817762642);
//        changeAll.put((byte) 4, 31114.669464522856);
//        changeAll.put((byte) 5, 34598.05717948475);
//        changeAll.put((byte) 6, 17621.49991544307);
//        changeAll.put((byte) 7, 62317.38477178279);
//        changeAll.put((byte) 8, 188232.2675591018);
//        changeAll.put((byte) 9, 90111.07547487883);
//        changeAll.put((byte) 10, 68052.50996744036);
//        changeAll.put((byte) 11, 33259.53730763032);
//        changeAll.put((byte) 12, 38072.805181146614);
        // Data to graph.
        env.log("Data to graph");
        env.log("GOR,GORName,change" + vName + "Subset,change" + vName + "All");
        Iterator<Byte> ite = gors.iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            if (gor != 3) {
                env.log("" + gor + "," + GORNameLookup.get(gor) + ","
                        + changeSubset.get(gor) + ","
                        + changeAll.get(gor));
            }
        }

        // Graph data
        String title = "Average change in " + vName + " for " + name + " (Wave 5 minus Wave 1)";
        String xAxisLabel = "Government Office Region";
        String yAxisLabel = "£";
        int numberOfYAxisTicks = 10;
        createLineGraph(title, xAxisLabel, yAxisLabel, vName, changeSubset,
                changeAll, numberOfYAxisTicks, yIncrement);
    }
}
