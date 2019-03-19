/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leeds.ccg.andyt.projects.ukfi.process;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Collection;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Combined_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_HHOLD_Handler;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave2_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave3_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave4_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave5_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave1Or2Or3Or4Or5_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave1_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave2_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave3_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave4_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave5_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.util.Generic_Collections;

/**
 *
 * @author Andy Turner
 */
public class UKFI_Process_HPROPW extends UKFI_Main_Process {

    public UKFI_Process_HPROPW(UKFI_Main_Process p) {
        super(p);
    }

    /**
     *
     */
    public void createGraph() {
        /**
         * Get HPROPW Total Household Property Wealth for each wave in the
         * subsets.
         */
        TreeMap<Byte, Double> changeHPROPWSubset;
        changeHPROPWSubset = getChangeHPROPWSubset();
//        changeHPROPWSubset = new TreeMap<>();
//        changeHPROPWSubset.put((byte) 1, 15543.71633729733);
//        changeHPROPWSubset.put((byte) 2, 25043.078226624522);
//        changeHPROPWSubset.put((byte) 4, 52316.468204716686);
//        changeHPROPWSubset.put((byte) 5, 25161.244726051722);
//        changeHPROPWSubset.put((byte) 6, 28463.983883376408);
//        changeHPROPWSubset.put((byte) 7, 43457.62488630164);
//        changeHPROPWSubset.put((byte) 8, 217870.42118621065);
//        changeHPROPWSubset.put((byte) 9, 106894.89840824757);
//        changeHPROPWSubset.put((byte) 10, 45446.714779741014);
//        changeHPROPWSubset.put((byte) 11, 12773.929645721946);
//        changeHPROPWSubset.put((byte) 12, 47155.80562571576);
        /**
         * Get HPROPW Total Household Property Wealth for each wave for all
         * records.
         */
        TreeMap<Byte, Double> changeHPROPWAll;
        changeHPROPWAll = getChangeHPROPWAll();
//        changeHPROPWAll = new TreeMap<>();
//        changeHPROPWAll.put((byte) 1, 1356.6025079350366);
//        changeHPROPWAll.put((byte) 2, 21243.045817762642);
//        changeHPROPWAll.put((byte) 4, 31114.669464522856);
//        changeHPROPWAll.put((byte) 5, 34598.05717948475);
//        changeHPROPWAll.put((byte) 6, 17621.49991544307);
//        changeHPROPWAll.put((byte) 7, 62317.38477178279);
//        changeHPROPWAll.put((byte) 8, 188232.2675591018);
//        changeHPROPWAll.put((byte) 9, 90111.07547487883);
//        changeHPROPWAll.put((byte) 10, 68052.50996744036);
//        changeHPROPWAll.put((byte) 11, 33259.53730763032);
//        changeHPROPWAll.put((byte) 12, 38072.805181146614);
        // Data to graph.
        env.log("Data to graph");
        env.log("GOR,GORName,changeHPROPWSubset,changeHPROPWAll");
        Iterator<Byte> ite;
        ite = gors.iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            if (gor != 3) {
                env.log("" + gor + "," + GORNameLookup.get(gor) + ","
                        + changeHPROPWSubset.get(gor) + ","
                        + changeHPROPWAll.get(gor));
            }
        }

        // Graph data
        String title;
        String xAxisLabel;
        String yAxisLabel;
        title = "Average change in HPROPW (Wave 5 minus Wave 1)";
        xAxisLabel = "Government Office Region";
        yAxisLabel = "£";
        BigDecimal yIncrement = new BigDecimal("20000");
        int numberOfYAxisTicks = 10;
        createLineGraph(title, xAxisLabel, yAxisLabel, "HPROPW",
                changeHPROPWSubset, changeHPROPWAll, numberOfYAxisTicks,
                yIncrement);
    }

    /**
     * Get HPROPW Total Household Property Wealth for each wave in the subsets.
     *
     * @return
     */
    public TreeMap<Byte, Double> getChangeHPROPWSubset() {
        TreeMap<Byte, Double> r;
        HashMap<Byte, HashMap<Short, Double>>[] HPROPWSubsets;
        HPROPWSubsets = new HashMap[WaAS_Data.NWAVES];
        for (byte w = 0; w < WaAS_Data.NWAVES; w++) {
            HPROPWSubsets[w] = getHPROPWForGORSubsets((byte) (w + 1));
        }
        r = new TreeMap<>();
        double countW1 = 0;
        double countZeroW1 = 0;
        double countNegativeW1 = 0;
        double countW5 = 0;
        double countZeroW5 = 0;
        double countNegativeW5 = 0;
        env.log("HPROPW for each wave in the subsets.");
        String h = "GORNumber,GORName,HPROPW5_Average-HPROPW1_Average";
        for (byte w = 1; w < WaAS_Data.NWAVES + 1; w++) {
            h += ",HPROPWW" + w + "_Count,HPROPWW" + w + "_ZeroCount,HPROPWW"
                    + w + "_NegativeCount,HPROPWW" + w + "_Average";
        }
        env.log(h);
        Iterator<Byte> ite;
        ite = gors.iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            double[][] aHPROPW = new double[WaAS_Data.NWAVES][];
            for (byte w = 0; w < WaAS_Data.NWAVES; w++) {
                aHPROPW[w] = getSummaryStatistics(
                        HPROPWSubsets[w].get(gor).values());
            }
            countW1 += aHPROPW[0][5];
            countZeroW1 += aHPROPW[0][6];
            countNegativeW1 += aHPROPW[0][7];
            countW5 += aHPROPW[4][5];
            countZeroW5 += aHPROPW[4][6];
            countNegativeW5 += aHPROPW[4][7];
            double diff = aHPROPW[4][4] - aHPROPW[0][4];
            String s;
            s = "" + gor + "," + GORNameLookup.get(gor) + "," + diff;
            for (byte w = 0; w < WaAS_Data.NWAVES; w++) {
                s += "," + aHPROPW[w][4] + "," + aHPROPW[w][5] + ","
                        + aHPROPW[w][6] + "," + aHPROPW[w][7];
            }
            env.log(s);
            r.put(gor, diff);
        }
        env.log("HPROPW For Wave 1 Subset");
        env.log("" + countW1 + "\t Count");
        env.log("" + countZeroW1 + "\t Zero");
        env.log("" + countNegativeW1 + "\t Negative");
        env.log("HPROPW For Wave 5 Subset");
        env.log("" + countW5 + "\t Count");
        env.log("" + countZeroW5 + "\t Zero");
        env.log("" + countNegativeW5 + "\t Negative");
        return r;
    }

    /**
     * Get the total HPROPW in subset.
     *
     * @param wave
     * @return Map with keys as GOR and Values as map with keys as CASEWX and
     * values as HPROPW.
     */
    public HashMap<Byte, HashMap<Short, Double>> getHPROPWForGORSubsets(
            byte wave) {
        // Initialise result
        HashMap<Byte, HashMap<Short, Double>> r = new HashMap<>();
        Iterator<Byte> ite = GORSubsets[wave - 1].keySet().iterator();
        while (ite.hasNext()) {
            Byte GOR = ite.next();
            r.put(GOR, new HashMap<>());
        }
        if (wave == env.W1) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr = c.getData().get(CASEW1);
                        WaAS_Wave1_HHOLD_Record w1 = cr.w1Record.getHhold();
                        Byte GOR = GORLookups[wave - 1].get(CASEW1);
                        Generic_Collections.addToMap(r, GOR, CASEW1,
                                w1.getHPROPW());
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == env.W2) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr = c.getData().get(CASEW1);
                        HashMap<Short, WaAS_Wave2_Record> w2Records;
                        w2Records = cr.w2Records;
                        Iterator<Short> ite2 = w2Records.keySet().iterator();
                        while (ite2.hasNext()) {
                            Short CASEW2 = ite2.next();
                            Byte GOR = GORLookups[wave - 1].get(CASEW2);
                            WaAS_Wave2_HHOLD_Record w2;
                            w2 = w2Records.get(CASEW2).getHhold();
                            Generic_Collections.addToMap(r, GOR, CASEW2,
                                    w2.getHPROPW());
                        }
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == env.W3) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, WaAS_Wave3_Record>> w3Records;
                        w3Records = cr.w3Records;
                        Iterator<Short> ite1 = w3Records.keySet().iterator();
                        while (ite1.hasNext()) {
                            Short CASEW2 = ite1.next();
                            HashMap<Short, WaAS_Wave3_Record> w3_2;
                            w3_2 = w3Records.get(CASEW2);
                            Iterator<Short> ite2 = w3_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                Short CASEW3 = ite2.next();
                                Byte GOR = GORLookups[wave - 1].get(CASEW3);
                                WaAS_Wave3_HHOLD_Record w3;
                                w3 = w3_2.get(CASEW3).getHhold();
                                Generic_Collections.addToMap(r, GOR, CASEW3,
                                        w3.getHPROPW());
                            }
                        }
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == env.W4) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave4_Record>>> w4Records;
                        w4Records = cr.w4Records;
                        Iterator<Short> ite1 = w4Records.keySet().iterator();
                        while (ite1.hasNext()) {
                            Short CASEW2 = ite1.next();
                            HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                            w4_2 = w4Records.get(CASEW2);
                            Iterator<Short> ite2 = w4_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                Short CASEW3 = ite2.next();
                                HashMap<Short, WaAS_Wave4_Record> w4_3;
                                w4_3 = w4_2.get(CASEW3);
                                Iterator<Short> ite3 = w4_3.keySet().iterator();
                                while (ite3.hasNext()) {
                                    Short CASEW4 = ite3.next();
                                    Byte GOR = GORLookups[wave - 1].get(CASEW4);
                                    WaAS_Wave4_HHOLD_Record w4;
                                    w4 = w4_3.get(CASEW4).getHhold();
                                    Generic_Collections.addToMap(r, GOR, CASEW4, w4.getHPROPW());
                                }
                            }
                        }
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == env.W5) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>>> w5Records;
                        w5Records = cr.w5Records;
                        Iterator<Short> ite1  = w5Records.keySet().iterator();
                        while (ite1.hasNext()) {
                            Short CASEW2 = ite1.next();
                            HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>> w5_2;
                            w5_2 = w5Records.get(CASEW2);
                            Iterator<Short> ite2 = w5_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                Short CASEW3 = ite2.next();
                                HashMap<Short, HashMap<Short, WaAS_Wave5_Record>> w5_3;
                                w5_3 = w5_2.get(CASEW3);
                                Iterator<Short> ite3  = w5_3.keySet().iterator();
                                while (ite3.hasNext()) {
                                    Short CASEW4 = ite3.next();
                                    HashMap<Short, WaAS_Wave5_Record> w5_4;
                                    w5_4 = w5_3.get(CASEW4);
                                    Iterator<Short> ite4  = w5_4.keySet().iterator();
                                    while (ite4.hasNext()) {
                                        Short CASEW5 = ite4.next();
                                        Byte GOR = GORLookups[wave - 1].get(CASEW5);
                                        WaAS_Wave5_HHOLD_Record w5;
                                        w5 = w5_4.get(CASEW5).getHhold();
                                        Generic_Collections.addToMap(r, GOR, CASEW5, w5.getHPROPW());
                                    }
                                }
                            }
                        }
                    }
                });
                data.clearCollection(cID);
            });
        }
        return r;
    }

    /**
     * Get the HPROPW.
     *
     * @param gors
     * @param w5All
     * @param wave
     * @return Map with keys as GOR and Values as map with keys as CASEWX and
     * values as HPROPW.
     */
    public HashMap<Byte, HashMap<Short, Double>> getHPROPWForGOR(
            ArrayList<Byte> gors,            TreeMap<Short, ?> w5All,
            //TreeMap<Short, WaAS_Wave1Or2Or3Or4Or5_HHOLD_Record> w5All, 
            byte wave) {
        // Initialise result
        HashMap<Byte, HashMap<Short, Double>> r  = new HashMap<>();
        gors.stream().forEach(gor -> {
            r.put(gor, new HashMap<>());
        });
        int countNegative = 0;
        int countZero = 0;
        Iterator<Short> ite = w5All.keySet().iterator();
        while (ite.hasNext()) {
            Short CASEWX = ite.next();
            WaAS_Wave1Or2Or3Or4Or5_HHOLD_Record rec;
            rec = (WaAS_Wave1Or2Or3Or4Or5_HHOLD_Record) w5All.get(CASEWX);
            Byte GOR = rec.getGOR();
            double HPROPW = rec.getHPROPW();
            if (HPROPW == 0.0d) {
                countZero++;
            } else if (HPROPW < 0.0d) {
                countNegative++;
            }
            Generic_Collections.addToMap(r, GOR, CASEWX, HPROPW);
        }
        env.log("HPROPW for GOR W" + wave);
        env.log("count " + w5All.size());
        env.log("countZero " + countZero);
        env.log("countNegative " + countNegative);
        return r;
    }

    /**
     * Get HPROPW Total Household Property Wealth for each wave for all records.
     *
     * @return
     */
    public TreeMap<Byte, Double> getChangeHPROPWAll() {
        TreeMap<Byte, Double> r  = new TreeMap<>();
        WaAS_HHOLD_Handler hH = new WaAS_HHOLD_Handler(env.we);
        HashMap<Byte, HashMap<Short, Double>>[] HPROPWAll;
        HPROPWAll = new HashMap[env.NWAVES];
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> allW1 = hH.loadAllW1();
        HPROPWAll[0] = getHPROPWForGOR(gors, allW1, (byte) 1);
        allW1 = null; // Set to null to free memory.
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> allW5 = hH.loadAllW5();
        HPROPWAll[4] = getHPROPWForGOR(gors, allW5, (byte) 5);
        allW5 = null; // Set to null to free memory.
        env.log("HPROPW Total Household Property Wealth for each wave for all records.");
        String h  = "GORNumber,GORName,HPROPW5_Average-HPROPW1_Average";
        for (byte w = 1; w < WaAS_Data.NWAVES + 1; w++) {
            if (w == 1 || w == 5) {
                h += ",HPROPWW" + w + "_Count,HPROPWW" + w + "_ZeroCount,HPROPWW"
                        + w + "_NegativeCount,HPROPWW" + w + "_Average";
            }
        }
        env.log(h);
        Iterator<Byte> ite;
        ite = gors.iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            double[][] aHPROPW = new double[env.NWAVES][];
            for (byte w = 0; w < env.NWAVES; w++) {
                if (w == 0 || w == 4) {
                    aHPROPW[w] = getSummaryStatistics(
                            HPROPWAll[w].get(gor).values());
                }
            }
            double diff = aHPROPW[4][4] - aHPROPW[0][4];
            String s  = "" + gor + "," + GORNameLookup.get(gor) + "," + diff;
            for (byte w = 0; w < env.NWAVES; w++) {
                if (w == 0 || w == 4) {
                    s += "," + aHPROPW[w][4] + "," + aHPROPW[w][5] + ","
                            + aHPROPW[w][6] + "," + aHPROPW[w][7];
                }
            }
            env.log(s);
            r.put(gor, diff);
        }
        return r;
    }
}
