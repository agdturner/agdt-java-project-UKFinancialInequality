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
public class UKFI_Process_TENURE extends UKFI_Main_Process {

    /**
     * Value label information for Ten1W5
     * <ul>
     * <li>Value = 1.0	Label = Own it outright</li>
     * <li>Value = 2.0	Label = Buying it with the help of a mortgage or
     * loan</li>
     * <li>Value = 3.0	Label = Pay part rent and part mortgage (shared
     * ownership)</li>
     * <li>Value = 4.0	Label = Rent it</li>
     * <li>Value = 5.0	Label = Live here rent-free (including rent-free in
     * relatives friend</li>
     * <li>Value = 6.0	Label = Squatting</li>
     * <li>Value = -9.0	Label = Does not know</li>
     * <li>Value = -8.0	Label = No answer</li>
     * <li>Value = -7.0	Label = Does not apply</li>
     * <li>Value = -6.0	Label = Error/Partial</li>
     * </ul>
     */
    public TreeMap<Byte, String> TenureNameMap;

    // Tenure Counts for each Wave, GOR, and Tenure
    public TreeMap<Byte, TreeMap<Byte, TreeMap<Byte, Integer>>> TenureCountsWaveGORSubsets;
    public TreeMap<Byte, TreeMap<Byte, TreeMap<Byte, Integer>>> TenureCountsWaveGOR;

    public UKFI_Process_TENURE(UKFI_Main_Process p) {
        super(p);
        TenureNameMap = p.env.we.hh.getTenureNameMap();
        TenureCountsWaveGORSubsets = new TreeMap<>();
        TenureCountsWaveGOR = new TreeMap<>();
        /**
         * Initialise TenureCountsWaveGORSubsets and TenureCountsWaveGOR
         */
        for (byte w = 1; w <= WaAS_Data.NWAVES; w++) {
            TreeMap<Byte, TreeMap<Byte, Integer>> TenureCountsGORSubsets;
            TreeMap<Byte, TreeMap<Byte, Integer>> TenureCountsGOR;
            TenureCountsGORSubsets = new TreeMap<>();
            TenureCountsGOR = new TreeMap<>();
            TenureCountsWaveGORSubsets.put(w, TenureCountsGORSubsets);
            TenureCountsWaveGOR.put(w, TenureCountsGOR);
            Iterator<Byte> ite;
            ite = gors.iterator();
            while (ite.hasNext()) {
                byte gor = ite.next();
                if (gor != 3) {
                    TreeMap<Byte, Integer> TenureCountsSubset;
                    TreeMap<Byte, Integer> TenureCounts;
                    TenureCountsSubset = new TreeMap<>();
                    TenureCounts = new TreeMap<>();
                    TenureCountsGORSubsets.put(gor, TenureCounts);
                    TenureCountsGOR.put(gor, TenureCounts);
                    TenureNameMap.keySet().stream().forEach(tenure -> {
                        TenureCountsSubset.put(tenure, 0);
                        TenureCounts.put(tenure, 0);
                    });
                }
            }
        }
    }

    /**
     *
     */
    public void createGraph() {
        for (byte tenure = 1; tenure <= 6; tenure ++) {

        // Get tenure counts for subsets.
        getTenureCountsForGORSubsets(WaAS_Data.W1);
        getTenureCountsForGORSubsets(WaAS_Data.W5);
        TreeMap<Byte, TreeMap<Byte, Integer>> TenureCountsGORW1Subsets;
        TenureCountsGORW1Subsets = TenureCountsWaveGORSubsets.get(WaAS_Data.W1);
        TreeMap<Byte, TreeMap<Byte, Integer>> TenureCountsGORW5Subsets;
        TenureCountsGORW5Subsets = TenureCountsWaveGORSubsets.get(WaAS_Data.W5);

        // Calculate differences in Tenure tenure for subsets
        TreeMap<Byte, Double> changeTenure1Subset;
        changeTenure1Subset = new TreeMap<>();
        Iterator<Byte> ite;
        ite = TenureCountsGORW1Subsets.keySet().iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            TreeMap<Byte, Integer> TenureCountsW1;
            TenureCountsW1 = TenureCountsGORW1Subsets.get(gor);
            TreeMap<Byte, Integer> TenureCountsW5;
            TenureCountsW5 = TenureCountsGORW5Subsets.get(gor);
            double diff = (100.0d * TenureCountsW5.get((byte) tenure) / (double) 6990)
                    - (100.0d * TenureCountsW1.get((byte) tenure) / (double) 6990);
            changeTenure1Subset.put(gor, diff);
        }

        // Get tenure counts for all.
        WaAS_HHOLD_Handler handler;
        File inDir = files.getWaASInputDir();
        handler = new WaAS_HHOLD_Handler(env.we, inDir);
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> allW1 = handler.loadAllW1();
        TreeMap<Byte, TreeMap<Byte, Integer>> TenureCountsGORW1;
        TenureCountsGORW1 = getTenureCountsForGOR(gors, allW1, WaAS_Data.W1);
        int allW1size = allW1.size();
        allW1 = null; // Set to null to free memory.
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> allW5 = handler.loadAllW5();
        TreeMap<Byte, TreeMap<Byte, Integer>> TenureCountsGORW5;
        TenureCountsGORW5 = getTenureCountsForGOR(gors, allW5, WaAS_Data.W5);
        int allW5size = allW5.size();
        allW5 = null; // Set to null to free memory.

        // Calculate differences in Tenure 1 for all
        TreeMap<Byte, Double> changeTenure1;
        changeTenure1 = new TreeMap<>();
        ite = TenureCountsGORW1.keySet().iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            TreeMap<Byte, Integer> TenureCountsW1;
            TenureCountsW1 = TenureCountsGORW1.get(gor);
            TreeMap<Byte, Integer> TenureCountsW5;
            TenureCountsW5 = TenureCountsGORW5.get(gor);
            double diff = (100.0d * TenureCountsW5.get((byte) tenure) / (double) allW5size) 
                    - (100.0d * TenureCountsW1.get((byte) tenure) / (double) allW1size);
            changeTenure1.put(gor, diff);
        }

        // Data to graph.
        env.log("Data to graph");
        env.log("GOR,GORName,changeTenure1,changeTenure2");
        ite = gors.iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            if (gor != 3) {
                env.log("" + gor + "," + GORNameLookup.get(gor) + ","
                        + changeTenure1Subset.get(gor) + ","
                        + changeTenure1.get(gor));
            }
        }

        // Graph data
        String title;
        String xAxisLabel;
        String yAxisLabel;
        String variableName = "Tenure " + TenureNameMap.get(tenure);
        title = "Average change in " + variableName + " (Wave 5 minus Wave 1)";
        xAxisLabel = "Government Office Region";
        yAxisLabel = "%";
        //BigDecimal yIncrement = new BigDecimal("1");
        BigDecimal yIncrement = null;
        int numberOfYAxisTicks = 10;
        createLineGraph(title, xAxisLabel, yAxisLabel, variableName,
                changeTenure1Subset, changeTenure1, numberOfYAxisTicks,
                yIncrement);
        }
    }

    /**
     * Get the total HPROPW in subset.
     *
     * @param wave
     */
    public void getTenureCountsForGORSubsets(byte wave) {
        TreeMap<Byte, TreeMap<Byte, Integer>> TenureCountsGOR;
        TenureCountsGOR = TenureCountsWaveGORSubsets.get(wave);
        if (wave == WaAS_Data.W1) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        WaAS_Wave1_HHOLD_Record w1;
                        w1 = cr.w1Record.getHhold();
                        Byte GOR = GORLookups[wave - 1].get(CASEW1);
                        TreeMap<Byte, Integer> TenureCounts;
                        TenureCounts = TenureCountsGOR.get(GOR);
                        byte TEN1 = w1.getTEN1();
                        Generic_Collections.addToMap(TenureCounts, TEN1, 1);
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == WaAS_Data.W2) {
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
                            TreeMap<Byte, Integer> TenureCounts;
                            TenureCounts = TenureCountsGOR.get(GOR);
                            byte TEN1 = w2.getTEN1();
                            Generic_Collections.addToMap(TenureCounts, TEN1, 1);
                        }
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == WaAS_Data.W3) {
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
                                TreeMap<Byte, Integer> TenureCounts;
                                TenureCounts = TenureCountsGOR.get(GOR);
                                byte TEN1 = w3.getTEN1();
                                Generic_Collections.addToMap(TenureCounts, TEN1, 1);
                            }
                        }
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == WaAS_Data.W4) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
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
                                    TreeMap<Byte, Integer> TenureCounts;
                                    TenureCounts = TenureCountsGOR.get(GOR);
                                    byte TEN1 = w4.getTEN1();
                                    Generic_Collections.addToMap(TenureCounts, TEN1, 1);
                                }
                            }
                        }
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == WaAS_Data.W5) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>>> w5Records;
                        w5Records = cr.w5Records;
                        Short CASEW2;
                        Short CASEW3;
                        Short CASEW4;
                        Short CASEW5;
                        Iterator<Short> ite1;
                        ite1 = w5Records.keySet().iterator();
                        while (ite1.hasNext()) {
                            CASEW2 = ite1.next();
                            HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>> w5_2;
                            w5_2 = w5Records.get(CASEW2);
                            Iterator<Short> ite2;
                            ite2 = w5_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                CASEW3 = ite2.next();
                                HashMap<Short, HashMap<Short, WaAS_Wave5_Record>> w5_3;
                                w5_3 = w5_2.get(CASEW3);
                                Iterator<Short> ite3;
                                ite3 = w5_3.keySet().iterator();
                                while (ite3.hasNext()) {
                                    CASEW4 = ite3.next();
                                    HashMap<Short, WaAS_Wave5_Record> w5_4;
                                    w5_4 = w5_3.get(CASEW4);
                                    Iterator<Short> ite4;
                                    ite4 = w5_4.keySet().iterator();
                                    while (ite4.hasNext()) {
                                        CASEW5 = ite4.next();
                                        Byte GOR = GORLookups[wave - 1].get(CASEW5);
                                        WaAS_Wave5_HHOLD_Record w5;
                                        w5 = w5_4.get(CASEW5).getHhold();
                                        TreeMap<Byte, Integer> TenureCounts;
                                        TenureCounts = TenureCountsGOR.get(GOR);
                                        byte TEN1 = w5.getTEN1();
                                        Generic_Collections.addToMap(TenureCounts, TEN1, 1);
                                    }
                                }
                            }
                        }
                    }
                });
                data.clearCollection(cID);
            });
        }
    }

    /**
     *
     * @param gors
     * @param wAll
     * @param wave
     */
    public TreeMap<Byte, TreeMap<Byte, Integer>> getTenureCountsForGOR(
            ArrayList<Byte> gors,
            TreeMap<Short, ?> wAll,
            byte wave) {
        TreeMap<Byte, TreeMap<Byte, Integer>> TenureCountsGOR;
        TenureCountsGOR = TenureCountsWaveGOR.get(wave);
        HashMap<Byte, HashMap<Short, Double>> r;
        r = new HashMap<>();
        gors.stream().forEach(gor -> {
            r.put(gor, new HashMap<>());
        });
        Iterator<Short> ite = wAll.keySet().iterator();
        Short CASEWX;
        WaAS_Wave1Or2Or3Or4Or5_HHOLD_Record rec;
        Byte GOR;
        while (ite.hasNext()) {
            CASEWX = ite.next();
            rec = (WaAS_Wave1Or2Or3Or4Or5_HHOLD_Record) wAll.get(CASEWX);
            GOR = rec.getGOR();
            TreeMap<Byte, Integer> TenureCounts;
            TenureCounts = TenureCountsGOR.get(GOR);
            if (TenureCounts == null) {
                System.out.println("Tenure Counts is null for GOR " + GOR);
            } else {
                byte TEN1 = rec.getTEN1();
                Generic_Collections.addToMap(TenureCounts, TEN1, 1);
            }
        }
        return TenureCountsGOR;
    }
}
