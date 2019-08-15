/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leeds.ccg.andyt.projects.ukfi.process;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Collection;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_CombinedRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.handlers.WaAS_HHOLD_Handler;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W1ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W2ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W2Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W3ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W3Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W4ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W4Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W5ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W5Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W1W2W3W4W5HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W1HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W2HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W3HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W4HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W5HRecord;
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
        for (byte w = 1; w <= we.NWAVES; w++) {
            TreeMap<Byte, TreeMap<Byte, Integer>> TenureCountsGORSubsets;
            TreeMap<Byte, TreeMap<Byte, Integer>> TenureCountsGOR;
            TenureCountsGORSubsets = new TreeMap<>();
            TenureCountsGOR = new TreeMap<>();
            TenureCountsWaveGORSubsets.put(w, TenureCountsGORSubsets);
            TenureCountsWaveGOR.put(w, TenureCountsGOR);
            Iterator<Byte> ite = gors.iterator();
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
        for (byte tenure = 1; tenure <= 6; tenure++) {

            // Get tenure counts for subsets.
            getTenureCountsForGORSubsets(we.W1);
            getTenureCountsForGORSubsets(we.W5);
            TreeMap<Byte, TreeMap<Byte, Integer>> TenureCountsGORW1Subsets;
            TenureCountsGORW1Subsets = TenureCountsWaveGORSubsets.get(we.W1);
            TreeMap<Byte, TreeMap<Byte, Integer>> TenureCountsGORW5Subsets;
            TenureCountsGORW5Subsets = TenureCountsWaveGORSubsets.get(we.W5);

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
            WaAS_HHOLD_Handler hH = new WaAS_HHOLD_Handler(env.we);
            TreeMap<WaAS_W1ID, WaAS_W1HRecord> allW1 = hH.loadAllW1();
            TreeMap<Byte, TreeMap<Byte, Integer>> TenureCountsGORW1;
            TenureCountsGORW1 = getTenureCountsForGOR(gors, allW1, we.W1);
            int allW1size = allW1.size();
            allW1 = null; // Set to null to free memory.
            TreeMap<WaAS_W5ID, WaAS_W5HRecord> allW5 = hH.loadAllW5();
            TreeMap<Byte, TreeMap<Byte, Integer>> TenureCountsGORW5;
            TenureCountsGORW5 = getTenureCountsForGOR(gors, allW5, we.W5);
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

            // Graph collections
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
        if (wave == we.W1) {
            env.we.data.collections.keySet().stream().forEach(cID -> {
                WaAS_Collection c = env.we.data.getCollection(cID);
                c.getData().keySet().stream().forEach(w1ID -> {
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = c.getData().get(w1ID);
                        WaAS_W1HRecord w1 = cr.w1Rec.getHr();
                        Byte GOR = GORSubsetsAndLookups.w1_To_gor.get(w1ID);
                        TreeMap<Byte, Integer> TenureCounts;
                        TenureCounts = TenureCountsGOR.get(GOR);
                        byte TEN1 = w1.getTEN1();
                        Generic_Collections.addToMap(TenureCounts, TEN1, 1);
                    }
                });
                env.we.data.clearCollection(cID);
            });
        } else if (wave == we.W2) {
            env.we.data.collections.keySet().stream().forEach(cID -> {
                WaAS_Collection c = env.we.data.getCollection(cID);
                c.getData().keySet().stream().forEach(w1ID -> {
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = c.getData().get(w1ID);
                        HashMap<WaAS_W2ID, WaAS_W2Record> w2Records = cr.w2Recs;
                        Iterator<WaAS_W2ID> ite2 = w2Records.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W2ID w2ID = ite2.next();
                            Byte GOR = GORSubsetsAndLookups.w2_To_gor.get(w2ID);
                            WaAS_W2HRecord w2 = w2Records.get(w2ID).getHr();
                            TreeMap<Byte, Integer> TenureCounts;
                            TenureCounts = TenureCountsGOR.get(GOR);
                            byte TEN1 = w2.getTEN1();
                            Generic_Collections.addToMap(TenureCounts, TEN1, 1);
                        }
                    }
                });
                env.we.data.clearCollection(cID);
            });
        } else if (wave == we.W3) {
            env.we.data.collections.keySet().stream().forEach(cID -> {
                WaAS_Collection c = env.we.data.getCollection(cID);
                c.getData().keySet().stream().forEach(w1ID -> {
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = c.getData().get(w1ID);
                        HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, WaAS_W3Record>> w3Records;
                        w3Records = cr.w3Recs;
                        Iterator<WaAS_W2ID> ite1 = w3Records.keySet().iterator();
                        while (ite1.hasNext()) {
                            WaAS_W2ID w2ID = ite1.next();
                            HashMap<WaAS_W3ID, WaAS_W3Record> w3_2 = w3Records.get(w2ID);
                            Iterator<WaAS_W3ID> ite2 = w3_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                WaAS_W3ID w3ID = ite2.next();
                                Byte GOR = GORSubsetsAndLookups.w3_To_gor.get(w3ID);
                                WaAS_W3HRecord w3 = w3_2.get(w3ID).getHr();
                                TreeMap<Byte, Integer> TenureCounts;
                                TenureCounts = TenureCountsGOR.get(GOR);
                                byte TEN1 = w3.getTEN1();
                                Generic_Collections.addToMap(TenureCounts, TEN1, 1);
                            }
                        }
                    }
                });
                env.we.data.clearCollection(cID);
            });
        } else if (wave == we.W4) {
            env.we.data.collections.keySet().stream().forEach(cID -> {
                WaAS_Collection c;
                c = env.we.data.getCollection(cID);
                c.getData().keySet().stream().forEach(w1ID -> {
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr;
                        cr = c.getData().get(w1ID);
                        HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>>> w4Records;
                        w4Records = cr.w4Recs;
                        Iterator<WaAS_W2ID> ite1 = w4Records.keySet().iterator();
                        while (ite1.hasNext()) {
                            WaAS_W2ID w2ID = ite1.next();
                            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>> w4_2;
                            w4_2 = w4Records.get(w2ID);
                            Iterator<WaAS_W3ID> ite2 = w4_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                WaAS_W3ID w3ID = ite2.next();
                                HashMap<WaAS_W4ID, WaAS_W4Record> w4_3 = w4_2.get(w3ID);
                                Iterator<WaAS_W4ID> ite3 = w4_3.keySet().iterator();
                                while (ite3.hasNext()) {
                                    WaAS_W4ID w4ID = ite3.next();
                                    Byte GOR = GORSubsetsAndLookups.w4_To_gor.get(w4ID);
                                    WaAS_W4HRecord w4 = w4_3.get(w4ID).getHr();
                                    TreeMap<Byte, Integer> TenureCounts;
                                    TenureCounts = TenureCountsGOR.get(GOR);
                                    byte TEN1 = w4.getTEN1();
                                    Generic_Collections.addToMap(TenureCounts, TEN1, 1);
                                }
                            }
                        }
                    }
                });
                env.we.data.clearCollection(cID);
            });
        } else if (wave == we.W5) {
            env.we.data.collections.keySet().stream().forEach(cID -> {
                WaAS_Collection c;
                c = env.we.data.getCollection(cID);
                c.getData().keySet().stream().forEach(w1ID -> {
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = c.getData().get(w1ID);
                        HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>>> w5Records;
                        w5Records = cr.w5Recs;
                        Iterator<WaAS_W2ID> ite1 = w5Records.keySet().iterator();
                        while (ite1.hasNext()) {
                            WaAS_W2ID w2ID = ite1.next();
                            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>> w5_2;
                            w5_2 = w5Records.get(w2ID);
                            Iterator<WaAS_W3ID> ite2 = w5_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                WaAS_W3ID w3ID = ite2.next();
                                HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>> w5_3;
                                w5_3 = w5_2.get(w3ID);
                                Iterator<WaAS_W4ID> ite3 = w5_3.keySet().iterator();
                                while (ite3.hasNext()) {
                                    WaAS_W4ID w4ID = ite3.next();
                                    HashMap<WaAS_W5ID, WaAS_W5Record> w5_4 = w5_3.get(w4ID);
                                    Iterator<WaAS_W5ID> ite4 = w5_4.keySet().iterator();
                                    while (ite4.hasNext()) {
                                        WaAS_W5ID CASEW5 = ite4.next();
                                        Byte GOR = GORSubsetsAndLookups.w5_To_gor.get(CASEW5);
                                        WaAS_W5HRecord w5 = w5_4.get(CASEW5).getHr();
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
                env.we.data.clearCollection(cID);
            });
        }
    }

    /**
     *
     * @param <K>
     * @param gors
     * @param wAll
     * @param wave
     * @return
     */
    public <K> TreeMap<Byte, TreeMap<Byte, Integer>> getTenureCountsForGOR(
            ArrayList<Byte> gors, TreeMap<K, ?> wAll, byte wave) {
        TreeMap<Byte, TreeMap<Byte, Integer>> TenureCountsGOR;
        TenureCountsGOR = TenureCountsWaveGOR.get(wave);
        HashMap<Byte, HashMap<Short, Double>> r = new HashMap<>();
        gors.stream().forEach(gor -> {
            r.put(gor, new HashMap<>());
        });
        Iterator<K> ite = wAll.keySet().iterator();
        while (ite.hasNext()) {
            K CASEWX = ite.next();
            WaAS_W1W2W3W4W5HRecord rec = (WaAS_W1W2W3W4W5HRecord) wAll.get(CASEWX);
            Byte GOR;
            /**
             * The following change was required due to differences between the
             * data obtained in November 2018 and August 2019
             */
            //GOR = rec.getGOR();
            switch (wave) {
                case 1:
                    WaAS_W1HRecord w1HRec = (WaAS_W1HRecord) rec;
                    GOR = w1HRec.getGOR();
                    break;
                case 2:
                    WaAS_W2HRecord w2HRec = (WaAS_W2HRecord) rec;
                    GOR = w2HRec.getGOR();
                    break;
                case 3:
                    WaAS_W3HRecord w3HRec = (WaAS_W3HRecord) rec;
                    GOR = w3HRec.getGOR();
                    break;
                case 5:
                    WaAS_W5HRecord w5HRec = (WaAS_W5HRecord) rec;
                    GOR = w5HRec.getGOR();
                    break;
                default:
                    env.log("Unrecognised type");
                    GOR = null;
                    break;
            }
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
