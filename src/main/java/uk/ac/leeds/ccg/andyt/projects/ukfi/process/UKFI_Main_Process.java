/*
 * Copyright 2018 geoagdt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.leeds.ccg.andyt.projects.ukfi.process;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import uk.ac.leeds.ccg.andyt.generic.core.Generic_Environment;
import uk.ac.leeds.ccg.andyt.generic.core.Generic_Strings;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;
import uk.ac.leeds.ccg.andyt.projects.ukfi.core.UKFI_Environment;
import uk.ac.leeds.ccg.andyt.projects.ukfi.core.UKFI_Object;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Collection;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Combined_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_HHOLD_Handler;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Handler;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave2_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave3_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave4_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave5_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave1_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave2_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave3_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave4_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave5_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave1Or2Or3Or4Or5_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave1_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave2_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave3_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave4_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave5_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.execution.Generic_Execution;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_Files;
import uk.ac.leeds.ccg.andyt.generic.util.Generic_Collections;
import uk.ac.leeds.ccg.andyt.generic.visualisation.Generic_Visualisation;
import uk.ac.leeds.ccg.andyt.projects.ukfi.chart.WIGB_LineGraph;
import uk.ac.leeds.ccg.andyt.projects.ukfi.io.UKFI_Files;

/**
 *
 * @author geoagdt
 */
public class UKFI_Main_Process extends UKFI_Object {

    // For convenience
    protected final UKFI_Files files;
    protected final WaAS_Data data;

    /**
     * Subset of all records that have the same household composition.
     */
    HashSet<Short> subset;

    ArrayList<Byte> gors;
    TreeMap<Byte, String> GORNameLookup;
    HashMap<Byte, HashSet<Short>>[] GORSubsets;
    HashMap<Short, Byte>[] GORLookups;

    public UKFI_Main_Process(UKFI_Environment env) {
        super(env);
        files = env.files;
        this.data = env.data;
    }

    public UKFI_Main_Process(UKFI_Main_Process p) {
        data = p.data;
        files = p.files;
        env = p.env;
        subset = p.subset;
        gors = p.gors;
        GORNameLookup = p.GORNameLookup;
        GORSubsets = p.GORSubsets;
        GORLookups = p.GORLookups;
    }

    public static void main(String[] args) {
        Generic_Environment ge = new Generic_Environment();
        File wasDataDir = new File(
                ge.getFiles().getDataDir().getParentFile().getParentFile().getParentFile(),
                WaAS_Strings.s_generic);
        wasDataDir = new File(wasDataDir, Generic_Strings.s_data);
        wasDataDir = new File(wasDataDir, WaAS_Strings.PROJECT_NAME);
        wasDataDir = new File(wasDataDir, Generic_Strings.s_data);
        UKFI_Environment env = new UKFI_Environment(ge, wasDataDir);
        UKFI_Main_Process p = new UKFI_Main_Process(env);
        p.files.setDataDirectory(UKFI_Files.getDefaultDataDir());
        // Main switches
        //p.doJavaCodeGeneration = true;
        p.doLoadDataIntoCaches = true; // rename/reuse just left here for convenience...
        p.run();
    }

    /**
     * The aim is to measure: 1) Costs associated with tenure (expenditure on
     * rent, net of housing benefit, and/or mortgage interest for primary
     * residences); 2) Net gains associated with property wealth (capital gains
     * and rental income, net of capital losses, property maintenance costs,
     * mortgage interest costs associated with investment properties and costs
     * associated with meeting landlord obligations).
     *
     * How does the balance between costs and benefits vary for households in
     * different tenures and property wealth percentiles, as well as different
     * regions, income percentiles and age cohorts? Which groups are the biggest
     * gainers and losers over the period 2006-2016? To what extent are the
     * costs and benefits affecting households consistent over time? In other
     * words, how much mobility do households experience between the categories
     * of ‘winners’ and ‘losers’ over the course of those ten years?
     */
    public void run() {
        String m = this.getClass().getName() + ".run()";
        env.logStartTag(m);
        initSubset();
        /**
         * Init gors and GORNameLookup.
         */
        gors = WaAS_Handler.getGORs();
        // Government Office Region (GOR) name lookup looks up the name from the numerical code.
        GORNameLookup = WaAS_Handler.getGORNameLookup();
        Iterator<Byte> ite;

        /**
         * Init GORSubsets and GORLookups
         */
        initGORSubsetsAndLookups();
        int[] ttotals = new int[env.NWAVES];
        env.log("GOR Subsets");
        env.log("NW1,NW2,NW3,NW4,NW5,GORNumber,GORName");
        ite = gors.iterator();
        while (ite.hasNext()) {
            String s = "";
            int[] totals = new int[env.NWAVES];
            byte gor = ite.next();
            HashSet<Short> GORSubset;
            for (byte w = 0; w < env.NWAVES; w++) {
                GORSubset = GORSubsets[w].get(gor);
                totals[w] += GORSubset.size();
                s += totals[w] + ",";
                ttotals[w] += totals[w];
            }
            s += gor + "," + GORNameLookup.get(gor);
            env.log(s);
        }
        // Totals
        String s = "";
        for (byte w = 0; w < env.NWAVES; w++) {
            s += ttotals[w] + ",";
        }
        s += "0,All";
        env.log(s);
        /**
         * TENURE
         */
        UKFI_Process_TENURE tp = new UKFI_Process_TENURE(this);
        tp.createGraph();
//        /**
//         * HPROPW
//         */
//        WIGB_Process_HPROPW hp;
//        hp = new WIGB_Process_HPROPW(this);
//        hp.createGraph();
//
//        /**
//         * HVALUE
//         */
//        WIGB_Process_HVALUE hv;
//        hv = new WIGB_Process_HVALUE(this);
//        hv.createGraph();

        // Check some counts
        WaAS_HHOLD_Handler hH = new WaAS_HHOLD_Handler(env.we);
        Object[] w5 = hH.loadW5();
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> w5recs;
        w5recs = (TreeMap<Short, WaAS_Wave5_HHOLD_Record>) w5[0];
        Iterator<Short> ites = w5recs.keySet().iterator();
        int countMortgage = 0;
        int countNonMortgage = 0;
        int countBuyWithMortgage = 0;
        int countPartBuyWithMortgage = 0;
        int countZeroMIntRate1W5 = 0;
        int countPositiveMIntRate1W5 = 0;
        int countZeroMVal1W5 = 0;
        int countPositiveMVal1W5 = 0;
        int countZeroMNumbNW5 = 0;
        int countPositiveMNumbNW5 = 0;
        int countGT1MNumbNW5 = 0;
        while (ites.hasNext()) {
            short CASEW5 = ites.next();
            WaAS_Wave5_HHOLD_Record w5rec = w5recs.get(CASEW5);
            byte ten1 = w5rec.getTEN1();
            if (ten1 == 2 || ten1 == 3) {
                countMortgage++;
                if (ten1 == 2) {
                    countBuyWithMortgage++;
                }
                if (ten1 == 3) {
                    countPartBuyWithMortgage++;
                }
                double MIntRate1W5 = w5rec.getMINTRATE1();
                if (MIntRate1W5 == 0.0d) {
                    countZeroMIntRate1W5++;
                } else {
                    if (MIntRate1W5 > 0) {
                        countPositiveMIntRate1W5++;
                    }
                }
                int MVal1W5 = w5rec.getMVAL1();
                if (MVal1W5 == 0) {
                    countZeroMVal1W5++;
                } else {
                    if (MVal1W5 > 0) {
                        countPositiveMVal1W5++;
                    }
                }
                int MNumbNW5 = w5rec.getMNUMB();
                if (MNumbNW5 == 0) {
                    countZeroMNumbNW5++;
                } else {
                    if (MNumbNW5 > 0) {
                        countPositiveMNumbNW5++;
                        if (MNumbNW5 > 1) {
                            countGT1MNumbNW5++;
                        }
                    }
                }
            } else {
                countNonMortgage++;
            }
        }
        env.log("" + w5recs.size() + "\t countAllW5withW4");
        env.log("" + countMortgage + "\t countMortgage");
        env.log("" + countNonMortgage + "\t countNonMortgage");
        env.log("" + countBuyWithMortgage + "\t countBuyWithMortgage");
        env.log("" + countPartBuyWithMortgage + "\t countPartBuyWithMortgage");
        env.log("" + countZeroMIntRate1W5 + "\t countZeroMIntRate1W5");
        env.log("" + countPositiveMIntRate1W5 + "\t countPositiveMIntRate1W5");
        env.log("" + countZeroMVal1W5 + "\t countZeroMVal1W5");
        env.log("" + countPositiveMVal1W5 + "\t countPositiveMVal1W5");
        env.log("" + countZeroMNumbNW5 + "\t countZeroMNumbNW5");
        env.log("" + countPositiveMNumbNW5 + "\t countPositiveMNumbNW5");
        env.log("" + countGT1MNumbNW5 + "\t countGT1MNumbNW5");

        //TreeMap<Short, HashSet<Short>> CASEW4ToCASEW5;
        //CASEW4ToCASEW5 = (TreeMap<Short, HashSet<Short>>) ((Object[]) w5[4])[3];
        //Get Non-zero and zero counts for:
//        MIntRate1W5 (Interest rate on mortgage 1) 
//        MVal1W5 (Amount still outstanding mortgageloan) 
//        MValB1W5 (Banded amount outstanding on mortgageloan)
//        MNumbNW5 (Number of mortgages)
        // DVTotGIRW5	Variable label = Household Gross Annual (regular) income  - (rounded to 3 significant figures)
        // DVLUKVAL5
        // FINCVB5
        //getWave1Or2HPRICEBLookup();
        //getWave3Or4Or5HPRICEBLookup();
        env.logEndTag(m);
    }

    /**
     * Initialises {@link #subset} to be the set of all records that have a
     * "stable" household composition.
     */
    public void initSubset() {
        String m = "initSubset()";
        env.logStartTag(m);
        String fn = "SameCompositionHashSet_CASEW1.dat";
        File f = new File(files.getOutputDataDir(), fn);
        if (f.exists()) {
            subset = (HashSet<Short>) Generic_IO.readObject(f);
        } else {
            subset = getStableHouseholdComposition();
            Generic_IO.writeObject(subset, f);
        }
        env.log("Total number of initial households in wave 1 " + subset.size());
        env.logEndTag(m);
    }

    /**
     * Init GORSubsets and GORLookups
     */
    public void initGORSubsetsAndLookups() {
        Object[] o;
        File f = new File(files.getOutputDataDir(), "GORSubsetsAndLookups.dat");
        if (f.exists()) {
            o = (Object[]) Generic_IO.readObject(f);
        } else {
            o = WaAS_Handler.getGORSubsetsAndLookup(data, gors, subset);
            Generic_IO.writeObject(o, f);
        }
        GORSubsets = (HashMap<Byte, HashSet<Short>>[]) o[0];
        GORLookups = (HashMap<Short, Byte>[]) o[1];
    }

    /**
     *
     * @param c
     * @return
     */
    protected double[] getSummaryStatistics(Collection<Double> c) {
        DoubleSummaryStatistics stats = c.stream().
                collect(DoubleSummaryStatistics::new,
                        DoubleSummaryStatistics::accept,
                        DoubleSummaryStatistics::combine);
        double[] r = new double[8];
        r[0] = stats.getMax();
        r[1] = stats.getMin();
        r[2] = stats.getCount();
        r[3] = stats.getSum();
        r[4] = stats.getAverage();
        int countNegative = 0;
        int countZero = 0;
        Iterator<Double> ite;
        ite = c.iterator();
        double HPROPW;
        while (ite.hasNext()) {
            HPROPW = ite.next();
            if (HPROPW == 0.0d) {
                countZero++;
            } else if (HPROPW < 0.0d) {
                countNegative++;
                //System.out.println("" + HPROPW + " Negative HPROPW");
            }
        }
        r[5] = c.size();
        r[6] = countZero;
        r[7] = countNegative;
        return r;
    }

    /**
     * Go through hholds for all waves and figure which ones have not
     * significantly changed in terms of hhold composition. Having children and
     * children leaving home is fine. Anything else is perhaps an issue...
     *
     * @return
     */
    public HashSet<Short> getStableHouseholdComposition() {
        String m = "getStableHouseholdComposition";
        env.logIDSub = env.ge.initLog(m);
        env.logID = env.logIDSub;
        HashSet<Short> r = new HashSet<>();
        env.log("Number of combined records " + data.CASEW1ToCID.size());
        env.log("Number of collections of combined records " + data.data.size());
        int count0 = 0;
        int count1 = 0;
        int count2 = 0;
        int count3 = 0;
        // Check For Household Records
        String m1 = "Check For Household Records";
        env.logStartTag(m1);
        Iterator<Short> ite = data.data.keySet().iterator();
        while (ite.hasNext()) {
            short cID = ite.next();
            WaAS_Collection c = data.getCollection(cID);
            HashMap<Short, WaAS_Combined_Record> cData = c.getData();
            Iterator<Short> ite2 = cData.keySet().iterator();
            while (ite2.hasNext()) {
                short CASEW1 = ite2.next();
                WaAS_Combined_Record cr = cData.get(CASEW1);
                boolean check = process0(CASEW1, cr);
                if (check) {
                    count0++;
                }
                check = process1(CASEW1, cr);
                if (check) {
                    count1++;
                }
                check = process2(CASEW1, cr);
                if (check) {
                    count2++;
                }
                check = process3(CASEW1, cr);
                if (check) {
                    count3++;
                    r.add(CASEW1);
                }
            }
            data.clearCollection(cID);
        }
        env.log("" + count0 + " Total hholds in all 5 waves.");
        env.log("" + count1 + " Total hholds that are a single hhold over all 5 "
                + "waves.");
        env.log("" + count2 + " Total hholds that are a single hhold over all 5 "
                + "waves and have same number of adults in all 5 waves.");
        env.log("" + count3 + " Total hholds that are a single hhold over all 5 "
                + "waves and have the same basic adult household composition "
                + "over all 5 waves.");
        env.logEndTag(m1);
        env.logEndTag(m);
        env.logID = env.logIDMain;
        return r;
    }

    /**
     * Checks if cr has the same basic adult household composition in each wave
     * for those hholds that have only 1 record for each wave. Iff this is the
     * case then true is returned. The number of adults in a household is
     * allowed to decrease. If the number of adults increases, then a further
     * check is done: If the number of householders is the same and the number
     * of children has decreased (it might be assumed that children have become
     * non-dependents). But, if that is not the case, then if the number of
     * dependents increases for any wave then false is returned.
     *
     * @param CASEW1
     * @param cr
     * @return true iff cr has only 1 record for each wave.
     */
    protected boolean process3(short CASEW1, WaAS_Combined_Record cr) {
        boolean r;
        r = true;
        if (cr.w2Records.size() > 1) {
            env.log("There are multiple Wave 2 records for CASEW1 " + CASEW1);
            r = false;
        }
        Short CASEW2;
        Iterator<Short> ite2;
        ite2 = cr.w2Records.keySet().iterator();
        while (ite2.hasNext()) {
            CASEW2 = ite2.next();
            WaAS_Wave2_Record w2rec;
            w2rec = cr.w2Records.get(CASEW2);
            String m3;
            m3 = "There are multiple Wave 3 records for "
                    + "CASEW2 " + CASEW2 + " in CASEW1 " + CASEW1;
            if (cr.w3Records.containsKey(CASEW2)) {
                HashMap<Short, WaAS_Wave3_Record> w3_2;
                w3_2 = cr.w3Records.get(CASEW2);
                if (w3_2.size() > 1) {
                    env.log(m3);
                    r = false;
                } else {
                    Short CASEW3;
                    Iterator<Short> ite3;
                    ite3 = w3_2.keySet().iterator();
                    while (ite3.hasNext()) {
                        CASEW3 = ite3.next();
                        WaAS_Wave3_Record w3rec;
                        w3rec = w3_2.get(CASEW3);
                        String m4;
                        m4 = "There are multiple Wave 4 records for "
                                + "CASEW3 " + CASEW3
                                + " in CASEW2 " + CASEW2
                                + " in CASEW1 " + CASEW1;
                        if (cr.w4Records.containsKey(CASEW2)) {
                            HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                            w4_2 = cr.w4Records.get(CASEW2);
                            if (w4_2.containsKey(CASEW3)) {
                                HashMap<Short, WaAS_Wave4_Record> w4_3;
                                w4_3 = w4_2.get(CASEW3);
                                if (w4_3.size() > 1) {
                                    env.log(m4);
                                    r = false;
                                } else {
                                    Iterator<Short> ite4;
                                    ite4 = w4_3.keySet().iterator();
                                    while (ite4.hasNext()) {
                                        Short CASEW4;
                                        CASEW4 = ite4.next();
                                        WaAS_Wave4_Record w4rec;
                                        w4rec = w4_3.get(CASEW4);
                                        String m5;
                                        m5 = "There are multiple Wave 5 records for "
                                                + "CASEW4 " + CASEW4
                                                + " in CASEW3 " + CASEW3
                                                + " in CASEW2 " + CASEW2
                                                + " in CASEW1 " + CASEW1;
                                        if (cr.w5Records.containsKey(CASEW2)) {
                                            HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>> w5_2;
                                            w5_2 = cr.w5Records.get(CASEW2);
                                            if (w5_2.containsKey(CASEW3)) {
                                                HashMap<Short, HashMap<Short, WaAS_Wave5_Record>> w5_3;
                                                w5_3 = w5_2.get(CASEW3);
                                                if (w5_3.containsKey(CASEW4)) {
                                                    HashMap<Short, WaAS_Wave5_Record> w5_4;
                                                    w5_4 = w5_3.get(CASEW4);
                                                    if (w5_4.size() > 1) {
                                                        env.log(m5);
                                                        r = false;
                                                    } else {
                                                        Iterator<Short> ite5;
                                                        ite5 = w5_4.keySet().iterator();
                                                        while (ite5.hasNext()) {
                                                            Short CASEW5;
                                                            CASEW5 = ite5.next();
                                                            WaAS_Wave5_Record w5rec;
                                                            w5rec = w5_4.get(CASEW5);
                                                            // Wave 1
                                                            WaAS_Wave1_HHOLD_Record w1hhold;
                                                            w1hhold = cr.w1Record.getHhold();
                                                            ArrayList<WaAS_Wave1_PERSON_Record> w1people;
                                                            w1people = cr.w1Record.getPeople();
                                                            // Wave 2
                                                            WaAS_Wave2_HHOLD_Record w2hhold;
                                                            w2hhold = w2rec.getHhold();
                                                            ArrayList<WaAS_Wave2_PERSON_Record> w2people;
                                                            w2people = w2rec.getPeople();
                                                            // Wave 3
                                                            WaAS_Wave3_HHOLD_Record w3hhold;
                                                            w3hhold = w3rec.getHhold();
                                                            ArrayList<WaAS_Wave3_PERSON_Record> w3people;
                                                            w3people = w3rec.getPeople();
                                                            // Wave 4
                                                            WaAS_Wave4_HHOLD_Record w4hhold;
                                                            w4hhold = w4rec.getHhold();
                                                            ArrayList<WaAS_Wave4_PERSON_Record> w4people;
                                                            w4people = w4rec.getPeople();
                                                            // Wave 5
                                                            WaAS_Wave5_HHOLD_Record w5hhold;
                                                            w5hhold = w5rec.getHhold();
                                                            ArrayList<WaAS_Wave5_PERSON_Record> w5people;
                                                            w5people = w5rec.getPeople();
                                                            byte w1NUMADULT = w1hhold.getNUMADULT();
                                                            byte w1NUMCHILD = w1hhold.getNUMCHILD();
                                                            //byte w1NUMDEPCH = w1hhold.getNUMDEPCH();
                                                            byte w1NUMHHLDR = w1hhold.getNUMHHLDR();

                                                            byte w2NUMADULT = w2hhold.getNUMADULT();
                                                            byte w2NUMCHILD = w2hhold.getNUMCHILD();
                                                            //byte w2NUMDEPCH = w2hhold.getNUMDEPCH_HH();
                                                            //boolean w2NUMNDEP = w2hhold.getNUMNDEP();
                                                            byte w2NUMHHLDR = w2hhold.getNUMHHLDR();

                                                            byte w3NUMADULT = w3hhold.getNUMADULT();
                                                            byte w3NUMCHILD = w3hhold.getNUMCHILD();
                                                            //byte w3NUMDEPCH = w3hhold.getNUMDEPCH();
                                                            byte w3NUMHHLDR = w3hhold.getNUMHHLDR();

                                                            byte w4NUMADULT = w4hhold.getNUMADULT();
                                                            byte w4NUMCHILD = w4hhold.getNUMCHILD();
                                                            //byte w4NUMDEPCH = w4hhold.getNUMDEPCH();
                                                            byte w4NUMHHLDR = w4hhold.getNUMHHLDR();

                                                            byte w5NUMADULT = w5hhold.getNUMADULT();
                                                            byte w5NUMCHILD = w5hhold.getNUMCHILD();
                                                            //byte w5NUMDEPCH = w5hhold.getNUMDEPCH();
                                                            byte w5NUMHHLDR = w5hhold.getNUMHHLDR();
                                                            // Compare Wave 1 to Wave 2
                                                            if (w1NUMADULT > w2NUMADULT) {
                                                                if (!(w1NUMHHLDR == w2NUMHHLDR
                                                                        && w1NUMCHILD > w2NUMCHILD)) {
                                                                    // Compare Number of Non dependents in Waves 1 and 2
                                                                    int w1NUMNDep = getNUMNDEP(w1people);
                                                                    int w2NUMNDep = getNUMNDEP(w2people);
                                                                    if (w1NUMNDep < w2NUMNDep) {
                                                                        r = false;
                                                                    }
                                                                }
                                                            }
                                                            // Compare Wave 2 to Wave 3
                                                            if (w2NUMADULT > w3NUMADULT) {
                                                                if (!(w2NUMHHLDR == w3NUMHHLDR
                                                                        && w2NUMCHILD > w3NUMCHILD)) {
                                                                    // Compare Number of Non dependents in Waves 2 and 3
                                                                    int w2NUMNDep = getNUMNDEP(w2people);
                                                                    int w3NUMNDep = getNUMNDEP(w3people);
                                                                    if (w2NUMNDep < w3NUMNDep) {
                                                                        r = false;
                                                                    }
                                                                }
                                                            }
                                                            // Compare Wave 3 to Wave 4
                                                            if (w3NUMADULT > w4NUMADULT) {
                                                                if (!(w3NUMHHLDR == w4NUMHHLDR
                                                                        && w3NUMCHILD > w4NUMCHILD)) {
                                                                    // Compare Number of Non dependents in Waves 3 and 4
                                                                    int w3NUMNDep = getNUMNDEP(w3people);
                                                                    int w4NUMNDep = getNUMNDEP(w4people);
                                                                    if (w3NUMNDep < w4NUMNDep) {
                                                                        r = false;
                                                                    }
                                                                }
                                                            }
                                                            // Compare Wave 4 to Wave 5
                                                            if (w4NUMADULT > w5NUMADULT) {
                                                                if (!(w4NUMHHLDR == w5NUMHHLDR
                                                                        && w4NUMCHILD > w5NUMCHILD)) {
                                                                    // Compare Number of Non dependents in Waves 4 and 5
                                                                    int w4NUMNDep = getNUMNDEP(w4people);
                                                                    int w5NUMNDep = getNUMNDEP(w5people);
                                                                    if (w4NUMNDep < w5NUMNDep) {
                                                                        r = false;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                } else {
                                                    env.log("!w5_3.containsKey(CASEW4) " + m5);
                                                }
                                            } else {
                                                env.log("!w5_2.containsKey(CASEW3) " + m5);
                                                r = false;
                                            }
                                        } else {
                                            env.log("!cr.w5Records.containsKey(CASEW2) " + m5);
                                            r = false;
                                        }
                                    }
                                }
                            } else {
                                env.log("!w4_2.containsKey(CASEW3) " + m4);
                                r = false;
                            }
                        } else {
                            env.log("!cr.w4Records.containsKey(CASEW2) " + m4);
                            r = false;
                        }
                    }
                }
            } else {
                env.log(m3);
            }
        }
        return r;
    }

    /**
     * Get number of non child dependents.
     *
     * @param <T>
     * @param people
     * @return
     * @TODO Move to Person Handler
     */
    public <T> int getNUMNDEP(ArrayList<T> people) {
        int r = 0;
        WaAS_Wave1Or2Or3Or4Or5_PERSON_Record p2;
        Iterator<T> ite;
        ite = people.iterator();
        while (ite.hasNext()) {
            p2 = (WaAS_Wave1Or2Or3Or4Or5_PERSON_Record) ite.next();
//            p2.getISHRP();
//            p2.getISHRPPART();
//            p2.getISCHILD();
            if (p2.getISNDEP()) {
                r++;
            }
        }
        return r;
    }

    /**
     * Checks if cr has the same number of adults in each wave for those hholds
     * that have only 1 record for each wave.
     *
     * @param CASEW1
     * @param cr
     * @return true iff cr has only 1 record for each wave.
     */
    protected boolean process2(short CASEW1, WaAS_Combined_Record cr) {
        boolean r;
        r = true;
        if (cr.w2Records.size() > 1) {
            env.log("There are multiple Wave 2 records for CASEW1 " + CASEW1);
            r = false;
        }
        Short CASEW2;
        Iterator<Short> ite2;
        ite2 = cr.w2Records.keySet().iterator();
        while (ite2.hasNext()) {
            CASEW2 = ite2.next();
            WaAS_Wave2_Record w2rec;
            w2rec = cr.w2Records.get(CASEW2);
            String m3;
            m3 = "There are multiple Wave 3 records for "
                    + "CASEW2 " + CASEW2 + " in CASEW1 " + CASEW1;
            if (cr.w3Records.containsKey(CASEW2)) {
                HashMap<Short, WaAS_Wave3_Record> w3_2;
                w3_2 = cr.w3Records.get(CASEW2);
                if (w3_2.size() > 1) {
                    env.log(m3);
                    r = false;
                } else {
                    Short CASEW3;
                    Iterator<Short> ite3;
                    ite3 = w3_2.keySet().iterator();
                    while (ite3.hasNext()) {
                        CASEW3 = ite3.next();
                        WaAS_Wave3_Record w3rec;
                        w3rec = w3_2.get(CASEW3);
                        String m4;
                        m4 = "There are multiple Wave 4 records for "
                                + "CASEW3 " + CASEW3
                                + " in CASEW2 " + CASEW2
                                + " in CASEW1 " + CASEW1;
                        if (cr.w4Records.containsKey(CASEW2)) {
                            HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                            w4_2 = cr.w4Records.get(CASEW2);
                            if (w4_2.containsKey(CASEW3)) {
                                HashMap<Short, WaAS_Wave4_Record> w4_3;
                                w4_3 = w4_2.get(CASEW3);
                                if (w4_3.size() > 1) {
                                    env.log(m4);
                                    r = false;
                                } else {
                                    Iterator<Short> ite4;
                                    ite4 = w4_3.keySet().iterator();
                                    while (ite4.hasNext()) {
                                        Short CASEW4;
                                        CASEW4 = ite4.next();
                                        WaAS_Wave4_Record w4rec;
                                        w4rec = w4_3.get(CASEW4);
                                        String m5;
                                        m5 = "There are multiple Wave 5 records for "
                                                + "CASEW4 " + CASEW4
                                                + " in CASEW3 " + CASEW3
                                                + " in CASEW2 " + CASEW2
                                                + " in CASEW1 " + CASEW1;
                                        if (cr.w5Records.containsKey(CASEW2)) {
                                            HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>> w5_2;
                                            w5_2 = cr.w5Records.get(CASEW2);
                                            if (w5_2.containsKey(CASEW3)) {
                                                HashMap<Short, HashMap<Short, WaAS_Wave5_Record>> w5_3;
                                                w5_3 = w5_2.get(CASEW3);
                                                if (w5_3.containsKey(CASEW4)) {
                                                    HashMap<Short, WaAS_Wave5_Record> w5_4;
                                                    w5_4 = w5_3.get(CASEW4);
                                                    if (w5_4.size() > 1) {
                                                        env.log(m5);
                                                        r = false;
                                                    } else {
                                                        Iterator<Short> ite5;
                                                        ite5 = w5_4.keySet().iterator();
                                                        while (ite5.hasNext()) {
                                                            Short CASEW5;
                                                            CASEW5 = ite5.next();
                                                            WaAS_Wave5_Record w5rec;
                                                            w5rec = w5_4.get(CASEW5);
                                                            byte w1 = cr.w1Record.getHhold().getNUMADULT();
                                                            byte w2 = w2rec.getHhold().getNUMADULT();
                                                            byte w3 = w3rec.getHhold().getNUMADULT();
                                                            byte w4 = w4rec.getHhold().getNUMADULT();
                                                            byte w5 = w5rec.getHhold().getNUMADULT();
                                                            if (!(w1 == w2 && w2 == w3 && w3 == w4 && w4 == w5)) {
                                                                r = false;
                                                            }
                                                        }
                                                    }
                                                } else {
                                                    env.log("!w5_3.containsKey(CASEW4) " + m5);
                                                }
                                            } else {
                                                env.log("!w5_2.containsKey(CASEW3) " + m5);
                                                r = false;
                                            }
                                        } else {
                                            env.log("!cr.w5Records.containsKey(CASEW2) " + m5);
                                            r = false;
                                        }
                                    }
                                }
                            } else {
                                env.log("!w4_2.containsKey(CASEW3) " + m4);
                                r = false;
                            }
                        } else {
                            env.log("!cr.w4Records.containsKey(CASEW2) " + m4);
                            r = false;
                        }
                    }
                }
            } else {
                env.log(m3);
            }
        }
        return r;
    }

    /**
     * Checks if cr has only 1 record for each wave.
     *
     * @param CASEW1
     * @param cr
     * @return true iff cr has only 1 record for each wave.
     */
    protected boolean process1(short CASEW1, WaAS_Combined_Record cr) {
        boolean r;
        r = true;
        if (cr.w2Records.size() > 1) {
            env.log("There are multiple Wave 2 records for CASEW1 " + CASEW1);
            r = false;
        }
        Short CASEW2;
        Iterator<Short> ite2;
        ite2 = cr.w2Records.keySet().iterator();
        while (ite2.hasNext()) {
            CASEW2 = ite2.next();
            //WaAS_Wave2_Record w2rec;
            //w2rec = cr.w2Records.get(CASEW2);
            String m3;
            m3 = "There are multiple Wave 3 records for "
                    + "CASEW2 " + CASEW2 + " in CASEW1 " + CASEW1;
            if (cr.w3Records.containsKey(CASEW2)) {
                HashMap<Short, WaAS_Wave3_Record> w3_2;
                w3_2 = cr.w3Records.get(CASEW2);
                if (w3_2.size() > 1) {
                    env.log(m3);
                    r = false;
                } else {
                    Short CASEW3;
                    Iterator<Short> ite3;
                    ite3 = w3_2.keySet().iterator();
                    while (ite3.hasNext()) {
                        CASEW3 = ite3.next();
                        //WaAS_Wave3_Record w3rec;
                        //w3rec = w3_2.get(CASEW3);
                        String m4;
                        m4 = "There are multiple Wave 4 records for "
                                + "CASEW3 " + CASEW3
                                + " in CASEW2 " + CASEW2
                                + " in CASEW1 " + CASEW1;
                        if (cr.w4Records.containsKey(CASEW2)) {
                            HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                            w4_2 = cr.w4Records.get(CASEW2);
                            if (w4_2.containsKey(CASEW3)) {
                                HashMap<Short, WaAS_Wave4_Record> w4_3;
                                w4_3 = w4_2.get(CASEW3);
                                if (w4_3.size() > 1) {
                                    env.log(m4);
                                    r = false;
                                } else {
                                    Iterator<Short> ite4;
                                    ite4 = w4_3.keySet().iterator();
                                    while (ite4.hasNext()) {
                                        Short CASEW4;
                                        CASEW4 = ite4.next();
                                        //WaAS_Wave4_Record w4rec;
                                        //w4rec = w4_3.get(CASEW4);
                                        String m5;
                                        m5 = "There are multiple Wave 5 records for "
                                                + "CASEW4 " + CASEW4
                                                + " in CASEW3 " + CASEW3
                                                + " in CASEW2 " + CASEW2
                                                + " in CASEW1 " + CASEW1;
                                        if (cr.w5Records.containsKey(CASEW2)) {
                                            HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>> w5_2;
                                            w5_2 = cr.w5Records.get(CASEW2);
                                            if (w5_2.containsKey(CASEW3)) {
                                                HashMap<Short, HashMap<Short, WaAS_Wave5_Record>> w5_3;
                                                w5_3 = w5_2.get(CASEW3);
                                                if (w5_3.containsKey(CASEW4)) {
                                                    HashMap<Short, WaAS_Wave5_Record> w5_4;
                                                    w5_4 = w5_3.get(CASEW4);
                                                    if (w5_4.size() > 1) {
                                                        env.log(m5);
                                                        r = false;
                                                    } else {
//                                                        Iterator<Short> ite5;
//                                                        ite5 = w5_4.keySet().iterator();
//                                                        while (ite5.hasNext()) {
//                                                            Short CASEW5;
//                                                            CASEW5 = ite5.next();
//                                                            WaAS_Wave5_Record w5rec;
//                                                            w5rec = w5_4.get(CASEW5);
//                                                        }
                                                    }
                                                } else {
                                                    env.log("!w5_3.containsKey(CASEW4) " + m5);
                                                }
                                            } else {
                                                env.log("!w5_2.containsKey(CASEW3) " + m5);
                                                r = false;
                                            }
                                        } else {
                                            env.log("!cr.w5Records.containsKey(CASEW2) " + m5);
                                            r = false;
                                        }
                                    }
                                }
                            } else {
                                env.log("!w4_2.containsKey(CASEW3) " + m4);
                                r = false;
                            }
                        } else {
                            env.log("!cr.w4Records.containsKey(CASEW2) " + m4);
                            r = false;
                        }
                    }
                }
            } else {
                env.log(m3);
            }
        }
        return r;
    }

    /**
     * Checks if cr has records for each wave.
     *
     * @param CASEW1
     * @param cr
     * @return true iff cr has records for each wave.
     */
    protected boolean process0(short CASEW1, WaAS_Combined_Record cr) {
        boolean r;
        r = true;
        if (cr.w1Record == null) {
            env.log("There is no Wave 1 record for CASEW1 " + CASEW1);
            r = false;
        }
        if (cr.w2Records.isEmpty()) {
            env.log("There are no Wave 2 records for CASEW1 " + CASEW1);
            r = false;
        }
        Short CASEW2;
        Iterator<Short> ite2;
        ite2 = cr.w2Records.keySet().iterator();
        while (ite2.hasNext()) {
            CASEW2 = ite2.next();
            //WaAS_Wave2_Record w2rec;
            //w2rec = cr.w2Records.get(CASEW2);
            String m3;
            m3 = "There are no Wave 3 records for "
                    + "CASEW2 " + CASEW2 + " in CASEW1 " + CASEW1;
            if (cr.w3Records.containsKey(CASEW2)) {
                HashMap<Short, WaAS_Wave3_Record> w3_2;
                w3_2 = cr.w3Records.get(CASEW2);
                Short CASEW3;
                Iterator<Short> ite3;
                ite3 = w3_2.keySet().iterator();
                while (ite3.hasNext()) {
                    CASEW3 = ite3.next();
                    //WaAS_Wave3_Record w3rec;
                    //w3rec = w3_2.get(CASEW3);
                    String m4;
                    m4 = "There are no Wave 4 records for "
                            + "CASEW3 " + CASEW3
                            + " in CASEW2 " + CASEW2
                            + " in CASEW1 " + CASEW1;
                    if (cr.w4Records.containsKey(CASEW2)) {
                        HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                        w4_2 = cr.w4Records.get(CASEW2);
                        if (w4_2.containsKey(CASEW3)) {
                            HashMap<Short, WaAS_Wave4_Record> w4_3;
                            w4_3 = w4_2.get(CASEW3);
                            Iterator<Short> ite4;
                            ite4 = w4_3.keySet().iterator();
                            while (ite4.hasNext()) {
                                Short CASEW4;
                                CASEW4 = ite4.next();
                                //WaAS_Wave4_Record w4rec;
                                //w4rec = w4_3.get(CASEW4);
                                String m5;
                                m5 = "There are no Wave 5 records for "
                                        + "CASEW4 " + CASEW4
                                        + " in CASEW3 " + CASEW3
                                        + " in CASEW2 " + CASEW2
                                        + " in CASEW1 " + CASEW1;
                                if (cr.w5Records.containsKey(CASEW2)) {
                                    HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>> w5_2;
                                    w5_2 = cr.w5Records.get(CASEW2);
                                    if (w5_2.containsKey(CASEW3)) {
                                        HashMap<Short, HashMap<Short, WaAS_Wave5_Record>> w5_3;
                                        w5_3 = w5_2.get(CASEW3);
                                        if (w5_3.containsKey(CASEW4)) {
                                            HashMap<Short, WaAS_Wave5_Record> w5_4;
                                            w5_4 = w5_3.get(CASEW4);
//                                            Iterator<Short> ite5;
//                                            ite5 = w5_4.keySet().iterator();
//                                            while (ite5.hasNext()) {
//                                                Short CASEW5;
//                                                CASEW5 = ite5.next();
//                                                WaAS_Wave5_Record w5rec;
//                                                w5rec = w5_4.get(CASEW5);
//                                            }
                                        } else {
                                            env.log("!w5_3.containsKey(CASEW4) " + m5);
                                        }
                                    } else {
                                        env.log("!w5_2.containsKey(CASEW3) " + m5);
                                        r = false;
                                    }
                                } else {
                                    env.log("!cr.w5Records.containsKey(CASEW2) " + m5);
                                    r = false;
                                }
                            }
                        } else {
                            env.log("!w4_2.containsKey(CASEW3) " + m4);
                            r = false;
                        }
                    } else {
                        env.log("!cr.w4Records.containsKey(CASEW2) " + m4);
                        r = false;
                    }
                }
            } else {
                env.log("!cr.w3Records.containsKey(CASEW2) " + m3);
                r = false;
            }
        }
        return r;
    }

    protected void addVariable(String s, TreeMap<Integer, String> vIDToVName,
            TreeMap<String, Integer> vNameToVID) {
        vIDToVName.put(0, s);
        vNameToVID.put(s, 0);
    }

    /**
     * Get the total DVLUKVAL in subset.
     *
     * @param subset
     * @param wave
     * @return
     */
    public long getDVLUKVAL(HashSet<Short> subset, byte wave) {
//      Value label information for DVLUKVal
//	Value = -9.0	Label = Don t know
//	Value = -8.0	Label = Refused
//	Value = -7.0	Label = Does not apply
//	Value = -6.0	Label = Error Partial
        // For brevity/convenience.
        long tDVLUKVAL;
        if (wave == env.W1) {
            tDVLUKVAL = data.data.keySet().stream().mapToLong(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                long cDVLUKVAL = c.getData().keySet().stream().mapToLong(CASEW1 -> {
                    int DVLUKVAL = 0;
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        DVLUKVAL = cr.w1Record.getHhold().getDVLUKVAL();
                        if (DVLUKVAL == Integer.MIN_VALUE) {
                            DVLUKVAL = 0;
                        }
                    }
                    return DVLUKVAL;
                }).sum();
                data.clearCollection(cID);
                return cDVLUKVAL;
            }).sum();
        } else if (wave == env.W2) {
            tDVLUKVAL = data.data.keySet().stream().mapToLong(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                long cDVLUKVAL = c.getData().keySet().stream().mapToLong(CASEW1 -> {
                    int DVLUKVAL = 0;
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        HashMap<Short, WaAS_Wave2_Record> w2Records;
                        w2Records = cr.w2Records;
                        Short CASEW2;
                        Iterator<Short> ite;
                        ite = w2Records.keySet().iterator();
                        while (ite.hasNext()) {
                            CASEW2 = ite.next();
                            int DVLUKVALW2 = w2Records.get(CASEW2).getHhold().getDVLUKVAL();
                            if (DVLUKVALW2 != Integer.MIN_VALUE) {
                                DVLUKVAL += DVLUKVALW2;
                            }
                        }
                    }
                    return DVLUKVAL;
                }).sum();
                data.clearCollection(cID);
                return cDVLUKVAL;
            }).sum();
        } else if (wave == env.W3) {
            tDVLUKVAL = data.data.keySet().stream().mapToLong(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                long cDVLUKVAL = c.getData().keySet().stream().mapToLong(CASEW1 -> {
                    int DVLUKVAL = 0;
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, WaAS_Wave3_Record>> w3Records;
                        w3Records = cr.w3Records;
                        Short CASEW2;
                        Short CASEW3;
                        Iterator<Short> ite;
                        ite = w3Records.keySet().iterator();
                        while (ite.hasNext()) {
                            CASEW2 = ite.next();
                            HashMap<Short, WaAS_Wave3_Record> w3_2;
                            w3_2 = w3Records.get(CASEW2);
                            Iterator<Short> ite2;
                            ite2 = w3_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                CASEW3 = ite2.next();
                                int DVLUKVALW3 = w3_2.get(CASEW3).getHhold().getDVLUKVAL_SUM();
                                if (DVLUKVALW3 != Integer.MIN_VALUE) {
                                    DVLUKVAL += DVLUKVALW3;
                                }
                            }
                        }
                    }
                    return DVLUKVAL;
                }).sum();
                data.clearCollection(cID);
                return cDVLUKVAL;
            }).sum();
        } else if (wave == env.W4) {
            tDVLUKVAL = data.data.keySet().stream().mapToLong(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                long cDVLUKVAL = c.getData().keySet().stream().mapToLong(CASEW1 -> {
                    int DVLUKVAL = 0;
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave4_Record>>> w4Records;
                        w4Records = cr.w4Records;
                        Short CASEW2;
                        Short CASEW3;
                        Short CASEW4;
                        Iterator<Short> ite;
                        ite = w4Records.keySet().iterator();
                        while (ite.hasNext()) {
                            CASEW2 = ite.next();
                            HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                            w4_2 = w4Records.get(CASEW2);
                            Iterator<Short> ite2;
                            ite2 = w4_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                CASEW3 = ite2.next();
                                HashMap<Short, WaAS_Wave4_Record> w4_3;
                                w4_3 = w4_2.get(CASEW3);
                                Iterator<Short> ite3;
                                ite3 = w4_3.keySet().iterator();
                                while (ite3.hasNext()) {
                                    CASEW4 = ite3.next();
                                    int DVLUKVALW4 = w4_3.get(CASEW4).getHhold().getDVLUKVAL_SUM();
                                    if (DVLUKVALW4 != Integer.MIN_VALUE) {
                                        DVLUKVAL += DVLUKVALW4;
                                    }
                                }
                            }
                        }
                    }
                    return DVLUKVAL;
                }).sum();
                data.clearCollection(cID);
                return cDVLUKVAL;
            }).sum();
        } else if (wave == env.W5) {
            tDVLUKVAL = data.data.keySet().stream().mapToLong(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                long cDVLUKVAL = c.getData().keySet().stream().mapToLong(CASEW1 -> {
                    int DVLUKVAL = 0;
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>>> w5Records;
                        w5Records = cr.w5Records;
                        Short CASEW2;
                        Short CASEW3;
                        Short CASEW4;
                        Short CASEW5;
                        Iterator<Short> ite;
                        ite = w5Records.keySet().iterator();
                        while (ite.hasNext()) {
                            CASEW2 = ite.next();
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
                                        int DVLUKVALW5 = w5_4.get(CASEW5).getHhold().getDVLUKVAL_SUM();
                                        if (DVLUKVALW5 != Integer.MIN_VALUE) {
                                            DVLUKVAL += DVLUKVALW5;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return DVLUKVAL;
                }).sum();
                data.clearCollection(cID);
                return cDVLUKVAL;
            }).sum();
        } else {
            tDVLUKVAL = 0;
        }
        env.log("Total (Hhold aggregate) DVLUKVAL in Wave " + wave + " " + tDVLUKVAL);
        return tDVLUKVAL;
    }

    /**
     * Get FINCVB.
     *
     * @param subset
     * @param wave
     * @return
     */
    public long getFINCVB(HashSet<Short> subset, byte wave) {
        // For brevity/convenience.
        long tFINCVB;
        if (wave == env.W1) {
            tFINCVB = data.data.keySet().stream().mapToLong(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                long cFINCVB = c.getData().keySet().stream().mapToLong(CASEW1 -> {
                    byte FINCVB = 0;
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        ArrayList<WaAS_Wave1_PERSON_Record> w1p;
                        w1p = cr.w1Record.getPeople();
                        Iterator<WaAS_Wave1_PERSON_Record> ite;
                        ite = w1p.iterator();
                        while (ite.hasNext()) {
                            FINCVB = ite.next().getFINCVB();
                            if (FINCVB == Byte.MIN_VALUE) {
                                FINCVB = 0;
                            }
                        }
                    }
                    return FINCVB;
                }).sum();
                data.clearCollection(cID);
                return cFINCVB;
            }).sum();
        } else if (wave == env.W2) {
            tFINCVB = data.data.keySet().stream().mapToLong(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                long cFINCVB = c.getData().keySet().stream().mapToLong(CASEW1 -> {
                    byte FINCVB = 0;
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        HashMap<Short, WaAS_Wave2_Record> w2Records;
                        w2Records = cr.w2Records;
                        Short CASEW2;
                        Iterator<Short> ite;
                        ite = w2Records.keySet().iterator();
                        while (ite.hasNext()) {
                            CASEW2 = ite.next();
                            ArrayList<WaAS_Wave2_PERSON_Record> w2p;
                            w2p = w2Records.get(CASEW2).getPeople();
                            Iterator<WaAS_Wave2_PERSON_Record> ite2;
                            ite2 = w2p.iterator();
                            while (ite2.hasNext()) {
                                FINCVB = ite2.next().getFINCVB();
                                if (FINCVB == Byte.MIN_VALUE) {
                                    FINCVB = 0;
                                }
                            }
                        }
                    }
                    return FINCVB;
                }).sum();
                data.clearCollection(cID);
                return cFINCVB;
            }).sum();
        } else if (wave == env.W3) {
            tFINCVB = data.data.keySet().stream().mapToLong(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                long cFINCVB = c.getData().keySet().stream().mapToLong(CASEW1 -> {
                    byte FINCVB = 0;
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, WaAS_Wave3_Record>> w3Records;
                        w3Records = cr.w3Records;
                        Short CASEW2;
                        Short CASEW3;
                        Iterator<Short> ite;
                        ite = w3Records.keySet().iterator();
                        while (ite.hasNext()) {
                            CASEW2 = ite.next();
                            HashMap<Short, WaAS_Wave3_Record> w3_2;
                            w3_2 = w3Records.get(CASEW2);
                            Iterator<Short> ite2;
                            ite2 = w3_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                CASEW3 = ite2.next();
                                ArrayList<WaAS_Wave3_PERSON_Record> w3p;
                                w3p = w3_2.get(CASEW3).getPeople();
                                Iterator<WaAS_Wave3_PERSON_Record> ite3;
                                ite3 = w3p.iterator();
                                while (ite3.hasNext()) {
                                    FINCVB = ite3.next().getFINCVB();
                                    if (FINCVB == Byte.MIN_VALUE) {
                                        FINCVB = 0;
                                    }
                                }
                            }
                        }
                    }
                    return FINCVB;
                }).sum();
                data.clearCollection(cID);
                return cFINCVB;
            }).sum();
        } else if (wave == env.W4) {
            tFINCVB = data.data.keySet().stream().mapToLong(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                long cFINCVB = c.getData().keySet().stream().mapToLong(CASEW1 -> {
                    byte FINCVB = 0;
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave4_Record>>> w4Records;
                        w4Records = cr.w4Records;
                        Short CASEW2;
                        Short CASEW3;
                        Short CASEW4;
                        Iterator<Short> ite;
                        ite = w4Records.keySet().iterator();
                        while (ite.hasNext()) {
                            CASEW2 = ite.next();
                            HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                            w4_2 = w4Records.get(CASEW2);
                            Iterator<Short> ite2;
                            ite2 = w4_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                CASEW3 = ite2.next();
                                HashMap<Short, WaAS_Wave4_Record> w4_3;
                                w4_3 = w4_2.get(CASEW3);
                                Iterator<Short> ite3;
                                ite3 = w4_3.keySet().iterator();
                                while (ite3.hasNext()) {
                                    CASEW4 = ite3.next();
                                    ArrayList<WaAS_Wave4_PERSON_Record> w4p;
                                    w4p = w4_3.get(CASEW4).getPeople();
                                    Iterator<WaAS_Wave4_PERSON_Record> ite4;
                                    ite4 = w4p.iterator();
                                    while (ite4.hasNext()) {
                                        FINCVB = ite4.next().getFINCVB();
                                        if (FINCVB == Byte.MIN_VALUE) {
                                            FINCVB = 0;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return FINCVB;
                }).sum();
                data.clearCollection(cID);
                return cFINCVB;
            }).sum();
        } else if (wave == env.W5) {
            tFINCVB = data.data.keySet().stream().mapToLong(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                long cFINCVB = c.getData().keySet().stream().mapToLong(CASEW1 -> {
                    byte FINCVB = 0;
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>>> w5Records;
                        w5Records = cr.w5Records;
                        Short CASEW2;
                        Short CASEW3;
                        Short CASEW4;
                        Short CASEW5;
                        Iterator<Short> ite;
                        ite = w5Records.keySet().iterator();
                        while (ite.hasNext()) {
                            CASEW2 = ite.next();
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
                                        ArrayList<WaAS_Wave5_PERSON_Record> w5p;
                                        w5p = w5_4.get(CASEW5).getPeople();
                                        Iterator<WaAS_Wave5_PERSON_Record> ite5;
                                        ite5 = w5p.iterator();
                                        while (ite5.hasNext()) {
                                            FINCVB = ite5.next().getFINCVB();
                                            if (FINCVB == Byte.MIN_VALUE) {
                                                FINCVB = 0;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return FINCVB;
                }).sum();
                data.clearCollection(cID);
                return cFINCVB;
            }).sum();
        } else {
            tFINCVB = 0;
        }
        env.log("Total (Person aggregate) FINCVB in Wave " + wave + " " + tFINCVB);
        return tFINCVB;
    }

    /**
     * Get the total HPRICEB in subset.
     *
     * @param subset
     * @param GORSubsets
     * @param GORLookups
     * @param wave
     * @return Map with keys as GOR and Values as map with keys as CASEWX and
     * values as Houseprices.
     */
    public HashMap<Byte, HashMap<Short, Integer>> getHPRICE(
            HashSet<Short> subset,
            HashMap<Byte, HashSet<Short>>[] GORSubsets,
            HashMap<Short, Byte>[] GORLookups, byte wave) {
        // Initialise result
        HashMap<Byte, HashMap<Short, Integer>> r;
        r = new HashMap<>();
        Iterator<Byte> ite;
        ite = GORSubsets[wave - 1].keySet().iterator();
        while (ite.hasNext()) {
            Byte GOR;
            GOR = ite.next();
            r.put(GOR, new HashMap<>());
        }
        // For brevity/convenience.
        if (wave == env.W1) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        WaAS_Wave1_HHOLD_Record w1;
                        w1 = cr.w1Record.getHhold();
                        Byte GOR = GORLookups[wave - 1].get(CASEW1);
                        int HPRICE = w1.getHPRICE();
                        if (HPRICE < 0) {
                            byte HPRICEB = w1.getHPRICEB();
                            if (HPRICEB > 0) {
                                HPRICE = Wave1Or2HPRICEBLookup.get(HPRICEB);
                                Generic_Collections.addToMap(r, GOR, CASEW1, HPRICE);
                            }
                        } else {
                            Generic_Collections.addToMap(r, GOR, CASEW1, HPRICE);
                        }
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == env.W2) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        HashMap<Short, WaAS_Wave2_Record> w2Records;
                        w2Records = cr.w2Records;
                        Short CASEW2;
                        Iterator<Short> ite2;
                        ite2 = w2Records.keySet().iterator();
                        while (ite2.hasNext()) {
                            CASEW2 = ite2.next();
                            Byte GOR = GORLookups[wave - 1].get(CASEW2);
                            WaAS_Wave2_HHOLD_Record w2;
                            w2 = w2Records.get(CASEW2).getHhold();
                            int HPRICE = w2.getHPRICE();
                            if (HPRICE < 0) {
                                byte HPRICEB = w2.getHPRICEB();
                                if (HPRICEB > 0) {
                                    HPRICE = Wave1Or2HPRICEBLookup.get(HPRICEB);
                                    Generic_Collections.addToMap(r, GOR, CASEW2, HPRICE);
                                }
                            } else {
                                Generic_Collections.addToMap(r, GOR, CASEW2, HPRICE);
                            }
                        }
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == env.W3) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, WaAS_Wave3_Record>> w3Records;
                        w3Records = cr.w3Records;
                        Short CASEW2;
                        Short CASEW3;
                        Iterator<Short> ite1;
                        ite1 = w3Records.keySet().iterator();
                        while (ite1.hasNext()) {
                            CASEW2 = ite1.next();
                            HashMap<Short, WaAS_Wave3_Record> w3_2;
                            w3_2 = w3Records.get(CASEW2);
                            Iterator<Short> ite2;
                            ite2 = w3_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                CASEW3 = ite2.next();
                                Byte GOR = GORLookups[wave - 1].get(CASEW3);
                                WaAS_Wave3_HHOLD_Record w3;
                                w3 = w3_2.get(CASEW3).getHhold();
                                int HPRICE = w3.getHPRICE();
                                if (HPRICE < 0) {
                                    byte HPRICEB = w3.getHPRICEB();
                                    if (HPRICEB > 0) {
                                        HPRICE = Wave3Or4Or5HPRICEBLookup.get(HPRICEB);
                                        Generic_Collections.addToMap(r, GOR, CASEW3, HPRICE);
                                    }
                                } else {
                                    Generic_Collections.addToMap(r, GOR, CASEW3, HPRICE);
                                }
                            }
                        }
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == env.W4) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr;
                        cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave4_Record>>> w4Records;
                        w4Records = cr.w4Records;
                        Short CASEW2;
                        Short CASEW3;
                        Short CASEW4;
                        Iterator<Short> ite1;
                        ite1 = w4Records.keySet().iterator();
                        while (ite1.hasNext()) {
                            CASEW2 = ite1.next();
                            HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                            w4_2 = w4Records.get(CASEW2);
                            Iterator<Short> ite2;
                            ite2 = w4_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                CASEW3 = ite2.next();
                                HashMap<Short, WaAS_Wave4_Record> w4_3;
                                w4_3 = w4_2.get(CASEW3);
                                Iterator<Short> ite3;
                                ite3 = w4_3.keySet().iterator();
                                while (ite3.hasNext()) {
                                    CASEW4 = ite3.next();
                                    Byte GOR = GORLookups[wave - 1].get(CASEW4);
                                    WaAS_Wave4_HHOLD_Record w4;
                                    w4 = w4_3.get(CASEW4).getHhold();
                                    int HPRICE = w4.getHPRICE();
                                    if (HPRICE < 0) {
                                        byte HPRICEB = w4.getHPRICEB();
                                        if (HPRICEB > 0) {
                                            HPRICE = Wave3Or4Or5HPRICEBLookup.get(HPRICEB);
                                            Generic_Collections.addToMap(r, GOR, CASEW4, HPRICE);
                                        }
                                    } else {
                                        Generic_Collections.addToMap(r, GOR, CASEW4, HPRICE);
                                    }
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
                                        int HPRICE = w5.getHPRICE();
                                        if (HPRICE < 0) {
                                            byte HPRICEB = w5.getHPRICEB();
                                            if (HPRICEB > 0) {
                                                HPRICE = Wave3Or4Or5HPRICEBLookup.get(HPRICEB);
                                                Generic_Collections.addToMap(r, GOR, CASEW5, HPRICE);
                                            }
                                        } else {
                                            Generic_Collections.addToMap(r, GOR, CASEW5, HPRICE);
                                        }
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

    HashMap<Byte, Integer> Wave1Or2HPRICEBLookup;

    /**
     * <ul>
     * <li>Value = 1.0	Label = Less than £20,000</li>
     * <li>Value = 2.0	Label = £20,000 to £39,999</li>
     * <li>Value = 3.0	Label = £40,000 to £59,999</li>
     * <li>Value = 4.0	Label = £60,000 to £99,999</li>
     * <li>Value = 5.0	Label = £100,000 to £149,999</li>
     * <li>Value = 6.0	Label = £150,000 to £199,999</li>
     * <li>Value = 7.0	Label = £200,000 to £249,999</li>
     * <li>Value = 8.0	Label = £250,000 to £299,999</li>
     * <li>Value = 9.0	Label = £300,000 to £499,999</li>
     * <li>Value = 10.0	Label = £500,000 or more</li>
     * </ul>
     *
     * @return
     */
    public HashMap<Byte, Integer> getWave1Or2HPRICEBLookup() {
        if (Wave1Or2HPRICEBLookup == null) {
            Wave1Or2HPRICEBLookup = new HashMap<>();
            Wave1Or2HPRICEBLookup.put((byte) 1, 10000);
            Wave1Or2HPRICEBLookup.put((byte) 2, 30000);
            Wave1Or2HPRICEBLookup.put((byte) 3, 55000);
            Wave1Or2HPRICEBLookup.put((byte) 4, 80000);
            Wave1Or2HPRICEBLookup.put((byte) 5, 125000);
            Wave1Or2HPRICEBLookup.put((byte) 6, 175000);
            Wave1Or2HPRICEBLookup.put((byte) 7, 225000);
            Wave1Or2HPRICEBLookup.put((byte) 8, 275000);
            Wave1Or2HPRICEBLookup.put((byte) 9, 400000);
            Wave1Or2HPRICEBLookup.put((byte) 10, 600000);
        }
        return Wave1Or2HPRICEBLookup;
    }

    HashMap<Byte, Integer> Wave3Or4Or5HPRICEBLookup;

    /**
     * <ul>
     * <li>Value = 1.0	Label = Less than £60,000</li>
     * <li>Value = 2.0	Label = £60,000 to £99,999</li>
     * <li>Value = 3.0	Label = £100,000 to £149,999</li>
     * <li>Value = 4.0	Label = £150,000 to £199,999</li>
     * <li>Value = 5.0	Label = £200,000 to £249,999</li>
     * <li>Value = 6.0	Label = £250,000 to £299,999</li>
     * <li>Value = 7.0	Label = £300,000 to £349,999</li>
     * <li>Value = 8.0	Label = £350,000 to £399,999</li>
     * <li>Value = 9.0	Label = £400,000 to £499,999</li>
     * <li>Value = 10.0	Label = £500,000 to £749,999</li>
     * <li>Value = 11.0	Label = £750,000 to £999,999</li>
     * <li>Value = 12.0	Label = £1 million or more</li>
     * <li>Value = -9.0	Label = Does not know</li>
     * <li>Value = -8.0	Label = No answer</li>
     * <li>Value = -7.0	Label = Does not apply</li>
     * <li>Value = -6.0	Label = Error/Partial</li>
     * </ul>
     *
     * @return
     */
    public HashMap<Byte, Integer> getWave3Or4Or5HPRICEBLookup() {
        if (Wave3Or4Or5HPRICEBLookup == null) {
            Wave3Or4Or5HPRICEBLookup = new HashMap<>();
            Wave3Or4Or5HPRICEBLookup.put((byte) 1, 30000);
            Wave3Or4Or5HPRICEBLookup.put((byte) 2, 80000);
            Wave3Or4Or5HPRICEBLookup.put((byte) 3, 125000);
            Wave3Or4Or5HPRICEBLookup.put((byte) 4, 175000);
            Wave3Or4Or5HPRICEBLookup.put((byte) 5, 225000);
            Wave3Or4Or5HPRICEBLookup.put((byte) 6, 275000);
            Wave3Or4Or5HPRICEBLookup.put((byte) 7, 325000);
            Wave3Or4Or5HPRICEBLookup.put((byte) 8, 375000);
            Wave3Or4Or5HPRICEBLookup.put((byte) 9, 450000);
            Wave3Or4Or5HPRICEBLookup.put((byte) 10, 625000);
            Wave3Or4Or5HPRICEBLookup.put((byte) 11, 875000);
            Wave3Or4Or5HPRICEBLookup.put((byte) 12, 1500000);
        }
        return Wave3Or4Or5HPRICEBLookup;
    }

    boolean doJavaCodeGeneration = false;
    boolean doLoadDataIntoCaches = false;

    /**
     *
     * @param title
     * @param xAxisLabel
     * @param yAxisLabel
     * @param variableName
     * @param changeSubset
     * @param changeAll
     * @param numberOfYAxisTicks
     * @param yIncrement
     */
    public void createLineGraph(String title, String xAxisLabel,
            String yAxisLabel, String variableName,
            TreeMap<Byte, Double> changeSubset,
            TreeMap<Byte, Double> changeAll, int numberOfYAxisTicks,
            BigDecimal yIncrement) {
        Generic_Visualisation.getHeadlessEnvironment();
        /*
         * Initialise title and File to write image to
         */
        File file;
        String format = "PNG";
        System.out.println("Title: " + title);
        Generic_Files gf = new Generic_Files();
        File outdir;
        outdir = gf.getOutputDataDir();
        String filename;
        filename = title.replace(" ", "_");
        file = new File(outdir, filename + "." + format);
        System.out.println("File: " + file.toString());
        int dataWidth = 500;
        int dataHeight = 250;
        //int numberOfYAxisTicks = 10;
        BigDecimal yMax;
        yMax = null;
        ArrayList<BigDecimal> yPin;
        yPin = new ArrayList<>();
        yPin.add(BigDecimal.ZERO);
        //BigDecimal yIncrement = BigDecimal.ONE;
        //BigDecimal yIncrement = null; // Setting this to null means that numberOfYAxisTicks is used.
        //BigDecimal yIncrement = new BigDecimal("20000");
        //int yAxisStartOfEndInterval = 60;
        int decimalPlacePrecisionForCalculations = 10;
        int decimalPlacePrecisionForDisplay = 3;
        RoundingMode roundingMode = RoundingMode.HALF_UP;
        ExecutorService es = Executors.newSingleThreadExecutor();
        WIGB_LineGraph chart = new WIGB_LineGraph(es, file, format, title,
                dataWidth, dataHeight, xAxisLabel, yAxisLabel,
                yMax, yPin, yIncrement, numberOfYAxisTicks,
                decimalPlacePrecisionForCalculations,
                decimalPlacePrecisionForDisplay, roundingMode);
        chart.setData(variableName, gors, GORNameLookup, changeSubset, changeAll);
        chart.run();

        Future future = chart.future;
        Generic_Execution.shutdownExecutorService(es, future, chart);
    }
}
