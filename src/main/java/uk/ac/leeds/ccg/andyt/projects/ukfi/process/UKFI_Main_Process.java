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
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
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
import uk.ac.leeds.ccg.andyt.projects.ukfi.core.UKFI_Environment;
import uk.ac.leeds.ccg.andyt.projects.ukfi.core.UKFI_Object;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Collection;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_CollectionSimple;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Combined_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Combined_Record_Simple;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_HHOLD_Handler;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Handler;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_PERSON_Handler;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave2_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave3_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave4_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave5_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave1_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave2_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave3_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave4_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave5_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave1_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave2_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave3_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave4_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave5_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.execution.Generic_Execution;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_Files;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;
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
    protected final WaAS_HHOLD_Handler hh;

    /**
     * Subset of all records that have the same household composition.
     */
    HashSet<Short> subset0;

    HashSet<Short> subset;

    /**
     * Subset of all records that have the same household composition.
     */
    HashSet<Short>[] subsets;

    ArrayList<Byte> gors;
    /**
     * Government Office Region (GOR) name lookup looks up the name from the
     * numerical code.
     */
    TreeMap<Byte, String> GORNameLookup;
    HashMap<Byte, HashSet<Short>>[] GORSubsets;
    HashMap<Short, Byte>[] GORLookups;

    public UKFI_Main_Process(UKFI_Environment env) {
        super(env);
        files = env.files;
        this.data = env.data;
        hh = env.we.hh;
    }

    public UKFI_Main_Process(UKFI_Main_Process p) {
        data = p.data;
        files = p.files;
        env = p.env;
        subset0 = p.subset0;
        subset = p.subset;
        subsets = p.subsets;
        gors = p.gors;
        GORNameLookup = p.GORNameLookup;
        GORSubsets = p.GORSubsets;
        GORLookups = p.GORLookups;
        hh = env.we.hh;
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

        /**
         * Init subset.
         */
        subset0 = hh.getStableHouseholdCompositionSubset(data);

        //env.we.getPersonHandler();
        WaAS_PERSON_Handler pH = new WaAS_PERSON_Handler(env.we);

        //subset = subset0;
        //String name = "";
        //int min
        BigDecimal yIncrement = new BigDecimal("20000");

        boolean doProcessDataForAnastasia = true;
        if (doProcessDataForAnastasia) {
            File dataForAnastasia = new File(files.getOutputDataDir(), "Anastasia.csv");
            /**
             * Write out header.
             */
            try (PrintWriter pw = Generic_IO.getPrintWriter(dataForAnastasia, false)) {
                /**
                 * Write out header.
                 */
                String h = "CASEW1";
                for (int w = 1; w < 6; w++) {
                    h += ",GORW" + w + ",NUMADULTW" + w + ",NUMHHLDRW" + w
                            + ",TOTWLTHW" + w + ",HFINW_SUMW" + w + ",HPROPWW" + w
                            //+ ",ERECTAXW" + w + ",SEESMHRPW" + w +
                            + ",HHTYPEW" + w + ",TEN1W" + w + ",LLORDW" + w
                            + ",HSETYPEW" + w + ",GCONTVBW" + w + ",VCARN" + w
                            + ",HBEDRM" + w;
                    if (w > 2) {
                        h += ",DVTOTGIRW" + w;
                    }
                    if (w > 3) {
                        h += ",DVBENEFITANNUAL_AGGRW" + w;
                    }

                }
                h += ",HBFROMW5,HRTBEVW5,HHOSCHW5";
                pw.println(h);
                /**
                 * Write out values.
                 */
                HashMap<Short, WaAS_CollectionSimple> cs = env.we.data.dataSimple;
                //for (int decile = 1; decile < 10; decile++) {
                //    subset = subsets[decile - 1];
                subset = subset0;
                Iterator<Short> ite = cs.keySet().iterator();
                while (ite.hasNext()) {
                    Short cID = ite.next();
                    env.log("Collection ID " + cID);
                    WaAS_CollectionSimple c = env.we.data.getCollectionSimple(cID);
                    HashMap<Short, WaAS_Combined_Record_Simple> cr = c.getData();
                    Iterator<Short> ite2 = cr.keySet().iterator();
                    while (ite2.hasNext()) {
                        short CASEW1 = ite2.next();
                        if (subset.contains(CASEW1)) {
                            String line = "" + CASEW1 + ",";
                            WaAS_Combined_Record_Simple r = cr.get(CASEW1);
                            /**
                             * Wave 1
                             */
                            WaAS_Wave1_HHOLD_Record w1hrec = r.w1Record.getHhold();
                            line += w1hrec.getGOR() + ",";
                            line += w1hrec.getNUMADULT() + ",";
                            line += w1hrec.getNUMHHLDR() + ",";
                            line += w1hrec.getTOTWLTH() + ",";
                            line += w1hrec.getHFINW_SUM() + ",";
                            line += w1hrec.getHPROPW() + ",";
//                        line += w1hrec.getERECTAX() + ",";
//                        //w1hrec.getDVTOTNIR();
//                        // Add SEESMHRP
//                        boolean doneHRP = false;
//                        ArrayList<WaAS_Wave1_PERSON_Record> w1ps = r.w1Record.getPeople();
//                        Iterator<WaAS_Wave1_PERSON_Record> wlpsi = w1ps.iterator();
//                        while (wlpsi.hasNext()) {
//                            WaAS_Wave1_PERSON_Record w1p = wlpsi.next();
//                            if (w1p.getISHRP()) {
//                                line += w1p.getSEESM() + ",";
//                                doneHRP = true;
//                            }
//                        }
//                        if (!doneHRP) {
//                            line += ",";
//                        }
//                        doneHRP = false;
                            line += w1hrec.getHHOLDTYPE() + ",";
                            line += w1hrec.getTEN1() + ",";
                            line += w1hrec.getLLORD() + ",";
                            line += w1hrec.getHSETYPE() + ",";
                            line += w1hrec.getGCONTVB() + ",";
                            line += w1hrec.getVCARN() + ",";
                            line += w1hrec.getHBEDRM() + ",";
                            /**
                             * Wave 2
                             */
                            WaAS_Wave2_HHOLD_Record w2hrec = r.w2Record.getHhold();
                            line += w2hrec.getGOR() + ",";
                            line += w2hrec.getNUMADULT() + ",";
                            line += w2hrec.getNUMHHLDR() + ",";
                            line += w2hrec.getTOTWLTH() + ",";
                            line += w2hrec.getHFINW_SUM() + ",";
                            line += w2hrec.getHPROPW() + ",";
//                        line += w2hrec.getERECTAX() + ",";
//                        //w2hrec.getDVTOTNIR();
//                        // Add SEESMHRP
//                        ArrayList<WaAS_Wave2_PERSON_Record> w2ps = r.w2Record.getPeople();
//                        Iterator<WaAS_Wave2_PERSON_Record> w2psi = w2ps.iterator();
//                        while (w2psi.hasNext()) {
//                            WaAS_Wave2_PERSON_Record w2p = w2psi.next();
//                            if (w2p.getISHRP()) {
//                                line += w2p.getSEESM() + ",";
//                                doneHRP = true;
//                            }
//                        }
//                        if (!doneHRP) {
//                            line += ",";
//                        }
//                        doneHRP = false;
                            line += w2hrec.getHHOLDTYPE() + ",";
                            line += w2hrec.getTEN1() + ",";
                            line += w2hrec.getLLORD() + ",";
                            line += w2hrec.getHSETYPE() + ",";
                            line += w2hrec.getGCONTVB() + ",";
                            line += w2hrec.getVCARN() + ",";
                            line += w2hrec.getHBEDRM() + ",";
                            /**
                             * Wave 3
                             */
                            WaAS_Wave3_HHOLD_Record w3hrec = r.w3Record.getHhold();
                            line += w3hrec.getGOR() + ",";
                            line += w3hrec.getNUMADULT() + ",";
                            line += w3hrec.getNUMHHLDR() + ",";
                            line += w3hrec.getTOTWLTH() + ",";
                            line += w3hrec.getHFINW_SUM() + ",";
                            line += w3hrec.getHPROPW() + ",";
//                        line += w3hrec.getERECTAX() + ",";
//                        //w3hrec.getDVTOTNIR();
//                        ArrayList<WaAS_Wave3_PERSON_Record> w3ps = r.w3Record.getPeople();
//                        Iterator<WaAS_Wave3_PERSON_Record> w3psi = w3ps.iterator();
//                        while (wlpsi.hasNext()) {
//                            WaAS_Wave3_PERSON_Record w3p = w3psi.next();
//                            if (w3p.getISHRP()) {
//                                line += w3p.getSEESM() + ",";
//                                doneHRP = true;
//                            }
//                        }
//                        if (!doneHRP) {
//                            line += ",";
//                        }
//                        doneHRP = false;
                            line += w3hrec.getHHOLDTYPE() + ",";
                            line += w3hrec.getTEN1() + ",";
                            line += w3hrec.getLLORD() + ",";
                            line += w3hrec.getHSETYPE() + ",";
                            line += w3hrec.getGCONTVB() + ",";
                            line += w3hrec.getVCARN() + ",";
                            line += w3hrec.getHBEDRM() + ",";
                            line += w3hrec.getDVTOTGIR() + ",";
                            /**
                             * Wave 4
                             */
                            WaAS_Wave4_HHOLD_Record w4hrec = r.w4Record.getHhold();
                            line += w4hrec.getGOR() + ",";
                            line += w4hrec.getNUMADULT() + ",";
                            line += w4hrec.getNUMHHLDR() + ",";
                            line += w4hrec.getTOTWLTH() + ",";
                            line += w4hrec.getHFINW_SUM() + ",";
                            line += w4hrec.getHPROPW() + ",";
//                        line += w4hrec.getERECTAX() + ",";
//                        //w4hrec.getDVTOTNIR();
//                        ArrayList<WaAS_Wave4_PERSON_Record> w4ps = r.w4Record.getPeople();
//                        Iterator<WaAS_Wave4_PERSON_Record> w4psi = w4ps.iterator();
//                        while (wlpsi.hasNext()) {
//                            WaAS_Wave4_PERSON_Record w4p = w4psi.next();
//                            byte p_FLAG4 = w4p.getP_FLAG4();
//                            if (p_FLAG4 == 1 || p_FLAG4 == 3) {
//                                line += w4p.getSEESM() + ",";
//                                doneHRP = true;
//                            }
//                        }
//                        if (!doneHRP) {
//                            line += ",";
//                        }
//                        doneHRP = false;
                            line += w4hrec.getHHOLDTYPE() + ",";
                            line += w4hrec.getTEN1() + ",";
                            line += w4hrec.getLLORD() + ",";
                            line += w4hrec.getHSETYPE() + ",";
                            line += w4hrec.getGCONTVB() + ",";
                            line += w4hrec.getVCARN() + ",";
                            line += w4hrec.getHBEDRM() + ",";
                            line += w4hrec.getDVTOTGIR() + ",";
                            line += w4hrec.getDVBENEFITANNUAL_AGGR() + ",";
                            /**
                             * Wave 5
                             */
                            WaAS_Wave5_HHOLD_Record w5hrec = r.w5Record.getHhold();
                            line += w5hrec.getGOR() + ",";
                            line += w5hrec.getNUMADULT() + ",";
                            line += w5hrec.getNUMHHLDR() + ",";
                            line += w5hrec.getTOTWLTH() + ",";
                            line += w5hrec.getHFINW_SUM() + ",";
                            line += w5hrec.getHPROPW() + ",";
//                        line += w5hrec.getERECTAX() + ",";
//                        //w5hrec.getDVTOTNIR();
//                        ArrayList<WaAS_Wave5_PERSON_Record> w5ps = r.w5Record.getPeople();
//                        Iterator<WaAS_Wave5_PERSON_Record> w5psi = w5ps.iterator();
//                        while (w5psi.hasNext()) {
//                            WaAS_Wave5_PERSON_Record w5p = w5psi.next();
//                            byte p_FLAG4 = w5p.getP_FLAG4();
//                            if (p_FLAG4 == 1 || p_FLAG4 == 3) {
//                                line += w5p.getSEESM() + ",";
//                                doneHRP = true;
//                            }
//                        }
//                        if (!doneHRP) {
//                            line += ",";
//                        }
                            line += w5hrec.getHHOLDTYPE() + ",";
                            line += w5hrec.getTEN1() + ",";
                            line += w5hrec.getLLORD() + ",";
                            line += w5hrec.getHSETYPE() + ",";
                            line += w5hrec.getGCONTVB() + ",";
                            line += w5hrec.getVCARN() + ",";
                            line += w5hrec.getHBEDRM() + ",";
                            line += w5hrec.getDVTOTGIR() + ",";
                            line += w5hrec.getDVBENEFITANNUAL_AGGR() + ",";
                            line += w5hrec.getHBFROM() + ",";
                            line += w5hrec.getHRTBEV() + ",";
                            line += w5hrec.getHHOSCH();
                            pw.println(line);
                        }
                    }
                    env.we.data.clearCollectionSimple(cID);
                }
                //}
            }
        }

        TreeMap<Integer, Double> breaks = new TreeMap<>();

        boolean doInitialiseSubsets = false;
        if (doInitialiseSubsets) {
            /**
             * Init subsets. These are deciles of variable though negative and
             * zero values are treated as special case deciles.
             */
            int ndivs = 10;
            subsets = new HashSet[ndivs];
            for (int i = 0; i < ndivs; i++) {
                subsets[i] = new HashSet<>();
            }
            String loadName = WaAS_Strings.s_InW1W2W3W4W5
                    + WaAS_Strings.s_StableHouseholdCompositionSubset;
            Object[] w1 = hh.loadW1(subset0, loadName);
            TreeMap<Short, WaAS_Wave1_HHOLD_Record> w1recs;
            w1recs = (TreeMap<Short, WaAS_Wave1_HHOLD_Record>) w1[0];
            int n = w1recs.size();
            env.log("Size " + n);
            int zeroOrLessHPROPWCount = 0;
            // Initialise counts to have all the HPROPW values with counts
            TreeMap<Double, Integer> counts = new TreeMap<>();
            Iterator<Short> ites = subset0.iterator();
            while (ites.hasNext()) {
                short CASEW1 = ites.next();
                WaAS_Wave1_HHOLD_Record w1rec = w1recs.get(CASEW1);
                double HPROPW = w1rec.getHPROPW();
                if (HPROPW <= 0) {
                    zeroOrLessHPROPWCount += 1;
                }
                if (counts.containsKey(HPROPW)) {
                    counts.put(HPROPW, counts.get(HPROPW) + 1);
                } else {
                    counts.put(HPROPW, 1);
                }
            }
            int ndiv8 = (n - zeroOrLessHPROPWCount) / 8;
            env.log("ndiv8 " + ndiv8);
            Iterator<Double> ited = counts.keySet().iterator();
            double HPROPW0 = ited.next();
            double HPROPW1 = HPROPW0;
            int breakIndex = 0;
            while (ited.hasNext()) {
                HPROPW0 = ited.next();
                if (HPROPW0 == 0) {
                    env.log("Adding break[" + breakIndex + "] " + HPROPW1);
                    breaks.put(breakIndex, HPROPW1);
                    breakIndex++;
                    env.log("Adding break[" + breakIndex + "] " + HPROPW0);
                    breaks.put(breakIndex, HPROPW0);
                    breakIndex++;
                    HPROPW0 = HPROPW1;
                    break;
                }
                HPROPW1 = HPROPW0;
            }
            int divc = counts.get(HPROPW0);
            int divc0 = divc;
            while (ited.hasNext()) {
                double HPROPW = ited.next();
                int count = counts.get(HPROPW);
                //env.log("HPROPW " + HPROPW + " count " + count);
                divc += count;
                if (divc > ndiv8) {
                    env.log("HPROPW (" + HPROPW0 + ", " + HPROPW1 + ") count "
                            + divc0);
                    env.log("Adding break[" + breakIndex + "] " + HPROPW1);
                    breaks.put(breakIndex, HPROPW1);
                    breakIndex++;
                    divc -= divc0;
                    divc0 = divc;
                    HPROPW0 = HPROPW1;
                } else {
                    divc0 = divc;
                }
                HPROPW1 = HPROPW;
            }
            // Go through the data again and get the subsets
            ites = subset0.iterator();
            while (ites.hasNext()) {
                short CASEW1 = ites.next();
                WaAS_Wave1_HHOLD_Record w1rec = w1recs.get(CASEW1);
                double HPROPW = w1rec.getHPROPW();
                if (HPROPW <= breaks.get(0)) {
                    subsets[0].add(CASEW1);
                } else if (HPROPW <= breaks.get(1)) {
                    subsets[1].add(CASEW1);
                } else if (HPROPW <= breaks.get(2)) {
                    subsets[2].add(CASEW1);
                } else if (HPROPW <= breaks.get(3)) {
                    subsets[3].add(CASEW1);
                } else if (HPROPW <= breaks.get(4)) {
                    subsets[4].add(CASEW1);
                } else if (HPROPW <= breaks.get(5)) {
                    subsets[5].add(CASEW1);
                } else if (HPROPW <= breaks.get(6)) {
                    subsets[6].add(CASEW1);
                } else if (HPROPW <= breaks.get(7)) {
                    subsets[7].add(CASEW1);
                } else if (HPROPW <= breaks.get(8)) {
                    subsets[8].add(CASEW1);
                } else {
                    subsets[9].add(CASEW1);
                }
            }
        }

        gors = WaAS_Handler.getGORs();
        GORNameLookup = WaAS_Handler.getGORNameLookup();

        boolean doSumariseAndGraphDataForSubsets = false;
        if (doSumariseAndGraphDataForSubsets) {
            for (int decile = 1; decile < 10; decile++) {
                subset = subsets[decile - 1];
                String name;
                if (decile < 9) {
                    if (decile == 1) {
                        name = "Wave_1_HPROPW_LT_" + breaks.get(decile);
                    } else {
                        name = "Wave_1_HPROPW_E_" + breaks.get(decile - 1);
                    }
                } else {
                    name = "Wave_1_HPROPW_GT_" + breaks.get(decile - 2);
                }
                /**
                 * Init gors, GORNameLookup, GORSubsets and GORLookups.
                 */
                if (true) {
                    Object[] o = hh.getGORSubsetsAndLookup(name, data, gors, subset);
                    GORSubsets = (HashMap<Byte, HashSet<Short>>[]) o[0];
                    GORLookups = (HashMap<Short, Byte>[]) o[1];
                }

                /**
                 * Summarise data by GOR.
                 */
                int[] ttotals = new int[env.NWAVES];
                env.log("GOR Subsets " + name);
                env.log("NW1,NW2,NW3,NW4,NW5,GORNumber,GORName");
                Iterator<Byte> ite = gors.iterator();
                while (ite.hasNext()) {
                    String s = "";
                    int[] totals = new int[env.NWAVES];
                    byte gor = ite.next();
                    for (byte w = 0; w < env.NWAVES; w++) {
                        HashSet<Short> GORSubset = GORSubsets[w].get(gor);
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

                /**
                 * HVALUE, HPROPW
                 */
                UKFI_Process_Variable p = new UKFI_Process_Variable(this);
                p.createGraph(name, yIncrement, WaAS_Strings.s_HVALUE);
                p.createGraph(name, yIncrement, WaAS_Strings.s_TOTWLTH);
                p.createGraph(name, yIncrement, WaAS_Strings.s_HPROPW);
            }
        }
//
//        // Check some counts
//        Object[] w5 = hh.loadW5();
//        TreeMap<Short, WaAS_Wave5_HHOLD_Record> w5recs;
//        w5recs = (TreeMap<Short, WaAS_Wave5_HHOLD_Record>) w5[0];
//        ites = w5recs.keySet().iterator();
//        int countMortgage = 0;
//        int countNonMortgage = 0;
//        int countBuyWithMortgage = 0;
//        int countPartBuyWithMortgage = 0;
//        int countZeroMIntRate1W5 = 0;
//        int countPositiveMIntRate1W5 = 0;
//        int countZeroMVal1W5 = 0;
//        int countPositiveMVal1W5 = 0;
//        int countZeroMNumbNW5 = 0;
//        int countPositiveMNumbNW5 = 0;
//        int countGT1MNumbNW5 = 0;
//        while (ites.hasNext()) {
//            short CASEW5 = ites.next();
//            WaAS_Wave5_HHOLD_Record w5rec = w5recs.get(CASEW5);
//            byte ten1 = w5rec.getTEN1();
//            if (ten1 == 2 || ten1 == 3) {
//                countMortgage++;
//                if (ten1 == 2) {
//                    countBuyWithMortgage++;
//                }
//                if (ten1 == 3) {
//                    countPartBuyWithMortgage++;
//                }
//                double MIntRate1W5 = w5rec.getMINTRATE1();
//                if (MIntRate1W5 == 0.0d) {
//                    countZeroMIntRate1W5++;
//                } else {
//                    if (MIntRate1W5 > 0) {
//                        countPositiveMIntRate1W5++;
//                    }
//                }
//                int MVal1W5 = w5rec.getMVAL1();
//                if (MVal1W5 == 0) {
//                    countZeroMVal1W5++;
//                } else {
//                    if (MVal1W5 > 0) {
//                        countPositiveMVal1W5++;
//                    }
//                }
//                int MNumbNW5 = w5rec.getMNUMB();
//                if (MNumbNW5 == 0) {
//                    countZeroMNumbNW5++;
//                } else {
//                    if (MNumbNW5 > 0) {
//                        countPositiveMNumbNW5++;
//                        if (MNumbNW5 > 1) {
//                            countGT1MNumbNW5++;
//                        }
//                    }
//                }
//            } else {
//                countNonMortgage++;
//            }
//        }
//        env.log("" + w5recs.size() + "\t countAllW5withW4");
//        env.log("" + countMortgage + "\t countMortgage");
//        env.log("" + countNonMortgage + "\t countNonMortgage");
//        env.log("" + countBuyWithMortgage + "\t countBuyWithMortgage");
//        env.log("" + countPartBuyWithMortgage + "\t countPartBuyWithMortgage");
//        env.log("" + countZeroMIntRate1W5 + "\t countZeroMIntRate1W5");
//        env.log("" + countPositiveMIntRate1W5 + "\t countPositiveMIntRate1W5");
//        env.log("" + countZeroMVal1W5 + "\t countZeroMVal1W5");
//        env.log("" + countPositiveMVal1W5 + "\t countPositiveMVal1W5");
//        env.log("" + countZeroMNumbNW5 + "\t countZeroMNumbNW5");
//        env.log("" + countPositiveMNumbNW5 + "\t countPositiveMNumbNW5");
//        env.log("" + countGT1MNumbNW5 + "\t countGT1MNumbNW5");
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
