/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.util;

import com.boha.rivers.data.Category;
import com.boha.rivers.data.Conditions;
import com.boha.rivers.data.Evaluation;
import com.boha.rivers.data.Evaluationinsect;
import com.boha.rivers.data.Evaluationsite;
import com.boha.rivers.data.Insect;
import com.boha.rivers.data.River;
import com.boha.rivers.data.Teammember;
import static com.boha.rivers.util.SiteAndObservationImport.log;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.ejb.Stateless;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author CodeTribe1
 */
@Stateless
@TransactionManagement(TransactionManagementType.CONTAINER)
public class DumbFilesImporter {

    @PersistenceContext
    EntityManager em;

   
    public void doDaBusiness() throws FileNotFoundException, IOException {
        File fileSite = new File("C:/Android/workspaces/rivers/dump-sites.csv");
        if (!fileSite.exists()) {
            fileSite = new File("/opt/river_data/dump-sites.csv");
        }
        if (!fileSite.exists()) {
            throw new IOException("Shape File Not found");
        }
        File fileObs = new File("C:/Android/workspaces/rivers/dump-obs.csv");
        if (!fileObs.exists()) {
            fileObs = new File("/opt/river_data/dump-obs.csv");
        }
        if (!fileObs.exists()) {
            throw new IOException("Shape File Not found");
        }

        startImport(fileSite.toString());
        addObservations(fileObs.toString());
//        printReport();
    }

    public void setSiteRivers(List<Evaluationsite> evaluationsites, HashMap<String, River> xMap) {

        log.log(Level.INFO, "######### Starting to extract sites ready for database");
        Collections.sort(evaluationsites);

        List<River> riverList = new ArrayList<>();

        for (Map.Entry<String, River> entry : xMap.entrySet()) {
            River river = entry.getValue();
            riverList.add(river);

        }
        List<Evaluationsite> goodSites = new ArrayList<>();
        Collections.sort(riverList);
        for (River river : riverList) {
            log.log(Level.INFO, "+++ processing river, id: " + river.getRiverID() + " - " + river.getRiverName().trim());
            String riverName = river.getRiverName().trim();
            for (Evaluationsite site : evaluationsites) {
                String siteRiverName = site.getRiverName().trim();
                if (riverName.equalsIgnoreCase(siteRiverName)) {
                    site.setRiver(river);
                    goodSites.add(site);
                }
            }

        }

        log.log(Level.INFO, "Sites ready for database insert: " + goodSites.size());

        Evaluationsite xx = null;
        try {
            for (Evaluationsite e : goodSites) {
                e.setDateRegistered(new Date());

                xx = e;
                try {

                    em.persist(e);
                    em.flush();
                } catch (PersistenceException ex) {
                    try {

                        log.log(Level.WARNING, "-----  Duplicate detected: " + xx.getRiverName() + " - " + xx.getSiteName());
                    } catch (Exception ex1) {
                    }
                }
            }

        } catch (Exception e) {
            try {

            } catch (Exception ex) {

            }
            log.log(Level.OFF, "Failed", e);
        }
    }

    private void startImport(String siteFilePath) throws UnsupportedEncodingException, FileNotFoundException, IOException {

        Query qc = em.createNamedQuery("Category.findByCategoryName", Category.class);
        qc.setParameter("categoryName", "Rocky Type");
        qc.setMaxResults(1);
        Category rocky = (Category) qc.getSingleResult();
        qc.setParameter("categoryName", "Sandy Type");
        qc.setMaxResults(1);
        Category sandy = (Category) qc.getSingleResult();
        List<HashMap<String, String>> hashFiltedRivers = new ArrayList<>();
        //
        File siteFile = new File(siteFilePath);
        log.log(Level.INFO, "File for import found: " + siteFile.getAbsolutePath() + " size: " + siteFile.length());
        if (!siteFile.exists()) {
            log.log(Level.SEVERE, "Import File not found");
            return;
        }
        HashMap<String, String> riverMap = new HashMap<>();
        List<Evaluationsite> evaluationsites = new ArrayList<>();
        try (BufferedReader brReadMe = new BufferedReader(new InputStreamReader(
                new FileInputStream(siteFile), "UTF-8"))) {
            String strLine = brReadMe.readLine();
            while (strLine != null) {
                if (!strLine.contains("POINT")) {
                    log.log(Level.WARNING, strLine);
                    strLine = brReadMe.readLine();
                    continue;
                }
                if (parseRiverAndSite(strLine) == null) {
                    log.log(Level.WARNING, strLine);
                    strLine = brReadMe.readLine();
                    continue;
                }
                riverMap.put(parseRiverAndSite(strLine), parseRiverAndSite(strLine));
                evaluationsites.add(parseSite(strLine, parseRiverAndSite(strLine), rocky, sandy));
                strLine = brReadMe.readLine();
            }

            match(riverMap, evaluationsites);
        }
        // System.out.println("+++++ rivers found in import file: " + riverCount);

    }

    private void addObservations(String observationFile) throws UnsupportedEncodingException, FileNotFoundException, IOException {
        File obsFile = new File(observationFile);
        log.log(Level.INFO, "\n\n$$$$$$ addObservations - File for import found: " + obsFile.getAbsolutePath() + " size: " + obsFile.length());
        if (!obsFile.exists()) {
            log.log(Level.SEVERE, "Import File not found");
            return;
        }

        try (BufferedReader brReadMe = new BufferedReader(new InputStreamReader(
                new FileInputStream(obsFile), "UTF-8"))) {
            String strLine = brReadMe.readLine();
            while (strLine != null) {
                if (!strLine.contains("POINT")) {
                    log.log(Level.INFO, strLine);
                    strLine = brReadMe.readLine();
                    continue;
                }
                parseObservation(strLine);
                strLine = brReadMe.readLine();
            }
            
        }
        log.log(Level.OFF, "Score records: {0}", i);
    }

    private static Double getDouble(String m) {
        Double d = Double.parseDouble("0");
        Pattern patt = Pattern.compile(",");
        String[] result = patt.split(m);
        if (result.length == 1) {
            return Double.parseDouble(m);
        }
        if (result.length == 2) {
            String x = result[0] + "." + result[1];
            return Double.parseDouble(x);
        }
        return d;
    }

    private int conditionIDFinder(int categoryID, double average) {

        log.log(Level.INFO, "Score : {0}", average);
        int statusCondition = 0;
        if (categoryID == 8) {
            if (average > 6.9) {
                statusCondition = Constants.UNMODIFIED_NATURAL_SAND;
            } else if (average > 5.8 && average < 6.9) {
                statusCondition = Constants.LARGELY_NATURAL_SAND;
            } else if (average > 4.9 && average < 5.8) {
                statusCondition = Constants.MODERATELY_MODIFIED_SAND;
            } else if (average > 4.3 && average < 4.9) {
                statusCondition = Constants.LARGELY_MODIFIED_SAND;
            } else if (average < 4.3) {
                statusCondition = Constants.CRITICALLY_MODIFIED_SAND;
            }
        } else if (categoryID == 9) {
            if (average > 7.9) {
                statusCondition = Constants.UNMODIFIED_NATURAL_ROCK;
            } else if (average > 6.8 && average < 7.9) {
                statusCondition = Constants.LARGELY_NATURAL_ROCK;
            } else if (average > 6.1 && average < 6.8) {
                statusCondition = Constants.MODERATELY_MODIFIED_ROCK;
            } else if (average > 5.1 && average < 6.1) {
                statusCondition = Constants.LARGELY_MODIFIED_ROCK;
            } else if (average < 5.1) {
                statusCondition = Constants.CRITICALLY_MODIFIED_ROCK;
            }
        }
        return statusCondition;
    }
    int i = 0;

    private void parseObservation(String line) {
        DateTimeFormatter dtf1 = DateTimeFormat.forPattern("yy/mm/dd");
        Pattern patt = Pattern.compile(";");
        String[] result = patt.split(line);
        Evaluation eval = new Evaluation();
        Query q = em.createNamedQuery("Evaluationsite.findByGID", Evaluationsite.class);
        q.setParameter("gID", Integer.parseInt(result[1]));
        q.setMaxResults(1);

        try {
            Evaluationsite site = (Evaluationsite) q.getSingleResult();
            eval.setEvaluationSite(site);

            eval.setConditions(em.find(Conditions.class, conditionIDFinder(site.getCategory().getCategoryId(), getDouble(result[9]))));
            eval.setScore(getDouble(result[9]));
            if (result[10] != null && !result[10].trim().isEmpty()) {
                eval.setWaterClarity(getDouble(result[10]));
            }
            if (result[11] != null && !result[11].trim().isEmpty()) {
                eval.setWaterTemperature(getDouble(result[11]));
            }
            if (result[12] != null && !result[12].trim().isEmpty()) {
                eval.setpH(getDouble(result[12]));
            }
            if (result[13] != null && !result[13].trim().isEmpty()) {
                eval.setOxygen(getDouble(result[13]));
            }

            if (result[17] != null && !result[17].trim().isEmpty()) {
                eval.setEvaluationDate(dtf1.parseDateTime(result[17]).toDate());
            }
            eval.setRemarks(result[18]);
            eval.setTeamMember(em.find(Teammember.class, 2));

            em.persist(eval);
            em.flush();
            i++;
        } catch (NoResultException e) {
            try {

                log.log(Level.SEVERE, e.getMessage());
                return;
            } catch (Exception ez) {
            }
        } catch (Exception e) {
            try {

                log.log(Level.SEVERE, e.getMessage());
            } catch (Exception ez) {
            }
        }

        int index = 19;
        try {
            if (result[index] != null && result[index].equalsIgnoreCase("TRUE")) {   //flatworms
                Insect i = em.find(Insect.class, 1);
                Evaluationinsect ei = new Evaluationinsect();
                ei.setInsect(i);
                ei.setEvaluation(eval);
                em.persist(ei);
            }
            index++;
            if (result[index] != null && result[index].equalsIgnoreCase("TRUE")) {   //worms
                Insect i = em.find(Insect.class, 2);
                Evaluationinsect ei = new Evaluationinsect();
                ei.setInsect(i);
                ei.setEvaluation(eval);
                em.persist(ei);
            }
            index++;
            if (result[index] != null && result[index].equalsIgnoreCase("TRUE")) {   //leeches
                Insect i = em.find(Insect.class, 3);
                Evaluationinsect ei = new Evaluationinsect();
                ei.setInsect(i);
                ei.setEvaluation(eval);
                em.persist(ei);
            }
            index++;
            if (result[index] != null && result[index].equalsIgnoreCase("TRUE")) {   //crabs
                Insect i = em.find(Insect.class, 4);
                Evaluationinsect ei = new Evaluationinsect();
                ei.setInsect(i);
                ei.setEvaluation(eval);
                em.persist(ei);
            }
            index++;
            if (result[index] != null && result[index].equalsIgnoreCase("TRUE")) {   //stoneflies
                Insect i = em.find(Insect.class, 5);
                Evaluationinsect ei = new Evaluationinsect();
                ei.setInsect(i);
                ei.setEvaluation(eval);
                em.persist(ei);
            }
            index++;
            if (result[index] != null && result[index].equalsIgnoreCase("TRUE")) {   //minnow
                Insect i = em.find(Insect.class, 6);
                Evaluationinsect ei = new Evaluationinsect();
                ei.setInsect(i);
                ei.setEvaluation(eval);
                em.persist(ei);
            }
            index++;
            if (result[index] != null && result[index].equalsIgnoreCase("TRUE")) {   //othermayflies
                Insect i = em.find(Insect.class, 7);
                Evaluationinsect ei = new Evaluationinsect();
                ei.setInsect(i);
                ei.setEvaluation(eval);
                em.persist(ei);
            }
            index++;
            if (result[index] != null && result[index].equalsIgnoreCase("TRUE")) {   //damselflies
                Insect i = em.find(Insect.class, 8);
                Evaluationinsect ei = new Evaluationinsect();
                ei.setInsect(i);
                ei.setEvaluation(eval);
                em.persist(ei);
            }
            index++;
            if (result[index] != null && result[index].equalsIgnoreCase("TRUE")) {   //dragonflies
                Insect i = em.find(Insect.class, 9);
                Evaluationinsect ei = new Evaluationinsect();
                ei.setInsect(i);
                ei.setEvaluation(eval);
                em.persist(ei);
            }
            index++;
            if (result[index] != null && result[index].equalsIgnoreCase("TRUE")) {   //bugs
                Insect i = em.find(Insect.class, 10);
                Evaluationinsect ei = new Evaluationinsect();
                ei.setInsect(i);
                ei.setEvaluation(eval);
                em.persist(ei);
            }
            index++;
            if (result[index] != null && result[index].equalsIgnoreCase("TRUE")) {   //caddisflies
                Insect i = em.find(Insect.class, 11);
                Evaluationinsect ei = new Evaluationinsect();
                ei.setInsect(i);
                ei.setEvaluation(eval);
                em.persist(ei);
            }
            index++;
            if (result[index] != null && result[index].equalsIgnoreCase("TRUE")) {   //true flies
                Insect i = em.find(Insect.class, 12);
                Evaluationinsect ei = new Evaluationinsect();
                ei.setInsect(i);
                ei.setEvaluation(eval);
                em.persist(ei);
            }
            index++;
            if (result[index].equalsIgnoreCase("TRUE")) {   //snails
                Insect i = em.find(Insect.class, 13);
                Evaluationinsect ei = new Evaluationinsect();
                ei.setInsect(i);
                ei.setEvaluation(eval);
                em.persist(ei);
            }

        } catch (IndexOutOfBoundsException e) {
            try {

            } catch (Exception za) {
            }
            log.log(Level.SEVERE, "IndexOutOfBoundsException{0}");
        }

    }

    private String parseRiverAndSite(String line) {
        Pattern patt = Pattern.compile(";");
        String[] result = patt.split(line);
        String riverName = null;
        try {
            riverName = result[7].trim();
            if (riverName.contains("2015") || riverName.contains("2014")) {
                //suspectLines.add(line);
                log.log(Level.WARNING, "--- Bad river data: " + line);
                return null;
            }
            try {
                Integer.parseInt(riverName);
                //suspectLines.add(line);
                log.log(Level.WARNING, "--- integer river name: " + line);
                return null;
            } catch (NumberFormatException e) {
                //cool
            }
            Pattern patt2 = Pattern.compile(" ");
            String[] result2 = patt2.split(riverName);
            if (result2.length == 2) {
                if (result2[1].equalsIgnoreCase("River") || result2[1].equalsIgnoreCase("Rivier")) {
                    riverName = result2[0];
                }
            }
            if (result2.length == 3) {
                if (result2[2].equalsIgnoreCase("River") || result2[2].equalsIgnoreCase("Rivier")) {
                    riverName = result2[0] + " " + result2[1];
                }
            }
            //riverMap.put(riverName, riverName);
//            parseSite(line, riverName);

        } catch (ArrayIndexOutOfBoundsException e) {
            log.log(Level.WARNING, "Bad line: " + line);
            //suspectLines.add(line);
        }
        return riverName;
    }

    private Evaluationsite parseSite(String line, String riverName, Category rocky, Category sandy) {

        Pattern patt = Pattern.compile(";");
        String[] result = patt.split(line);
        Evaluationsite site = new Evaluationsite();

        site.setGID(Integer.parseInt(result[1]));
        site.setSiteName(result[2]);
        site.setDescription(result[3]);
        if (result[4].trim().equalsIgnoreCase("rocky")) {
            site.setCategory(rocky);
        }
        if (result[4].trim().equalsIgnoreCase("sandy")) {
            site.setCategory(sandy);
        }
        site.setRiverName(riverName);

        String point = result[0].substring(6);
        int i = point.indexOf(")");
        point = point.substring(0, i);
        Pattern patt2 = Pattern.compile(" ");
        String[] r2 = patt2.split(point);

        BigDecimal db1 = new BigDecimal(r2[0]).setScale(10, RoundingMode.HALF_EVEN);
        BigDecimal db2 = new BigDecimal(r2[1]).setScale(10, RoundingMode.HALF_EVEN);
        Double longitude = Double.parseDouble(db1.toString());
        Double latitude = Double.parseDouble(db2.toString());
        site.setLongitude(longitude);
        site.setLatitude(latitude);
//        evaluationsites.add(site);
        return site;
    }

    public void match(HashMap<String, String> riverMap, List<Evaluationsite> evaluationsites) {
        List<String> riverStringList = new ArrayList<>();

        /*riverMap.entrySet().stream().map((entry) -> entry.getValue()).forEach((string) -> {
         riverStringList.add(string);
         });*/
        Set<String> riverMapKeys = riverMap.keySet();
        for (String k : riverMapKeys) {
            riverStringList.add(k);
        }
        Collections.sort(riverStringList);
        HashMap<String, River> xMap = new HashMap<>();
        Query q = em.createNamedQuery("River.findByRiverName", River.class);

        for (String riverName : riverStringList) {
            q.setParameter("riverName", riverName);
            q.setMaxResults(1);
            try {
                River river = (River) q.getSingleResult();
                xMap.put(river.getRiverName(), river);
                //riverList.add(river);
            } catch (NoResultException e) {
                // noMatchList.add(string);
            }
        }
        /* riverStringList.stream().map((string) -> {
         q.setParameter("riverName", string);
         return string;
         }).forEach((string) -> {
         q.setMaxResults(1);
         try {
         River river = (River) q.getSingleResult();
         xMap.put(river.getRiverName(), river);
         //riverList.add(river);
         } catch (NoResultException e) {
         // noMatchList.add(string);
         }
         });*/
        setSiteRivers(evaluationsites, xMap);
    }

    static final Logger log = Logger.getLogger(DumbFilesImporter.class.getSimpleName());
}
