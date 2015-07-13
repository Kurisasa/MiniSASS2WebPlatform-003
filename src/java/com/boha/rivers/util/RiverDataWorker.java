/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.util;

/**
 *
 * @author aubreyM
 */
import com.boha.rivers.data.Evaluation;
import com.boha.rivers.data.Evaluationinsect;
import com.boha.rivers.data.Evaluationsite;
import com.boha.rivers.data.River;
import com.boha.rivers.data.Riverpart;
import com.boha.rivers.data.Riverpoint;
import com.boha.rivers.dto.EvaluationDTO;
import com.boha.rivers.dto.EvaluationInsectDTO;
import com.boha.rivers.dto.EvaluationSiteDTO;
import com.boha.rivers.dto.RiverDTO;
import com.boha.rivers.dto.RiverPartDTO;
import com.boha.rivers.dto.RiverPointDTO;
import com.google.gson.Gson;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.Stateless;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

@Stateless
@TransactionManagement(TransactionManagementType.CONTAINER)
public class RiverDataWorker {

    @PersistenceContext
    EntityManager em;

    private PreparedStatement preparedStatement;
    private static final String SQL_STATEMENT = "select a.riverPointID, a.riverPartID, a.latitude, a.longitude,  "
            + "( ? * acos( cos( radians(?) ) * cos( radians( a.latitude) ) "
            + "* cos( radians( a.longitude ) - radians(?) ) + sin( radians(?) ) "
            + "* sin( radians( a.latitude ) ) ) ) "
            + "AS distance FROM riverpoint a  HAVING distance < ? order by distance";

    private static final String SQL_STATEMENT2 = "select a.evaluationSiteID, a.latitude, a.longitude,  "
            + "( ? * acos( cos( radians(?) ) * cos( radians( a.latitude) ) "
            + "* cos( radians( a.longitude ) - radians(?) ) + sin( radians(?) ) "
            + "* sin( radians( a.latitude ) ) ) ) "
            + "AS distance FROM evaluationSite a  HAVING distance < ? order by distance";

    public static final int KILOMETRES = 1, MILES = 2, PARM_KM = 6371, PARM_MILES = 3959;

    private Connection conn;

    public List<RiverDTO> getRiversWithinRadius(Double latitude, Double longitude,
            int radius, int type, int flag)
            throws Exception {
        log.log(Level.INFO, "#### getRiversWithinRadius, lat: {0} lng: {1} rad: {2}",
                new Object[]{latitude, longitude, radius});
        long start = System.currentTimeMillis();
        if (conn == null || conn.isClosed()) {
            conn = em.unwrap(Connection.class);
            log.log(Level.INFO, "..........SQL Connection unwrapped from EntityManager");
        }
        if (preparedStatement == null || preparedStatement.isClosed()) {
            preparedStatement = conn.prepareStatement(SQL_STATEMENT);
            log.log(Level.INFO, "..........SQL Statement prepared from Connection: {0}", preparedStatement.toString());
        }
        switch (type) {
            case KILOMETRES:
                preparedStatement.setInt(1, PARM_KM);
                break;
            case MILES:
                preparedStatement.setInt(1, PARM_MILES);
                break;
            case 0:
                preparedStatement.setInt(1, PARM_KM);
                break;
        }
        preparedStatement.setDouble(2, latitude);
        preparedStatement.setDouble(3, longitude);
        preparedStatement.setDouble(4, latitude);
        preparedStatement.setInt(5, radius);
        ResultSet resultSet = preparedStatement.executeQuery();
        long end = System.currentTimeMillis();

        log.log(Level.INFO, "RiverDataWorkerBee -  rivers by radius elapsed: {0} fetchSize: {1}",
                new Object[]{Elapsed.getElapsed(start, end), resultSet.getFetchSize()});
        if (flag == 1) {
            return buildRiverListWithSites(resultSet);
        } else {
            return buildRiverList(resultSet);
        }

    }

    private List<RiverDTO> buildRiverListWithSites(ResultSet resultSet) throws SQLException {
        long start = System.currentTimeMillis();
        List<RiverDTO> rivers = new ArrayList<>();
        List<Integer> riverPartIDList = new ArrayList<>();
        HashMap<Integer, Integer> map = new HashMap<>();
        while (resultSet.next()) {
            Integer id = resultSet.getInt("riverPartID");
            Integer distance = resultSet.getInt("distance");
            riverPartIDList.add(id);
            Integer riverPointID = resultSet.getInt("riverPointID");
            if (!map.containsKey(riverPointID)) {
                map.put(riverPointID, distance);
            }
        }
        if (riverPartIDList.isEmpty()) {
            return rivers;
        }
        Query qw = em.createNamedQuery("Riverpart.findRiversByRiverPartList", River.class);
        qw.setParameter("list", riverPartIDList);
        List<River> riverList = qw.getResultList();
        System.out.print("### rivers found: " + riverList.size());
        for (River river : riverList) {
            RiverDTO riverDTO = new RiverDTO(river);
            riverDTO.setRiverpartList(new ArrayList<>());
            List<Riverpart> rpList = river.getRiverpartList();
            System.out.print("\t### " + river.getRiverName() + " riverParts found: " + rpList.size());
            riverDTO.setEvaluationsiteList(buildSites(river.getRiverID()));
            rivers.add(riverDTO);
        }

        long end = System.currentTimeMillis();
        System.out.print("\n\n################# buildRiverList elapsed: " + Elapsed.getElapsed(start, end));

        resultSet.close();
        return rivers;
    }

    private List<EvaluationDTO> buildEvaluation(int evaluationSiteID) {
        List<EvaluationDTO> list = new ArrayList<>();
        Query qw = em.createNamedQuery("Evaluation.findByevaluationSiteID", Evaluation.class);
        qw.setParameter("evaluationSiteID", evaluationSiteID);
        List<Evaluation> l = qw.getResultList();
        for (Evaluation e : l) {
            EvaluationDTO dTO = new EvaluationDTO(e);
            log.log(Level.OFF, new Gson().toJson(dTO));
            for (Evaluationinsect ea : e.getEvaluationinsectList()) {
                EvaluationInsectDTO eid = new EvaluationInsectDTO(ea);
                dTO.getEvaluationinsectList().add(eid);
            }
            list.add(dTO);
        }
        return list;
    }

    private List<EvaluationSiteDTO> buildSites(int riverID) {
        List<EvaluationSiteDTO> list = new ArrayList<>();
        Query qw = em.createNamedQuery("Evaluationsite.findByRiverID", Evaluationsite.class);
        qw.setParameter("riverID", riverID);
        List<Evaluationsite> l = qw.getResultList();
        for (Evaluationsite e : l) {
            EvaluationSiteDTO siteDTO = new EvaluationSiteDTO(e);
            siteDTO.setEvaluationList(buildEvaluation(e.getEvaluationSiteID()));
            list.add(siteDTO);
        }
        return list;
    }

    private List<RiverDTO> buildRiverList(ResultSet resultSet) throws SQLException {
        long start = System.currentTimeMillis();
        List<RiverDTO> rivers = new ArrayList<>();
        List<Integer> riverPartIDList = new ArrayList<>();
        HashMap<Integer, Integer> map = new HashMap<>();
        while (resultSet.next()) {
            Integer id = resultSet.getInt("riverPartID");
            Integer distance = resultSet.getInt("distance");
            riverPartIDList.add(id);
            Integer riverPointID = resultSet.getInt("riverPointID");
            if (!map.containsKey(riverPointID)) {
                map.put(riverPointID, distance);
            }
        }
        if (riverPartIDList.isEmpty()) {
            return rivers;
        }
        Query qw = em.createNamedQuery("Riverpart.findRiversByRiverPartList", River.class);
        qw.setParameter("list", riverPartIDList);
        List<River> riverList = qw.getResultList();
        System.out.print("### rivers found: " + riverList.size());
        for (River river : riverList) {
            RiverDTO riverDTO = new RiverDTO(river);
            riverDTO.setRiverpartList(new ArrayList<>());
            List<Riverpart> rpList = river.getRiverpartList();
            System.out.print("\t### " + river.getRiverName() + " riverParts found: " + rpList.size());
            for (Riverpart rp : rpList) {
                RiverPartDTO rpDTO = new RiverPartDTO(rp);
                rpDTO.setRiverpointList(new ArrayList<>());

                List<Riverpoint> pointList = rp.getRiverpointList();
                for (Riverpoint riverPoint : pointList) {
                    RiverPointDTO dto = new RiverPointDTO(riverPoint);
                    Integer dist = map.get(dto.getRiverPointID());
                    dto.setDistance(dist);
                    rpDTO.getRiverpointList().add(dto);
                }
                System.out.print("\t\t### " + river.getRiverName() + " riverPart: " + rp.getRiverPartID() + " points found: " + pointList.size());
                riverDTO.getRiverpartList().add(rpDTO);
            }
            riverDTO.setEvaluationsiteList(buildSites(river.getRiverID()));
            rivers.add(riverDTO);
        }

        long end = System.currentTimeMillis();
        System.out.print("\n\n################# buildRiverList elapsed: " + Elapsed.getElapsed(start, end));

        resultSet.close();
        return rivers;
    }

    public EntityManager getEntityManager() {
        return em;
    }

    public static final int ROWS_PER_PAGE = 100;
    static final Logger log = Logger.getLogger(RiverDataWorker.class.getName());
}
