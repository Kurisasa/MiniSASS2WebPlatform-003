/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.util;

import com.boha.rivers.data.River;
import com.boha.rivers.data.Riverpart;
import com.boha.rivers.data.Riverpoint;
import com.boha.rivers.dto.TeamMemberDTO;
import static com.boha.rivers.util.RiverDataWorker.log;
import com.mysql.fabric.xmlrpc.base.Data;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.logging.Level;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import nl.knaw.dans.common.dbflib.CorruptedTableException;
import nl.knaw.dans.common.dbflib.IfNonExistent;
import nl.knaw.dans.common.dbflib.Record;
import nl.knaw.dans.common.dbflib.Table;
import org.nocrala.tools.gis.data.esri.shapefile.ShapeFileReader;
import org.nocrala.tools.gis.data.esri.shapefile.exception.InvalidShapeFileException;
import org.nocrala.tools.gis.data.esri.shapefile.header.ShapeFileHeader;
import org.nocrala.tools.gis.data.esri.shapefile.shape.AbstractShape;
import org.nocrala.tools.gis.data.esri.shapefile.shape.PointData;
import org.nocrala.tools.gis.data.esri.shapefile.shape.ShapeHeader;
import org.nocrala.tools.gis.data.esri.shapefile.shape.shapes.PolylineShape;

/**
 *
 * @author aubreyM
 */
@Stateless
public class Shape2SQLParser {

    @PersistenceContext
    EntityManager em;

    private PreparedStatement preparedStatement1, preparedStatement2, preparedStatement3, preparedStatement4;
    private static final String SQL_STATEMENT_INSERT_POINT = "insert into riverpoint (riverPartID, iprivID, latitude, longitude) VALUES (?,?,?,?)  ";
    private static final String SQL_STATEMENT_INSERT_RIVER = "insert into river (riverName) VALUES (?)";
    private static final String SQL_STATEMENT_INSERT_PART = "insert into riverpart (riverID, iprivID, riverName, fNode, tNode, distanceToMouth) VALUES (?,?,?, ?, ?, ?)  ";
    private static final String SQL_STATEMENT_MEMBER = "insert into teammember (teamID, firstName, lastName, email, cellphone, dateRegistered, pin, activeFlag, teamMemberImage) VALUES (?,?,?, ?, ?, ?,?,?,?)  ";

    private Connection conn;

    static final int BATCH_SIZE = 20000;

    public void processDataBase() throws IOException, InvalidShapeFileException, SQLException {
        readDBF();
    }

    public void processCSV() throws IOException, SQLException{
        readCSV();
    }
    public int parseShapefileInBatch() throws SQLException, IOException, InvalidShapeFileException {
        long start = System.currentTimeMillis();
        em = EMUtil.getEntityManager();
        if (conn == null || conn.isClosed()) {
            conn = em.unwrap(Connection.class);
            log.log(Level.INFO, "..........SQL Connection unwrapped from EntityManager");
        }
        if (preparedStatement3 == null || preparedStatement3.isClosed()) {
            preparedStatement3 = conn.prepareStatement(SQL_STATEMENT_INSERT_POINT);
            log.log(Level.INFO, "..........SQL Statement prepared from Connection: {0}", preparedStatement3.toString());
        }

        List<Riverpoint> points = getShapeFilePoints();
        System.err.println("###### total river points: " + points.size());

        int index = 0;
        for (Riverpoint rp : points) {
            preparedStatement3.setInt(1, rp.getRiverPart().getRiverPartID());
            preparedStatement3.setInt(2, rp.getIprivID());
            preparedStatement3.setDouble(3, rp.getLatitude());
            preparedStatement3.setDouble(4, rp.getLongitude());
            preparedStatement3.addBatch();
            if ((index + 1) % BATCH_SIZE == 0) {
                int[] results = preparedStatement3.executeBatch();

                int goodResults = 0, badResult = 0;
                for (int i = 0; i < results.length; i++) {
                    int j = results[i];
                    if (j > 0) {
                        goodResults++;
                    } else {
                        badResult++;
                    }

                }
                System.out.println("++++++ " + BATCH_SIZE + " points saved in batch: " + index
                        + " goodResults: " + goodResults + " badResult: " + badResult + " - " + new Date().toString());
            }
            index++;
        }
        int[] results = preparedStatement3.executeBatch();
        System.err.println("########### last results from batch: " + results.length);
        long end = System.currentTimeMillis();
        System.err.println("%%%%%%%%%%%%%%%% ELAPSED TIME: " + Elapsed.getElapsed(start, end));
        return 0;

    }

    private Riverpart getRiverPart(int recordNumber) {
        Query q = em.createNamedQuery("Riverpart.findByIprivID", Riverpart.class);
        try {
            q.setParameter("iprivID", recordNumber);
            q.setMaxResults(1);
            Riverpart riverPart = (Riverpart) q.getSingleResult();
            riverPart.setRiverName(riverPart.getRiverName().trim());
            return riverPart;
        } catch (NoResultException e) {

        }
        return null;
    }

    /**
     * Get rivers from shapefile and write them to database
     *
     * @throws IOException
     */
    private void readDBF() throws IOException, SQLException {
        //File file = new File("/workspaces/rivers/wriall500.dbf");
        File file = new File("C:/Android/workspaces/rivers/wriall500.dbf");

        if (!file.exists()) {
            file = new File("/opt/river_data/wriall500.dbf");
        }
        if (!file.exists()) {
            throw new IOException("Shape File DBF Not found");
        }
        System.out.println("*** Rivers DBF file is: " + file.getAbsolutePath());
        if (!file.exists()) {
            throw new IOException();
        }
        Table table = new Table(file);

        try {
            table.open(IfNonExistent.ERROR);
            Iterator<Record> iterator = table.recordIterator();
            while (iterator.hasNext()) {
                Record record = iterator.next();
                addRiver(record);
            }

        } catch (IOException | CorruptedTableException e) {
            System.out.println("*** Rivers DBF file is: " + e.getMessage());
            throw new IOException();
        } finally {
            table.close();
        }

        System.out.println("============================> DBF read & process completed! OK!\n\n\n");
    }

    private void addTeamMember(Integer teamID, String name, String surname, String email, Date dateJoin) throws SQLException {
     
        String pin = getRandomPin();
        em = EMUtil.getEntityManager();
        if (conn == null || conn.isClosed()) {
            conn = em.unwrap(Connection.class);
            log.log(Level.INFO, "..........SQL Connection unwrapped from EntityManager");
        }
        if (preparedStatement4 == null || preparedStatement4.isClosed()) {
            preparedStatement4 = conn.prepareStatement(SQL_STATEMENT_MEMBER, Statement.RETURN_GENERATED_KEYS);
            log.log(Level.INFO, "..........SQL Statement prepared from Connection: {0}", preparedStatement4.toString());
        }
        preparedStatement4.setInt(1, teamID);
        preparedStatement4.setString(2, name);
        preparedStatement4.setString(3, surname);
        preparedStatement4.setString(4, email);
        preparedStatement4.setString(5, "");
        preparedStatement4.setDate(6, new java.sql.Date(dateJoin.getTime()));
        preparedStatement4.setString(7, pin);
        preparedStatement4.setInt(8, 1);
        preparedStatement4.setString(9, "");

        int result = preparedStatement4.executeUpdate();
        if (result > 0) {
            System.err.println("++++ RiverPart added to river: " + name + ", riverPart iprivID: " + surname);
        }
    }

    private String getRandomPin() {
        StringBuilder sb = new StringBuilder();
        Random rand = new Random(System.currentTimeMillis());
        int x = rand.nextInt(9);
        if (x == 0) {
            x = 3;
        }
        sb.append(x);
        sb.append(rand.nextInt(9));
        sb.append(rand.nextInt(9));
        sb.append(rand.nextInt(9));
        sb.append(rand.nextInt(9));
        sb.append(rand.nextInt(9));
        return sb.toString();
    }

    private void addRiver(Record record) throws SQLException {
        String name = record.getStringValue("NAME");
        if (name == null || name.trim().isEmpty()) {
            System.out.println("----- river has no name, ignored ...");
            return;
        }
        em = EMUtil.getEntityManager();
        if (conn == null || conn.isClosed()) {
            conn = em.unwrap(Connection.class);
            log.log(Level.INFO, "..........SQL Connection unwrapped from EntityManager");
        }
        if (preparedStatement1 == null || preparedStatement1.isClosed()) {
            preparedStatement1 = conn.prepareStatement(SQL_STATEMENT_INSERT_RIVER, Statement.RETURN_GENERATED_KEYS);
            log.log(Level.INFO, "..........SQL Statement prepared from Connection: {0}", preparedStatement1.toString());
        }
        try {
            Query q = em.createNamedQuery("River.findByRiverName", River.class);
            q.setParameter("riverName", name);
            q.setMaxResults(1);
            River r = (River) q.getSingleResult();
            addRiverPart(r, record);

        } catch (NoResultException e) {
            preparedStatement1.setString(1, name);
            preparedStatement1.executeUpdate();
            ResultSet rs = preparedStatement1.getGeneratedKeys();
            Integer id = null;
            if (rs.next()) {
                id = rs.getInt(1);
            }
            if (id != null) {
                Query q = em.createNamedQuery("River.findByRiverName", River.class);
                q.setParameter("riverName", name);
                q.setMaxResults(1);
                River r = (River) q.getSingleResult();
                addRiverPart(r, record);

                System.out.println("######################### River added to database: " + name + "\n\n");
            } else {
                System.err.println("##### ERROR - could not get river ID generated");
            }

        }

    }

    private void addRiverPart(River river, Record record) throws SQLException {
        Number fNode = record.getNumberValue("FNODE_");
        Number tNode = record.getNumberValue("TNODE_");
        Number tImprivID = record.getNumberValue("TMPRIV_ID");
        Number dist = record.getNumberValue("DIST2MTH");

        em = EMUtil.getEntityManager();
        if (conn == null || conn.isClosed()) {
            conn = em.unwrap(Connection.class);
            log.log(Level.INFO, "..........SQL Connection unwrapped from EntityManager");
        }
        if (preparedStatement2 == null || preparedStatement1.isClosed()) {
            preparedStatement2 = conn.prepareStatement(SQL_STATEMENT_INSERT_PART);
            log.log(Level.INFO, "..........SQL Statement prepared from Connection: {0}", preparedStatement2.toString());
        }
        preparedStatement2.setInt(1, river.getRiverID());
        preparedStatement2.setInt(2, tImprivID.intValue());
        preparedStatement2.setString(3, river.getRiverName());
        preparedStatement2.setInt(4, fNode.intValue());
        preparedStatement2.setInt(5, tNode.intValue());
        preparedStatement2.setDouble(6, dist.doubleValue());

        int result = preparedStatement2.executeUpdate();
        if (result > 0) {
            System.err.println("++++ RiverPart added to river: " + river.getRiverName() + ", riverPart iprivID: " + tImprivID.intValue());
        }
    }

    private List<Riverpoint> getShapeFilePoints() throws IOException,
            InvalidShapeFileException {

        long start = System.currentTimeMillis();
        List<Riverpoint> riverPoints = new ArrayList<>();
        File file = new File("C:/Android/workspaces/rivers/wriall500.shp");
        if (!file.exists()) {
            file = new File("/opt/river_data/wriall500.shp");
        }
        if (!file.exists()) {
            throw new IOException("Shape File Not found");
        }
        System.out.println("ShapeFile is: " + file.getAbsolutePath());

        FileInputStream is = new FileInputStream(file);
        ShapeFileReader r = new ShapeFileReader(is);

        ShapeFileHeader h = r.getHeader();
        System.out.println("&&&& ********* The shape type of this files is " + h.getShapeType());

        int count = 0;
        AbstractShape s;
        Riverpart riverPart = null;
        try {
            while ((s = r.next()) != null) {
                switch (s.getShapeType()) {
                    case POLYLINE:
                        ShapeHeader shdr = s.getHeader();

                        riverPart = getRiverPart(shdr.getRecordNumber());
                        if (riverPart == null) {
                            continue;
                        }

                        PolylineShape pls = (PolylineShape) s;
                        PointData[] pts = pls.getPoints();

                        for (int i = 0; i < pts.length; i++) {
                            PointData pointData = pts[i];
                            double lat = pointData.getY();
                            double lng = pointData.getX();

                            Riverpoint rp = new Riverpoint();
                            rp.setIprivID(shdr.getRecordNumber());
                            rp.setLatitude(lat);
                            rp.setLongitude(lng);
                            rp.setRiverPart(riverPart);
                            riverPoints.add(rp);
                            count++;
                        }
                        break;

                    default:
                        System.out.println("===========> WTF???");
                }
            }

        } finally {
            is.close();
        }
        long end = System.currentTimeMillis();
        System.err.println("%%%%%%%%%%%%%%%% getShapeFilePoints ELAPSED TIME: " + Elapsed.getElapsed(start, end) + " count: " + count);
        System.out.println("################### ------> YEBO!!! - shapeFile parsing completed, river points added to list: " + riverPoints.size());
        return riverPoints;
    }

    private void readCSV() throws IOException, SQLException {
        File file = new File("C:/Android/workspaces/Android/workspaces/rivers/users.csv");
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ";";
        if (!file.exists()) {
            file = new File("/opt/river_data/users.csv");
        }
        if (!file.exists()) {
            throw new IOException("user csv Not found");
        }
        System.out.println("*** User csv file is: " + file.getAbsolutePath());
        System.out.println("User csv is: " + file.getAbsolutePath());
        try {

            br = new BufferedReader(new FileReader(file.getAbsoluteFile()));
            while ((line = br.readLine()) != null) {
                String[] user = line.split(cvsSplitBy);

                System.out.println("Country [code= " + user[1]
                        + " , NAME=" + user[5].substring(0, 19) + "]");
  
                
                addTeamMember(30, user[2], user[3], user[4], new Date());
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
