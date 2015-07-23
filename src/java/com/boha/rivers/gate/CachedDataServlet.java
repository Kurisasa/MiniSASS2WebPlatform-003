/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.gate;

import static com.boha.rivers.gate.CachedRequestWebSocket.log;
import static com.boha.rivers.gate.Test.log;
import static com.boha.rivers.servlet.RiverListingServlet.getElapsed;
import com.boha.rivers.transfer.RequestDTO;
import com.boha.rivers.transfer.RequestList;
import com.boha.rivers.transfer.ResponseDTO;
import com.boha.rivers.util.CloudMsgUtil;
import com.boha.rivers.util.DataUtil;
import com.boha.rivers.util.Elapsed;
import com.boha.rivers.util.GZipUtility;
import com.boha.rivers.util.ListUtil;
import com.boha.rivers.util.PlatformUtil;
import com.boha.rivers.util.TrafficCop;
import com.google.gson.Gson;
import com.oreilly.servlet.ServletUtils;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author CodeTribe1
 */
@WebServlet(name = "CachedDataServlet", urlPatterns = {"/rsrequest"})
public class CachedDataServlet extends HttpServlet {

    @EJB
    DataUtil dataUtil;
    @EJB
    ListUtil listUtil;
    @EJB
    PlatformUtil platformUtil;
    @EJB
    TrafficCop trafficCop;
    @EJB
    CloudMsgUtil cloudMsgUtil;
    static final String SOURCE = "CachedDataServlet";

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("application/json;charset=UTF-8");
        log.log(Level.OFF, "Servlet starting ...");
        PrintWriter out = response.getWriter();

        ResponseDTO ur = new ResponseDTO();
        ByteBuffer bb = null;
        int goodCount = 0, badCount = 0;
        long start = System.currentTimeMillis();
        try {

            RequestList dto = getRequest(request);
            for (RequestDTO req : dto.getRequests()) {
                ResponseDTO resp = trafficCop.processRequest(req, dataUtil, listUtil, cloudMsgUtil, platformUtil);
                if (resp.getStatusCode() == 0) {
                    goodCount++;
                } else {
                    badCount++;
                }
            }
            ur.setStatusCode(0);
            ur.setMessage("Cached requests processed");
            ur.setGoodCount(goodCount);
            ur.setBadCount(badCount);
            long end = System.currentTimeMillis();
            ur.setElapsedRequestTimeInSeconds(Elapsed.getElapsed(start, end));
            log.log(Level.INFO, "Total elapsed time: {0}", ur.getElapsedRequestTimeInSeconds());

        } catch (Exception ex) {
            ur.setMessage("Unable to process cached requests");
            ur.setStatusCode(777);
        } finally {
            response.setContentType("application/zip;charset=UTF-8");
            File zipped;
            String json = gson.toJson(ur);
            //System.out.println("%%%$$$ RESPONSE JSON:\n" + json);
            try {
                zipped = GZipUtility.getZipped(json);
                ServletUtils.returnFile(zipped.getAbsolutePath(), response.getOutputStream());
                response.getOutputStream().close();
                log.log(Level.OFF, "### Zipped Length of Response: {0} -  "
                        + "unzipped length: {1}", new Object[]{zipped.length(), json.length()});
            } catch (IOException e) {
                log.log(Level.SEVERE, "Zipping problem - probably the zipper cannot find the zipped file", e);
            }

            long end = System.currentTimeMillis();
            log.log(Level.INFO, "#########---> RiverListingServlet completed in {0} seconds", getElapsed(start, end));

        }
    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

    private RequestList getRequest(HttpServletRequest req) {
        String json = req.getParameter("JSON");
        RequestList re = null;
        if (json == null) {
            log.log(Level.OFF, "Json parameter not found...");
            re = new RequestList();
            re.setRequestType(0);
            return re;
        }

        try {
            re = gson.fromJson(json, RequestList.class);
            log.log(Level.INFO, "JSON okay. ...");
        } catch (Exception e) {
            log.log(Level.OFF, "JSON is not okay. ...");
            re = new RequestList();
            re.setRequestType(0);
        }
        return re;
    }
    Gson gson = new Gson();
    static final Logger log = Logger.getLogger(CachedDataServlet.class.getSimpleName());
}
