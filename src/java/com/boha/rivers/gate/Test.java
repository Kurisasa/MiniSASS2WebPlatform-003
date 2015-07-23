/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.gate;

import com.boha.rivers.transfer.RequestDTO;
import com.boha.rivers.transfer.ResponseDTO;
import com.boha.rivers.util.CloudMsgUtil;
import com.boha.rivers.util.DataUtil;
import com.boha.rivers.util.ListUtil;
import com.boha.rivers.util.PlatformUtil;
import com.boha.rivers.util.TrafficCop;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.PrintWriter;
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
@WebServlet(name = "Test", urlPatterns = {"/test1"})
public class Test extends HttpServlet {

    @EJB
    DataUtil dataUtil;
    @EJB
    ListUtil listUtil;
    @EJB
    TrafficCop trafficCop;
    @EJB
    CloudMsgUtil cloudMsgUtil;
    @EJB
    PlatformUtil platformUtil;

    Gson gson = new Gson();

    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("application/json;charset=UTF-8");
        log.log(Level.OFF, "Servlet starting ...");
        PrintWriter out = response.getWriter();
        String json;
        ResponseDTO ur = new ResponseDTO();
        try {
            RequestDTO req = getRequest(request);
            ur = trafficCop.processRequest(req,
                    dataUtil, listUtil, cloudMsgUtil, platformUtil);

        } catch (Exception ex) {
            log.log(Level.OFF, "Failed.....{0}", ex);
            ur.setStatusCode(10);
            ur.setMessage("broken sasa u r nt a geek");
        } finally {
            json = gson.toJson(ur);
            out.println(json);
            out.close();
            log.log(Level.OFF, "### Zipped Length of Response: {0} -  "
                    + "unzipped length: {1}", new Object[]{json.length()});
            log.log(Level.OFF, "Servlet ending");
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

    private RequestDTO getRequest(HttpServletRequest req) {
        String json = req.getParameter("JSON");
        RequestDTO re = null;
        if (json == null) {
            log.log(Level.OFF, "Json parameter not found...");
            re = new RequestDTO();
            re.setRequestType(0);
            return re;
        }

        try {
            re = gson.fromJson(json, RequestDTO.class);
            log.log(Level.INFO, "JSON okay. ...");
        } catch (Exception e) {
            log.log(Level.OFF, "JSON is not okay. ...");
            re = new RequestDTO();
            re.setRequestType(0);
        }
        return re;
    }

    static final Logger log = Logger.getLogger(Test.class.getSimpleName());
}
