/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.servlet;

import com.boha.rivers.util.Elapsed;
import com.boha.rivers.util.Shape2SQLParser;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.nocrala.tools.gis.data.esri.shapefile.exception.InvalidShapeFileException;
//import org.nocrala.tools.gis.data.esri.shapefile.exception.InvalidShapeFileException;

/**
 *
 * @author aubreyM
 */
@WebServlet(name = "RiverDataServlet", urlPatterns = {"/parse"})
public class RiverDataServlet extends HttpServlet {

    @EJB
    Shape2SQLParser shape2SQLParser;

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
        response.setContentType("text/html;charset=UTF-8");
        PrintWriter out = response.getWriter();
        out.println("<!DOCTYPE html>");
        out.println("<html>");
        out.println("<head>");
        out.println("<title>RiverDataServlet</title>");
        out.println("</head>");
        out.println("<body>");
        long start = System.currentTimeMillis();
        try {
            String parm = request.getParameter("PARM");
            if (parm.contains("DBF")) {
                shape2SQLParser.processDataBase();
            }
            if (parm.contains("CSV")) {
                shape2SQLParser.processCSV();
            } else {
                shape2SQLParser.parseShapefileInBatch();
            }
            long end = System.currentTimeMillis();
            double elapsed = Elapsed.getElapsed(start, end);
            out.println("<h1>RiverDataServlet completed shapeFile parsing </h1>");
            out.println("<h2>Elapsed time in seconds: " + elapsed + "</h2>");
            out.println("</body>");
            out.println("</html>");

        } catch (InvalidShapeFileException ex) {
            Logger.getLogger(RiverDataServlet.class.getName()).log(Level.SEVERE, null, ex);

            out.println("<h1>RiverDataServlet fucked up!! -- " + ex.getMessage() + " </h1>");
            out.println("</body>");
            out.println("</html>");
        } catch (SQLException ex) {
            Logger.getLogger(RiverDataServlet.class.getName()).log(Level.SEVERE, null, ex);
            out.println("<h1>RiverDataServlet fucked up!! -- " + ex.getMessage() + " </h1>");
            out.println("</body>");
            out.println("</html>");
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

}
