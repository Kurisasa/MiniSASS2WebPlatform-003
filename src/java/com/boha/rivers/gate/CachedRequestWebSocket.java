/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.gate;

import com.boha.rivers.data.Teammember;
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.ejb.Stateful;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

/**
 *
 * @author aubreyM
 */
@ServerEndpoint("/wsrequest")
@Stateful
public class CachedRequestWebSocket {

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
    static final String SOURCE = "CachedRequestWebSocket";
    //TODO - clean up expired sessions!!!!
    public static final Set<Session> peers
            = Collections.synchronizedSet(new HashSet<Session>());

    @OnMessage
    public ByteBuffer onMessage(String message) {
        log.log(Level.WARNING, SOURCE + " - onMessage: {0}", message);
        ResponseDTO response = new ResponseDTO();
        ByteBuffer bb = null;
        int goodCount = 0, badCount = 0;
        long start = System.currentTimeMillis();
        try {

            RequestList dto = gson.fromJson(message, RequestList.class);
            for (RequestDTO req : dto.getRequests()) {
                ResponseDTO resp = trafficCop.processRequest(req, dataUtil, listUtil, cloudMsgUtil, platformUtil);
                if (resp.getStatusCode() == 0) {
                    goodCount++;
                } else {
                    badCount++;
                }
            }
            response.setStatusCode(0);
            response.setMessage("Cached requests processed");
            response.setGoodCount(goodCount);
            response.setBadCount(badCount);
            long end = System.currentTimeMillis();
            response.setElapsedRequestTimeInSeconds(Elapsed.getElapsed(start, end));
            log.log(Level.INFO, "Total elapsed time: {0}", response.getElapsedRequestTimeInSeconds());
            bb = GZipUtility.getZippedResponse(response);
        } catch (IOException ex) {
            response.setMessage("Unable to process cached requests");
            response.setStatusCode(777);
            try {
                bb = GZipUtility.getZippedResponse(response);
            } catch (IOException ex1) {
                log.log(Level.SEVERE, null, ex1);
            }
        }
        return bb;
    }

    @OnOpen
    public void onOpen(Session session) {

        peers.add(session);
        try {
            ResponseDTO r = new ResponseDTO();
            r.setSessionID(session.getId());
            session.getBasicRemote().sendText(gson.toJson(r));
            log.log(Level.WARNING, SOURCE + " - onOpen...sent session id: {0}", session.getId());
        } catch (IOException ex) {
            log.log(Level.SEVERE, "Failed to send websocket sessionID", ex);
        }
    }

    @OnClose
    public void onClose(Session session
    ) {
        log.log(Level.WARNING, SOURCE + " onClose - remove session: {0}", session.getId());
        for (Session mSession : peers) {
            if (session.getId().equalsIgnoreCase(mSession.getId())) {
                peers.remove(mSession);
                break;
            }
        }
    }

    @OnError
    public void onError(Throwable t) {
        log.log(Level.SEVERE, SOURCE, t);

    }

    Gson gson = new Gson();
    static final Logger log = Logger.getLogger(CachedRequestWebSocket.class.getSimpleName());
}
