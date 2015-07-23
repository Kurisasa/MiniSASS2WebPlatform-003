/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.gate;

import com.boha.rivers.data.Teammember;
import com.boha.rivers.transfer.RequestDTO;
import com.boha.rivers.transfer.ResponseDTO;
import com.boha.rivers.util.CloudMsgUtil;
import com.boha.rivers.util.DataUtil;
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
import javax.ejb.Stateless;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

/**
 *
 * @author CodeTribe1
 */
@ServerEndpoint("/wsmini")
@Stateful
public class MinisassWebsocket {

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

    static final String SOURCE = "TeamWebSocket";

    public static final Set<Session> peers
            = Collections.synchronizedSet(new HashSet<Session>());

    @OnMessage
    public ByteBuffer onMessage(String message) {
        log.log(Level.WARNING, "***** onMessage: {0}", message);
        ResponseDTO resp = new ResponseDTO();
        ByteBuffer gg = null;

        try {
            RequestDTO dto = gson.fromJson(message, RequestDTO.class);
            resp = trafficCop.processRequest(dto,
                    dataUtil, listUtil, cloudMsgUtil, platformUtil);
            System.err.println(gson.toJson(resp));
            gg = GZipUtility.getZippedResponse(resp);

        } catch (Exception ex) {
            log.log(Level.SEVERE, null, ex);
            resp.setStatusCode(111);
            resp.setMessage("Problem processing request on server");
            //dataUtil.addErrorStore(19, dataUtil.getErrorString(ex), SOURCE);
            try {
                gg = GZipUtility.getZippedResponse(resp);
            } catch (Exception ex1) {
                log.log(Level.SEVERE, "Failed to zip error response!!!!!!!!", ex1);
            }
        }
        return gg;
    }

    @OnOpen
    public void onOpen(Session session) {
        peers.add(session);
        try {
            ResponseDTO r = new ResponseDTO();
            r.setSessionID(session.getId());
            r.setStatusCode(0);
            session.getBasicRemote().sendText(gson.toJson(r));
            log.log(Level.WARNING, "******** onOpen...sent....session id: {0}", session.getId());
        } catch (IOException ex) {
            log.log(Level.SEVERE, "Failed to send websocket sessionID keep digging", ex);
        }
    }

    @OnClose
    public void onClose(Session session
    ) {
        log.log(Level.WARNING, "onClose - remove session: {0}", session.getId());
        for (Session mSession : peers) {
            if (session.getId().equalsIgnoreCase(mSession.getId())) {
                peers.remove(mSession);
                break;
            }
        }
    }

    @OnError
    public void onError(Throwable t) {
        log.log(Level.SEVERE, "#############################@OnError, websocket failed", t);

        //session.getBasicRemote().sendText(gson.toJson(r));
    }

    Gson gson = new Gson();
    static final Logger log = Logger.getLogger(MinisassWebsocket.class.getSimpleName());
}
