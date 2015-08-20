/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.util;

import com.boha.rivers.data.Gcmdevice;
import com.boha.rivers.data.Team;
import com.boha.rivers.data.Teammember;
import com.boha.rivers.data.Tmember;
import com.boha.rivers.dto.TeamDTO;
import com.boha.rivers.dto.TmemberDTO;
import com.boha.rivers.transfer.ResponseDTO;
import com.google.android.gcm.server.Constants;
import com.google.android.gcm.server.Message;
import com.google.android.gcm.server.MulticastResult;
import com.google.android.gcm.server.Result;
import com.google.android.gcm.server.Sender;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.Stateless;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

/**
 *
 * @author sifiso
 */
@Stateless
@TransactionManagement(TransactionManagementType.CONTAINER)
public class CloudMsgUtil {

    @PersistenceContext
    EntityManager em;
    private static final int RETRIES = 5;
    public static final String API_KEY = "AIzaSyBQap7FTpd14vmg8lua78NtlbvsEJ9eJiI";

    public ResponseDTO sendInviteToTeam(int teamMemberID, int teamID, PlatformUtil platformUtil, DataUtil dataUtil) throws
            Exception, DataException {
        ResponseDTO resp = new ResponseDTO();
        //send message to Google servers
        // add when relationship is established
        Tmember h = new Tmember();
        h.setAcceptInvite(0);
        h.setDateCreated(new Date());
        h.setTeam(em.find(Team.class, teamID));
        h.setTeamMember(em.find(Teammember.class, teamMemberID));

        try {
            em.persist(h);
            em.flush();
            LOG.log(Level.INFO, "Invite added to db");
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Invite add failed", e);
            throw new DataException("Invite add failed\n" + dataUtil.getErrorString(e));
        }

        resp.settMember(new TmemberDTO(h));

        Sender sender = new Sender(API_KEY);
        Gson g = new Gson();
        String txtGson ="You've been invited to join team "+ h.getTeam().getTeamName()+" from miniSASS river health care";
        Message message = new Message.Builder()
                .addData("message", txtGson)
                .addData("dateStamp", "" + new Date().getTime()).build();

        Query q = em.createNamedQuery("Gcmdevice.findTeamMember", Gcmdevice.class);
        q.setParameter("teamMemberID", teamMemberID);
        List<Gcmdevice> gList = q.getResultList();
        List<String> registrationIDs = new ArrayList<>();
        for (Gcmdevice m : gList) {
            registrationIDs.add(m.getRegistrationID());
        }
        if (registrationIDs.isEmpty()) {
            LOG.log(Level.SEVERE, "#### No instructor registrationIDs found ");
            resp.setMessage("Send an email to invite the into using the app");
            resp.setStatusCode(RETRIES);
            //platformUtil.addErrorStore(889, "#### No intructor devices found ", "Cloud Message Services");
            return resp;
        }

        boolean OK;
        String rMsg;

        if (registrationIDs.size() == 1) {
            Result result = sender.send(message, registrationIDs.get(0), RETRIES);
            OK = handleResult(result, platformUtil);
        } else {
            MulticastResult multiCastResult = sender.send(
                    message, registrationIDs, RETRIES);
            OK = handleMultiCastResult(multiCastResult, platformUtil);
        }
        if (OK) {
            rMsg = "Google GCM - message has been sent to Google servers";

        } else {
            rMsg = "Google GCM - Send an email to invite the into using the app";

            resp.setStatusCode(ResponseDTO.ERROR_SERVER);
            resp.setMessage(rMsg);
            //platformUtil.addErrorStore(889, "Google GCM - message has not been sent. Error occured", "Cloud Message Services");
        }

        resp.setMessage(rMsg);
        return resp;
    }

    public static final int GCM_MESSAGE_ERROR = 3, ALL_OK = 0;

    public int sendMessage(List<String> registrationIDs, PlatformUtil platformUtil) throws IOException, Exception {
        Sender sender = new Sender(API_KEY);
        Message message = new Message.Builder()
                .addData("message", "Observation created")
                .addData("dateStamp", "" + new Date().getTime()).build();

        boolean OK;
        if (registrationIDs.size() == 1) {
            Result result = sender.send(message, registrationIDs.get(0), RETRIES);
            OK = handleResult(result, platformUtil);
        } else {
            MulticastResult multiCastResult = sender.send(
                    message, registrationIDs, RETRIES);
            OK = handleMultiCastResult(multiCastResult, platformUtil);
        }
        if (!OK) {
            //platformUtil.addErrorStore(889, "Google GCM - message has not been sent. Error occured", "Cloud Message Services");
            return GCM_MESSAGE_ERROR;
        }
        return ALL_OK;
    }

    private boolean handleResult(Result result, PlatformUtil platformUtil)
            throws Exception {

        LOG.log(Level.INFO, "Handle result from Google GCM servers: {0}", result.toString());
        if (result.getErrorCodeName() != null) {
            if (result.getErrorCodeName().equals(Constants.ERROR_NOT_REGISTERED)) {
                // TODO remove the registration from the database
                LOG.log(Level.SEVERE, "#### GCM device not registered");
                //platformUtil.addErrorStore(889, "#### GCM device not registered", "Cloud Message Services");
                return false;
            }
            if (result.getErrorCodeName().equals(Constants.ERROR_UNAVAILABLE)) {
                LOG.log(Level.SEVERE, "#### GCM servers not available");
                //platformUtil.addErrorStore(889, "#### GCM servers not available", "Cloud Message Services");
                return false;
            }
            LOG.log(Level.SEVERE, "#### GCM message send error : {0}",
                    result.getErrorCodeName());
            //platformUtil.addErrorStore(889, "#### GCM message send error\nErrorCodeName: " + result.getErrorCodeName(), "Cloud Message Services");
            return false;
        }

        if (result.getMessageId() != null) {
            LOG.log(Level.INFO, "Result messageID from GCM: {0}", result.getMessageId());
            if (result.getCanonicalRegistrationId() != null) {
                LOG.log(Level.INFO,
                        "### Google GCM - canonical registration id found, updating db ...");
                //TODO update device registration id
                //EntityManager em = EMUtil.getEntityManager();
            }
        }
        return true;
    }

    private boolean handleMultiCastResult(MulticastResult multiCastResult, PlatformUtil platformUtil)
            throws Exception {
        LOG.log(Level.INFO, "Handle result from Google GCM servers: {0}", multiCastResult.toString());
        if (multiCastResult.getFailure() == 0
                && multiCastResult.getCanonicalIds() == 0) {
            LOG.log(Level.INFO, "### Google Cloud message send is OK, messages: {0}", multiCastResult.getTotal());
            return true;
        }
        LOG.log(Level.INFO,
                "### Google GCM - iterating through multicast Result for errors...");
        for (Result result : multiCastResult.getResults()) {
            if (result.getErrorCodeName() != null) {
                if (result.getErrorCodeName().equals(Constants.ERROR_NOT_REGISTERED)) {
                    // TODO remove the registration from the database
                    LOG.log(Level.SEVERE, "#### GCM device not registered");
                    //platformUtil.addErrorStore(889, "#### GCM device not registered", "Cloud Message Services");
                    return false;
                }
                if (result.getErrorCodeName().equals(Constants.ERROR_UNAVAILABLE)) {
                    // TODO resubmit this message after back-off
                    LOG.log(Level.SEVERE, "#### GCM servers not available");
                    //platformUtil.addErrorStore(889, "#### GCM servers not available", "Cloud Message Services");
                    return false;
                }
                LOG.log(Level.SEVERE, "#### GCM message send error : {0}",
                        result.getErrorCodeName());
                //platformUtil.addErrorStore(889, "#### GCM message send error\nErrorCodeName: " + result.getErrorCodeName(), "Cloud Message Services");
                return false;
            }

            if (result.getMessageId() != null) {
                LOG.log(Level.INFO, "Result messageID from GCM: {0}", result.getMessageId());
                if (result.getCanonicalRegistrationId() != null) {
                    LOG.log(Level.INFO,
                            "### Google GCM - canonical registration id found, updating db ...");
                    //update device registration id - query by gcmdevice by reg id ???????????

                }
            }
        }
        return true;
    }
    static final Logger LOG = Logger.getLogger("CloudMsgUtil");
}
