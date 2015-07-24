/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.util;

import com.boha.rivers.data.Gcmdevice;
import com.boha.rivers.transfer.RequestDTO;
import com.boha.rivers.transfer.ResponseDTO;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

/**
 *
 * @author CodeTribe1
 */
@Stateless
public class TrafficCop {

    @EJB
    DataUtil dataUtil;

    @EJB
    ListUtil listUtil;

    @EJB
    RiverDataWorker dataWorker;

    @EJB
    CloudMsgUtil cloudMsgUtil;

    @EJB
    PlatformUtil platformUtil;

    public ResponseDTO processRequest(RequestDTO req,
            DataUtil dataUtil, ListUtil listUtil, CloudMsgUtil cloudMsgUtil, PlatformUtil platformUtil) {
        long start = System.currentTimeMillis();
        ResponseDTO ur = new ResponseDTO();
        try {
            switch (req.getRequestType()) {
                case RequestDTO.REGISTER_TEAM:
                    ur = dataUtil.registerTeam(req.getTeam());
                    break;
                case RequestDTO.REGISTER_TEAM_MEMBER:
                    ur = dataUtil.registerTeamMember(req.getTeamMember(), listUtil);
                    break;
                case RequestDTO.SIGN_IN_MEMBER:
                    ur = dataUtil.loginTeamMember(req.getEmail(), req.getPassword(), req.getGcmDevice(), listUtil);
                    break;
                case RequestDTO.ADD_COMMENT:
                    ur = dataUtil.addComment(req.getComment());
                    break;
                case RequestDTO.ADD_EVALUATION:
                    ur = dataUtil.addEvaluation(req.getEvaluation(), req.getInsectImages(), cloudMsgUtil, platformUtil);
                    break;
                case RequestDTO.INVITE_MEMBER:
                    dataUtil.inviteMembers(req.getTmember());
                    ur.setMessage("Member Invited");
                    break;
                case RequestDTO.ADD_INSECT_IMAGE:
                    ur = dataUtil.addInsertImage(req.getInsectImage());
                    break;
                case RequestDTO.ADD_STREAM:
                    ur = dataUtil.addStream(req.getStream());
                    break;
                case RequestDTO.LIST_EVALUATION_SITES:
                    ur = listUtil.getEvaluationList();
                    break;
                case RequestDTO.GET_MEMBER:
                    ur = listUtil.getTeamMemberProfileData(req.getTeamMemberID(), listUtil, dataUtil);
                    break;
                case RequestDTO.ADD_EVALUATION_INSECT:
                    ur = dataUtil.addEvaluationInsect(req.getEvaluationInsect());
                    break;
                case RequestDTO.UPDATE_TEAM:
                    ur = dataUtil.updateTeam(req.getTeam());
                    break;
                case RequestDTO.UPDATE_PROFILE:
                    ur = dataUtil.updateTeamMember(req.getTeamMember());
                    break;
                case RequestDTO.UPDATE_COMMENT:
                    ur = dataUtil.updateComment(req.getComment());
                    break;
                case RequestDTO.UPDATE_EVALUATION:
                    ur = dataUtil.updateEvaluation(req.getEvaluation());
                    break;
                case RequestDTO.UPDATE_CONDITIONS:
                    ur = dataUtil.updateConditions(req.getConditions());
                    break;
                case RequestDTO.SEND_INVITE_TO_TEAM_MEMBER:
                    ur = cloudMsgUtil.sendInviteToTeam(req.getTeamMemberID(), req.getTeamID(), platformUtil, dataUtil);
                    break;
                case RequestDTO.UPDATE_STREAM:
                    ur = dataUtil.updateStream(req.getStream());
                    break;
                case RequestDTO.LIST_EVALUATION_BY_TEAM_MEMBER:
                    ur = listUtil.getEvaluationByTeamMember(req.getTeamMemberID());
                    break;
                case RequestDTO.LIST_EVALUATION_BY_CONDITIONS:
                    ur = listUtil.getEvaluationByCondtions(req.getConditionsID());
                    break;
                case RequestDTO.LIST_EVALUATION_SITE_BY_CATEGORY:
                    ur = listUtil.getEvaluationSiteByCategory(req.getCategoryID());
                    break;
                case RequestDTO.LIST_EVALUATION_INSECT_BY_EVALUATION:
                    ur = listUtil.getEvaluationInsectByEvaluation(req.getEvaluationID());
                    break;
                case RequestDTO.LIST_TEAMS_BY_TOWN:
                    ur = listUtil.getTeamByTown(req.getTownID());
                    break;
                case RequestDTO.LIST_TEAM_MEMBERS:
                    ur = listUtil.getTeamMemberList();
                    break;
                case RequestDTO.LIST_REGISTER_DATA:
                    ur = listUtil.registrationData();
                    break;
                case RequestDTO.LIST_CATEGORY:
                    ur = listUtil.getCategoryList();
                    break;
                case RequestDTO.LIST_COMMENTS:
                    ur = listUtil.getCommentList();
                    break;

                case RequestDTO.LIST_EVALUATIONS:
                    ur = listUtil.getEvaluationList();
                    break;
                case RequestDTO.LIST_RIVERS:
                    ur = listUtil.getRiverList();
                    break;
                case RequestDTO.GET_DATA:
                    ur = listUtil.getData();
                    break;
                case RequestDTO.LIST_DATA_WITH_RADIUS_RIVERS:
                    ur = listUtil.getRiverData(req.getLatitude(), req.getLongitude(), req.getRadius(), req.getType(), 2);
                    break;

                case RequestDTO.GET_RIVERS_BY_RADIUS:
                    ur.setRiverList(dataWorker.getRiversWithinRadius(req.getLatitude(),
                            req.getLongitude(), req.getRadius(), req.getType(), 2));
                    break;
                case RequestDTO.LIST_EVALUATION_BY_RIVER_ID:
                    ur = listUtil.getEvaluationSiteByRiver(req.getRiverID());
                    break;

                case RequestDTO.LIST_STREAM:
                    ur = listUtil.getStream();
                    break;

                case RequestDTO.LIST_BY_STREAM_NAME:
                    ur = listUtil.getStreamByStreamName(req.getStream().getStreamName());
                    break;

                case RequestDTO.ADD_TEAM:
                    dataUtil.addTeam(req.getTeam(), req.getTeamMemberID());
                    ur.setMessage("Member registered team");
                    break;
                case RequestDTO.SEARCH_MEMBERS:
                    ur = listUtil.searchForMembers(req.getSearch(), req.getEmail());
                    break;

                default:
                    ur.setStatusCode(444);
                    ur.setMessage("#### Unknown Request");
                    logger.log(Level.SEVERE, "Couldn't find request,please try again");
                    break;

            }
        } catch (DataException e) {
            ur.setStatusCode(101);
            ur.setMessage("Data service failed to process your request");
            logger.log(Level.SEVERE, "Database related failure", e);

        } catch (Exception e) {
            ur.setStatusCode(102);
            ur.setMessage("Server process failed to process your request");
            logger.log(Level.SEVERE, "Generic server related failure", e);

        }
        if (ur.getStatusCode() == null) {
            ur.setStatusCode(0);
        }
        long end = System.currentTimeMillis();
        double elapsed = Elapsed.getElapsed(start, end);
        ur.setElapsedRequestTimeInSeconds(elapsed);
        logger.log(Level.WARNING, "*********** request elapsed time: {0} seconds", elapsed);
        return ur;
    }
    @PersistenceContext
    EntityManager em;
    static final Logger logger = Logger.getLogger(TrafficCop.class.getSimpleName());
}
