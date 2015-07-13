/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.transfer;

import com.boha.rivers.dto.CategoryDTO;
import com.boha.rivers.dto.CommentDTO;
import com.boha.rivers.dto.ConditionsDTO;
import com.boha.rivers.dto.CountryDTO;
import com.boha.rivers.dto.EvaluationDTO;
import com.boha.rivers.dto.EvaluationImageDTO;
import com.boha.rivers.dto.EvaluationInsectDTO;
import com.boha.rivers.dto.EvaluationSiteDTO;
import com.boha.rivers.dto.InsectDTO;
import com.boha.rivers.dto.InsectImageDTO;
import com.boha.rivers.dto.InsectImageListDTO;
import com.boha.rivers.dto.OrganisationtypeDTO;
import com.boha.rivers.dto.RiverDTO;
import com.boha.rivers.dto.StreamDTO;
import com.boha.rivers.dto.TeamDTO;
import com.boha.rivers.dto.TeamMemberDTO;
import com.boha.rivers.dto.TmemberDTO;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author aubreyM
 */
public class ResponseDTO implements Serializable {

    private Integer statusCode;
    private String message;
    private Integer statusCountInPeriod, goodCount, badCount;
    private String sessionID, GCMRegistrationID, fileString;
    private double elapsedRequestTimeInSeconds;

    private TeamDTO team;
    private TeamMemberDTO teamMember;
    private EvaluationDTO evaluation;
    private TmemberDTO tMember;
    private List<TmemberDTO> tmemberList = new ArrayList<>();
    private List<StreamDTO> streamList = new ArrayList<>();
    private List<EvaluationImageDTO> evaluationImageList = new ArrayList<>();
    private List<TeamDTO> teamList = new ArrayList<>();
    private List<TeamMemberDTO> teamMemberList = new ArrayList<>();
    private List<RiverDTO> riverList = new ArrayList<>();
    private List<InsectDTO> insectList = new ArrayList<>();
    private List<CommentDTO> commentList = new ArrayList<>();
    private List<EvaluationDTO> evaluationList = new ArrayList<>();
    private List<EvaluationSiteDTO> evaluationSiteList = new ArrayList<>();
    private List<CategoryDTO> categoryList = new ArrayList<>();
    private List<EvaluationInsectDTO> evaluationInsectList = new ArrayList<>();
    private List<ConditionsDTO> conditionsList = new ArrayList<>();
    private List<OrganisationtypeDTO> organisationtypeList = new ArrayList<>();
    private List<CountryDTO> countryList = new ArrayList<>();
    private List<InsectImageDTO> insectimageDTOList = new ArrayList<>();
    private List<InsectImageListDTO> insectImageListDTOs = new ArrayList<>();

    public TmemberDTO gettMember() {
        return tMember;
    }

    public void settMember(TmemberDTO tMember) {
        this.tMember = tMember;
    }

    public List<TmemberDTO> getTmemberList() {
        return tmemberList;
    }

    public void setTmemberList(List<TmemberDTO> tmemberList) {
        this.tmemberList = tmemberList;
    }

    public List<StreamDTO> getStreamList() {
        return streamList;
    }

    public void setStreamList(List<StreamDTO> streamList) {
        this.streamList = streamList;
    }

    public List<OrganisationtypeDTO> getOrganisationtypeList() {
        return organisationtypeList;
    }

    public void setOrganisationtypeList(List<OrganisationtypeDTO> organisationtypeList) {
        this.organisationtypeList = organisationtypeList;
    }

    public List<CountryDTO> getCountryList() {
        return countryList;
    }

    public void setCountryList(List<CountryDTO> countryList) {
        this.countryList = countryList;
    }

    public EvaluationDTO getEvaluation() {
        return evaluation;
    }

    public void setEvaluation(EvaluationDTO evaluation) {
        this.evaluation = evaluation;
    }

    public List<EvaluationImageDTO> getEvaluationImageList() {
        return evaluationImageList;
    }

    public void setEvaluationImageList(List<EvaluationImageDTO> evaluationImageList) {
        this.evaluationImageList = evaluationImageList;
    }

    public TeamDTO getTeam() {
        return team;
    }

    public void setTeam(TeamDTO team) {
        this.team = team;
    }

    public TeamMemberDTO getTeamMember() {
        return teamMember;
    }

    public void setTeamMember(TeamMemberDTO teamMember) {
        this.teamMember = teamMember;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(Integer statusCode) {
        this.statusCode = statusCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<TeamDTO> getTeamList() {
        return teamList;
    }

    public void setTeamList(List<TeamDTO> teamList) {
        this.teamList = teamList;
    }

    public List<TeamMemberDTO> getTeamMemberList() {
        return teamMemberList;
    }

    public void setTeamMemberList(List<TeamMemberDTO> teamMemberList) {
        this.teamMemberList = teamMemberList;
    }

    public List<RiverDTO> getRiverList() {
        return riverList;
    }

    public void setRiverList(List<RiverDTO> riverList) {
        this.riverList = riverList;
    }

    public List<InsectDTO> getInsectList() {
        return insectList;
    }

    public List<EvaluationInsectDTO> getEvaluationInsectList() {
        return evaluationInsectList;
    }

    public void setEvaluationInsectList(List<EvaluationInsectDTO> evaluationInsectList) {
        this.evaluationInsectList = evaluationInsectList;
    }

    public void setInsectList(List<InsectDTO> insectList) {
        this.insectList = insectList;
    }

    public List<CommentDTO> getCommentList() {
        return commentList;
    }

    public void setCommentList(List<CommentDTO> commentList) {
        this.commentList = commentList;
    }

    public List<EvaluationDTO> getEvaluationList() {
        return evaluationList;
    }

    public void setEvaluationList(List<EvaluationDTO> evaluationList) {
        this.evaluationList = evaluationList;
    }

    public List<EvaluationSiteDTO> getEvaluationSiteList() {
        return evaluationSiteList;
    }

    public void setEvaluationSiteList(List<EvaluationSiteDTO> evaluationSiteList) {
        this.evaluationSiteList = evaluationSiteList;
    }

    public List<CategoryDTO> getCategoryList() {
        return categoryList;
    }

    public void setCategoryList(List<CategoryDTO> categoryList) {
        this.categoryList = categoryList;
    }

    public List<ConditionsDTO> getConditionsList() {
        return conditionsList;
    }

    public void setConditionsList(List<ConditionsDTO> conditionsList) {
        this.conditionsList = conditionsList;
    }

    public Integer getStatusCountInPeriod() {
        return statusCountInPeriod;
    }

    public void setStatusCountInPeriod(Integer statusCountInPeriod) {
        this.statusCountInPeriod = statusCountInPeriod;
    }

    public Integer getGoodCount() {
        return goodCount;
    }

    public void setGoodCount(Integer goodCount) {
        this.goodCount = goodCount;
    }

    public Integer getBadCount() {
        return badCount;
    }

    public void setBadCount(Integer badCount) {
        this.badCount = badCount;
    }

    public String getSessionID() {
        return sessionID;
    }

    public void setSessionID(String sessionID) {
        this.sessionID = sessionID;
    }

    public String getGCMRegistrationID() {
        return GCMRegistrationID;
    }

    public void setGCMRegistrationID(String GCMRegistrationID) {
        this.GCMRegistrationID = GCMRegistrationID;
    }

    public String getFileString() {
        return fileString;
    }

    public void setFileString(String fileString) {
        this.fileString = fileString;
    }

    public double getElapsedRequestTimeInSeconds() {
        return elapsedRequestTimeInSeconds;
    }

    public void setElapsedRequestTimeInSeconds(double elapsedRequestTimeInSeconds) {
        this.elapsedRequestTimeInSeconds = elapsedRequestTimeInSeconds;
    }

    public List<InsectImageDTO> getInsectimageDTOList() {
        return insectimageDTOList;
    }

    public void setInsectimageDTOList(List<InsectImageDTO> insectimageDTOList) {
        this.insectimageDTOList = insectimageDTOList;
    }

    public List<InsectImageListDTO> getInsectImageListDTOs() {
        return insectImageListDTOs;
    }

    public void setInsectImageListDTOs(List<InsectImageListDTO> insectImageListDTOs) {
        this.insectImageListDTOs = insectImageListDTOs;
    }

}
