/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.Evaluation;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author CodeTribe1
 */
public class EvaluationDTO implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer evaluationID, evaluationImageID, teamMemberID, conditionsID, evaluationCommentID;
    private long evaluationDate;
    private String remarks, teamName, conditionName;
    private Double score;
    private Double pH;
    private Double waterTemperature;
    private Double oxygen;
    private Double waterClarity;
    private double latitude;
    private double longitude;
    private List<EvaluationImageDTO> evaluationimageList = new ArrayList<>();
    private TeamMemberDTO teamMember;
    private EvaluationSiteDTO evaluationSite;
    private ConditionsDTO conditions;
    private List<EvaluationInsectDTO> evaluationinsectList = new ArrayList<>();
    private List<EvaluationCommentDTO> evaluationcommentList = new ArrayList<>();

    public EvaluationDTO() {
    }

    public EvaluationDTO(Evaluation c) {
        evaluationID = c.getEvaluationID();
        evaluationDate = c.getEvaluationDate().getTime();
        remarks = c.getRemarks();
        score = c.getScore();
        pH = c.getPH();
        conditionsID = c.getConditions().getConditionsID();
        conditionName = c.getConditions().getConditionName();
        waterTemperature = c.getWaterTemperature();
        oxygen = c.getOxygen();
        waterClarity = c.getWaterClarity();
        latitude = c.getLatitude();
        longitude = c.getLongitude();
        teamMemberID = c.getTeamMember().getTeamMemberID();
        teamName = c.getTeamMember().getTeam().getTeamName();
        evaluationSite = new EvaluationSiteDTO(c.getEvaluationSite());
    }

    public String getConditionName() {
        return conditionName;
    }

    public void setConditionName(String conditionName) {
        this.conditionName = conditionName;
    }

    public Integer getEvaluationID() {
        return evaluationID;
    }

    public String getTeamName() {
        return teamName;
    }

    public void setTeamName(String teamName) {
        this.teamName = teamName;
    }

    public void setEvaluationID(Integer evaluationID) {
        this.evaluationID = evaluationID;
    }

    public Integer getEvaluationImageID() {
        return evaluationImageID;
    }

    public void setEvaluationImageID(Integer evaluationImageID) {
        this.evaluationImageID = evaluationImageID;
    }

    public Integer getTeamMemberID() {
        return teamMemberID;
    }

    public void setTeamMemberID(Integer teamMemberID) {
        this.teamMemberID = teamMemberID;
    }

    public Integer getConditionsID() {
        return conditionsID;
    }

    public void setConditionsID(Integer conditionsID) {
        this.conditionsID = conditionsID;
    }

    public Integer getEvaluationCommentID() {
        return evaluationCommentID;
    }

    public void setEvaluationCommentID(Integer evaluationCommentID) {
        this.evaluationCommentID = evaluationCommentID;
    }

    public long getEvaluationDate() {
        return evaluationDate;
    }

    public void setEvaluationDate(long evaluationDate) {
        this.evaluationDate = evaluationDate;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public Double getpH() {
        return pH;
    }

    public void setpH(Double pH) {
        this.pH = pH;
    }

    public Double getWaterTemperature() {
        return waterTemperature;
    }

    public void setWaterTemperature(Double waterTemperature) {
        this.waterTemperature = waterTemperature;
    }

    public Double getOxygen() {
        return oxygen;
    }

    public void setOxygen(Double oxygen) {
        this.oxygen = oxygen;
    }

    public Double getWaterClarity() {
        return waterClarity;
    }

    public void setWaterClarity(Double waterClarity) {
        this.waterClarity = waterClarity;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public List<EvaluationImageDTO> getEvaluationimageList() {
        return evaluationimageList;
    }

    public void setEvaluationimageList(List<EvaluationImageDTO> evaluationimageList) {
        this.evaluationimageList = evaluationimageList;
    }

    public TeamMemberDTO getTeamMember() {
        return teamMember;
    }

    public void setTeamMember(TeamMemberDTO teamMember) {
        this.teamMember = teamMember;
    }

    public EvaluationSiteDTO getEvaluationSite() {
        return evaluationSite;
    }

    public void setEvaluationSite(EvaluationSiteDTO evaluationSite) {
        this.evaluationSite = evaluationSite;
    }

    public ConditionsDTO getConditions() {
        return conditions;
    }

    public void setConditions(ConditionsDTO conditions) {
        this.conditions = conditions;
    }

    public List<EvaluationInsectDTO> getEvaluationinsectList() {
        return evaluationinsectList;
    }

    public void setEvaluationinsectList(List<EvaluationInsectDTO> evaluationinsectList) {
        this.evaluationinsectList = evaluationinsectList;
    }

    public List<EvaluationCommentDTO> getEvaluationcommentList() {
        return evaluationcommentList;
    }

    public void setEvaluationcommentList(List<EvaluationCommentDTO> evaluationcommentList) {
        this.evaluationcommentList = evaluationcommentList;
    }

}
