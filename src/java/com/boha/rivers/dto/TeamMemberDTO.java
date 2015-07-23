/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.Teammember;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author CodeTribe1
 */
public class TeamMemberDTO implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer teamMemberID, teamID, evaluationCount;
    private String firstName;
    private String lastName;
    private String email;
    private String cellphone;
    private long dateRegistered;
    private String pin;
    private Integer activeFlag;
    private String teamMemberImage;
    private List<EvaluationDTO> evaluationList = new ArrayList<>();
    private TeamDTO team;
    private List<TmemberDTO> tmemberList = new ArrayList<>();
    private List<GcmdeviceDTO> gcmdeviceList = new ArrayList<>();

    public TeamMemberDTO() {
    }

    public TeamMemberDTO(Teammember c) {
        teamMemberID = c.getTeamMemberID();
        teamID = c.getTeam().getTeamID();
        firstName = c.getFirstName();
        lastName = c.getLastName();
        email = c.getEmail();
        cellphone = c.getCellphone();
        dateRegistered = c.getDateRegistered().getTime();
        pin = c.getPin();
        activeFlag = c.getActiveFlag();
        teamMemberImage = c.getTeamMemberImage();
        // team = new TeamDTO(c.getTeam());
    }

    public List<GcmdeviceDTO> getGcmdeviceList() {
        return gcmdeviceList;
    }

    public void setGcmdeviceList(List<GcmdeviceDTO> gcmdeviceList) {
        this.gcmdeviceList = gcmdeviceList;
    }

    public List<TmemberDTO> getTmemberList() {
        return tmemberList;
    }

    public void setTmemberList(List<TmemberDTO> tmemberList) {
        this.tmemberList = tmemberList;
    }

    public Integer getEvaluationCount() {
        return evaluationCount;
    }

    public void setEvaluationCount(Integer evaluationCount) {
        this.evaluationCount = evaluationCount;
    }

    public Integer getTeamMemberID() {
        return teamMemberID;
    }

    public void setTeamMemberID(Integer teamMemberID) {
        this.teamMemberID = teamMemberID;
    }

    public Integer getTeamID() {
        return teamID;
    }

    public void setTeamID(Integer teamID) {
        this.teamID = teamID;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getCellphone() {
        return cellphone;
    }

    public void setCellphone(String cellphone) {
        this.cellphone = cellphone;
    }

    public long getDateRegistered() {
        return dateRegistered;
    }

    public void setDateRegistered(long dateRegistered) {
        this.dateRegistered = dateRegistered;
    }

    public String getPin() {
        return pin;
    }

    public void setPin(String pin) {
        this.pin = pin;
    }

    public Integer getActiveFlag() {
        return activeFlag;
    }

    public void setActiveFlag(Integer activeFlag) {
        this.activeFlag = activeFlag;
    }

    public String getTeamMemberImage() {
        return teamMemberImage;
    }

    public void setTeamMemberImage(String teamMemberImage) {
        this.teamMemberImage = teamMemberImage;
    }

    public List<EvaluationDTO> getEvaluationList() {
        return evaluationList;
    }

    public void setEvaluationList(List<EvaluationDTO> evaluationList) {
        this.evaluationList = evaluationList;
    }

    public TeamDTO getTeam() {
        return team;
    }

    public void setTeam(TeamDTO team) {
        this.team = team;
    }

}
