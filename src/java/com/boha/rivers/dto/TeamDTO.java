/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.Team;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author CodeTribe1
 */
public class TeamDTO implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer teamID, countryID, organisationTypeID;
    private String teamName;
    private long dateRegistered;
    private String teamImage;
    private CountryDTO country;
    private OrganisationtypeDTO organisationType;

    private List<TeamMemberDTO> teammemberList = new ArrayList<>();
    private List<TmemberDTO> tmemberList = new ArrayList<>();

    public TeamDTO() {
    }

    public TeamDTO(Team c) {
        teamID = c.getTeamID();
        teamName = c.getTeamName();
        dateRegistered = c.getDateRegistered().getTime();
        teamImage = c.getTeamImage();
        countryID = c.getCountry().getCountryID();
        organisationTypeID = c.getOrganisationType().getOrganisationTypeID();
    }

    public List<TmemberDTO> getTmemberList() {
        return tmemberList;
    }

    public void setTmemberList(List<TmemberDTO> tmemberList) {
        this.tmemberList = tmemberList;
    }

    public Integer getTeamID() {
        return teamID;
    }

    public void setTeamID(Integer teamID) {
        this.teamID = teamID;
    }

    public String getTeamName() {
        return teamName;
    }

    public void setTeamName(String teamName) {
        this.teamName = teamName;
    }

    public long getDateRegistered() {
        return dateRegistered;
    }

    public void setDateRegistered(long dateRegistered) {
        this.dateRegistered = dateRegistered;
    }

    public String getTeamImage() {
        return teamImage;
    }

    public void setTeamImage(String teamImage) {
        this.teamImage = teamImage;
    }

    public List<TeamMemberDTO> getTeammemberList() {
        return teammemberList;
    }

    public void setTeammemberList(List<TeamMemberDTO> teammemberList) {
        this.teammemberList = teammemberList;
    }

    public Integer getCountryID() {
        return countryID;
    }

    public void setCountryID(Integer countryID) {
        this.countryID = countryID;
    }

    public Integer getOrganisationTypeID() {
        return organisationTypeID;
    }

    public void setOrganisationTypeID(Integer organisationTypeID) {
        this.organisationTypeID = organisationTypeID;
    }

    public CountryDTO getCountry() {
        return country;
    }

    public void setCountry(CountryDTO country) {
        this.country = country;
    }

    public OrganisationtypeDTO getOrganisationType() {
        return organisationType;
    }

    public void setOrganisationType(OrganisationtypeDTO organisationType) {
        this.organisationType = organisationType;
    }

}
