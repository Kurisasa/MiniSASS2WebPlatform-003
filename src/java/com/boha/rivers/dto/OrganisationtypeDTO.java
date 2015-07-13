/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.Organisationtype;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author CodeTribe1
 */
public class OrganisationtypeDTO implements Serializable {
    private static final long serialVersionUID = 1L;
    private Integer organisationTypeID;
    private String organisationName;
    private List<TeamDTO> teamList = new ArrayList<>();
    
    public OrganisationtypeDTO(){
        
    }
    
     public OrganisationtypeDTO(Organisationtype o){
         this.organisationName = o.getOrganisationName();
         this.organisationTypeID = o.getOrganisationTypeID();
         
        
    }

    public Integer getOrganisationTypeID() {
        return organisationTypeID;
    }

    public void setOrganisationTypeID(Integer organisationTypeID) {
        this.organisationTypeID = organisationTypeID;
    }

    public String getOrganisationName() {
        return organisationName;
    }

    public void setOrganisationName(String organisationName) {
        this.organisationName = organisationName;
    }

    public List<TeamDTO> getTeamList() {
        return teamList;
    }

    public void setTeamList(List<TeamDTO> teamList) {
        this.teamList = teamList;
    }
     
     
}
