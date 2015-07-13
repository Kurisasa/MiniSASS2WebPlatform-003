/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.data;

import java.io.Serializable;
import java.util.List;
import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 *
 * @author CodeTribe1
 */
@Entity
@Table(name = "organisationtype")
@NamedQueries({
    @NamedQuery(name = "Organisationtype.findAll", query = "SELECT o FROM Organisationtype o"),
    @NamedQuery(name = "Organisationtype.findByOrganisationTypeID", query = "SELECT o FROM Organisationtype o WHERE o.organisationTypeID = :organisationTypeID"),
    @NamedQuery(name = "Organisationtype.findByOrganisationName", query = "SELECT o FROM Organisationtype o WHERE o.organisationName = :organisationName")})
public class Organisationtype implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "organisationTypeID")
    private Integer organisationTypeID;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 255)
    @Column(name = "organisationName")
    private String organisationName;
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "organisationType")
    private List<Team> teamList;

    public Organisationtype() {
    }

    public Organisationtype(Integer organisationTypeID) {
        this.organisationTypeID = organisationTypeID;
    }

    public Organisationtype(Integer organisationTypeID, String organisationName) {
        this.organisationTypeID = organisationTypeID;
        this.organisationName = organisationName;
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

    public List<Team> getTeamList() {
        return teamList;
    }

    public void setTeamList(List<Team> teamList) {
        this.teamList = teamList;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (organisationTypeID != null ? organisationTypeID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Organisationtype)) {
            return false;
        }
        Organisationtype other = (Organisationtype) object;
        if ((this.organisationTypeID == null && other.organisationTypeID != null) || (this.organisationTypeID != null && !this.organisationTypeID.equals(other.organisationTypeID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.Organisationtype[ organisationTypeID=" + organisationTypeID + " ]";
    }
    
}
