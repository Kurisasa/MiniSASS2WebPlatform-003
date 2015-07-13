/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.data;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 *
 * @author CodeTribe1
 */
@Entity
@Table(name = "team")
@NamedQueries({
    @NamedQuery(name = "Team.findAll", query = "SELECT t FROM Team t"),
    @NamedQuery(name = "Team.findByTeamID", query = "SELECT t FROM Team t WHERE t.teamID = :teamID"),
    @NamedQuery(name = "Team.findByTeamName", query = "SELECT t FROM Team t WHERE t.teamName = :teamName"),
    @NamedQuery(name = "Team.findByDateRegistered", query = "SELECT t FROM Team t WHERE t.dateRegistered = :dateRegistered"),
    @NamedQuery(name = "Team.findByTeamImage", query = "SELECT t FROM Team t WHERE t.teamImage = :teamImage")})
public class Team implements Serializable {
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "team")
    private List<Tmember> tmemberList;
    @JoinColumn(name = "organisationTypeID", referencedColumnName = "organisationTypeID")
    @ManyToOne(optional = false)
    private Organisationtype organisationType;
    @JoinColumn(name = "countryID", referencedColumnName = "countryID")
    @ManyToOne(optional = false)
    private Country country;
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "teamID")
    private Integer teamID;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 255)
    @Column(name = "teamName")
    private String teamName;
    @Basic(optional = false)
    @NotNull
    @Column(name = "dateRegistered")
    @Temporal(TemporalType.TIMESTAMP)
    private Date dateRegistered;
    @Size(max = 1000)
    @Column(name = "teamImage")
    private String teamImage;
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "team")
    private List<Teammember> teammemberList;

    public Team() {
    }

    public Team(Integer teamID) {
        this.teamID = teamID;
    }

    public Team(Integer teamID, String teamName, Date dateRegistered) {
        this.teamID = teamID;
        this.teamName = teamName;
        this.dateRegistered = dateRegistered;
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

    public Date getDateRegistered() {
        return dateRegistered;
    }

    public void setDateRegistered(Date dateRegistered) {
        this.dateRegistered = dateRegistered;
    }

    public String getTeamImage() {
        return teamImage;
    }

    public void setTeamImage(String teamImage) {
        this.teamImage = teamImage;
    }

    public List<Teammember> getTeammemberList() {
        return teammemberList;
    }

    public void setTeammemberList(List<Teammember> teammemberList) {
        this.teammemberList = teammemberList;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (teamID != null ? teamID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Team)) {
            return false;
        }
        Team other = (Team) object;
        if ((this.teamID == null && other.teamID != null) || (this.teamID != null && !this.teamID.equals(other.teamID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.Team[ teamID=" + teamID + " ]";
    }

    public Organisationtype getOrganisationType() {
        return organisationType;
    }

    public void setOrganisationType(Organisationtype organisationType) {
        this.organisationType = organisationType;
    }

    public Country getCountry() {
        return country;
    }

    public void setCountry(Country country) {
        this.country = country;
    }

    public List<Tmember> getTmemberList() {
        return tmemberList;
    }

    public void setTmemberList(List<Tmember> tmemberList) {
        this.tmemberList = tmemberList;
    }

    
}
