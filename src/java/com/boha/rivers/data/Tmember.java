/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.data;

import java.io.Serializable;
import java.util.Date;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotNull;

/**
 *
 * @author sifiso
 */
@Entity
@Table(name = "tmember")
@NamedQueries({
    @NamedQuery(name = "Tmember.findAll", query = "SELECT t FROM Tmember t"),
    @NamedQuery(name = "Tmember.findByTeamID", query = "SELECT t FROM Tmember t WHERE t.team.teamID = :teamID"),
    @NamedQuery(name = "Tmember.findByTeamMemberID", query = "SELECT t FROM Tmember t WHERE t.teamMember.teamMemberID = :teamMemberID"),
    @NamedQuery(name = "Tmember.findInvite", query = "SELECT t FROM Tmember t WHERE t.team.teamID = :teamID AND t.teamMember.teamMemberID = :teamMemberID"),
    @NamedQuery(name = "Tmember.findByTmemberID", query = "SELECT t FROM Tmember t WHERE t.tmemberID = :tmemberID"),
    @NamedQuery(name = "Tmember.findByDateCreated", query = "SELECT t FROM Tmember t WHERE t.dateCreated = :dateCreated")})
public class Tmember implements Serializable {
    @Basic(optional = false)
    @NotNull
    @Column(name = "acceptInvite")
    private int acceptInvite;

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "tmemberID")
    private Integer tmemberID;
    @Basic(optional = false)
    @NotNull
    @Column(name = "dateCreated")
    @Temporal(TemporalType.TIMESTAMP)
    private Date dateCreated;
    @JoinColumn(name = "teamID", referencedColumnName = "teamID")
    @ManyToOne(optional = false)
    private Team team;
    @JoinColumn(name = "teamMemberID", referencedColumnName = "teamMemberID")
    @ManyToOne(optional = false)
    private Teammember teamMember;

    public Tmember() {
    }

    public Tmember(Integer tmemberID) {
        this.tmemberID = tmemberID;
    }

    public Tmember(Integer tmemberID, Date dateCreated) {
        this.tmemberID = tmemberID;
        this.dateCreated = dateCreated;
    }

    public Integer getTmemberID() {
        return tmemberID;
    }

    public void setTmemberID(Integer tmemberID) {
        this.tmemberID = tmemberID;
    }

    public Date getDateCreated() {
        return dateCreated;
    }

    public void setDateCreated(Date dateCreated) {
        this.dateCreated = dateCreated;
    }

    public Team getTeam() {
        return team;
    }

    public void setTeam(Team team) {
        this.team = team;
    }

    public Teammember getTeamMember() {
        return teamMember;
    }

    public void setTeamMember(Teammember teamMember) {
        this.teamMember = teamMember;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (tmemberID != null ? tmemberID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Tmember)) {
            return false;
        }
        Tmember other = (Tmember) object;
        if ((this.tmemberID == null && other.tmemberID != null) || (this.tmemberID != null && !this.tmemberID.equals(other.tmemberID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.Tmember[ tmemberID=" + tmemberID + " ]";
    }

    public int getAcceptInvite() {
        return acceptInvite;
    }

    public void setAcceptInvite(int acceptInvite) {
        this.acceptInvite = acceptInvite;
    }

}
