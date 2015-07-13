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
import javax.persistence.FetchType;
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
@Table(name = "river")
@NamedQueries({
    @NamedQuery(name = "River.findAll", query = "SELECT r FROM River r"),
    @NamedQuery(name = "River.findByRiverID", query = "SELECT r FROM River r WHERE r.riverID = :riverID"),
    @NamedQuery(name = "River.findByRiverName", query = "SELECT r FROM River r WHERE r.riverName = :riverName")})
public class River implements Serializable {
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "river")
    private List<Stream> streamList;
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "riverID")
    private Integer riverID;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 255)
    @Column(name = "riverName")
    private String riverName;
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "river", fetch = FetchType.LAZY)
    private List<Evaluationsite> evaluationsiteList;
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "river", fetch = FetchType.LAZY)
    private List<Riverpart> riverpartList;

    public River() {
    }

    public River(Integer riverID) {
        this.riverID = riverID;
    }

    public River(Integer riverID, String riverName) {
        this.riverID = riverID;
        this.riverName = riverName;
    }

    public Integer getRiverID() {
        return riverID;
    }

    public void setRiverID(Integer riverID) {
        this.riverID = riverID;
    }

    public String getRiverName() {
        return riverName;
    }

    public void setRiverName(String riverName) {
        this.riverName = riverName;
    }

    public List<Evaluationsite> getEvaluationsiteList() {
        return evaluationsiteList;
    }

    public void setEvaluationsiteList(List<Evaluationsite> evaluationsiteList) {
        this.evaluationsiteList = evaluationsiteList;
    }

    public List<Riverpart> getRiverpartList() {
        return riverpartList;
    }

    public void setRiverpartList(List<Riverpart> riverpartList) {
        this.riverpartList = riverpartList;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (riverID != null ? riverID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof River)) {
            return false;
        }
        River other = (River) object;
        if ((this.riverID == null && other.riverID != null) || (this.riverID != null && !this.riverID.equals(other.riverID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.River[ riverID=" + riverID + " ]";
    }

    public List<Stream> getStreamList() {
        return streamList;
    }

    public void setStreamList(List<Stream> streamList) {
        this.streamList = streamList;
    }
    
}
