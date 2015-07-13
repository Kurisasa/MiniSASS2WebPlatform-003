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
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
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
@Table(name = "riverpart")
@NamedQueries({
    @NamedQuery(name = "Riverpart.findByRiverPartID", query = "SELECT r FROM Riverpart r WHERE r.riverPartID = :riverPartID"),
    @NamedQuery(name = "Riverpart.findByRiverName", query = "SELECT r FROM Riverpart r WHERE r.riverName = :riverName"),
    @NamedQuery(name = "Riverpart.findByFNode", query = "SELECT r FROM Riverpart r WHERE r.fNode = :fNode"),
    @NamedQuery(name = "Riverpart.findByRiverPartList", 
            query = "SELECT r FROM Riverpart r WHERE r.riverPartID in :list order by r.riverName, r.distanceToMouth"),
    @NamedQuery(name = "Riverpart.findRiversByRiverPartList", 
            query = "SELECT distinct r.river FROM Riverpart r WHERE r.riverPartID in :list order by r.riverName"),
    @NamedQuery(name = "Riverpart.findByIprivID", 
            query = "SELECT r FROM Riverpart r WHERE r.iprivID = :iprivID")})
public class Riverpart implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "riverPartID")
    private Integer riverPartID;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 255)
    @Column(name = "riverName")
    private String riverName;
    @Basic(optional = false)
    @NotNull
    @Column(name = "fNode")
    private int fNode;
    @Basic(optional = false)
    @NotNull
    @Column(name = "tNode")
    private int tNode;
    @Basic(optional = false)
    @NotNull
    @Column(name = "iprivID")
    private int iprivID;
    @Basic(optional = false)
    @NotNull
    @Column(name = "distanceToMouth")
    private double distanceToMouth;
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "riverPart", fetch = FetchType.LAZY)
    private List<Riverpoint> riverpointList;
    @JoinColumn(name = "riverID", referencedColumnName = "riverID")
    @ManyToOne(optional = false, fetch = FetchType.LAZY)
    private River river;

    public Riverpart() {
    }

    public Riverpart(Integer riverPartID) {
        this.riverPartID = riverPartID;
    }

    public Riverpart(Integer riverPartID, String riverName, int fNode, int tNode, int iprivID, double distanceToMouth) {
        this.riverPartID = riverPartID;
        this.riverName = riverName;
        this.fNode = fNode;
        this.tNode = tNode;
        this.iprivID = iprivID;
        this.distanceToMouth = distanceToMouth;
    }

    public Integer getRiverPartID() {
        return riverPartID;
    }

    public void setRiverPartID(Integer riverPartID) {
        this.riverPartID = riverPartID;
    }

    public String getRiverName() {
        return riverName;
    }

    public void setRiverName(String riverName) {
        this.riverName = riverName;
    }

    public int getFNode() {
        return fNode;
    }

    public void setFNode(int fNode) {
        this.fNode = fNode;
    }

    public int getTNode() {
        return tNode;
    }

    public void setTNode(int tNode) {
        this.tNode = tNode;
    }

    public int getIprivID() {
        return iprivID;
    }

    public void setIprivID(int iprivID) {
        this.iprivID = iprivID;
    }

    public double getDistanceToMouth() {
        return distanceToMouth;
    }

    public void setDistanceToMouth(double distanceToMouth) {
        this.distanceToMouth = distanceToMouth;
    }

    public List<Riverpoint> getRiverpointList() {
        return riverpointList;
    }

    public void setRiverpointList(List<Riverpoint> riverpointList) {
        this.riverpointList = riverpointList;
    }

    public int getfNode() {
        return fNode;
    }

    public void setfNode(int fNode) {
        this.fNode = fNode;
    }

    public int gettNode() {
        return tNode;
    }

    public void settNode(int tNode) {
        this.tNode = tNode;
    }

    public River getRiver() {
        return river;
    }

    public void setRiver(River river) {
        this.river = river;
    }


    @Override
    public int hashCode() {
        int hash = 0;
        hash += (riverPartID != null ? riverPartID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Riverpart)) {
            return false;
        }
        Riverpart other = (Riverpart) object;
        if ((this.riverPartID == null && other.riverPartID != null) || (this.riverPartID != null && !this.riverPartID.equals(other.riverPartID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.Riverpart[ riverPartID=" + riverPartID + " ]";
    }
    
}
