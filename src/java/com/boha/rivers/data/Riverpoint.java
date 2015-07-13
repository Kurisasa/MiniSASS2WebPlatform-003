/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.data;

import java.io.Serializable;
import javax.persistence.Basic;
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
import javax.persistence.Table;
import javax.validation.constraints.NotNull;

/**
 *
 * @author CodeTribe1
 */
@Entity
@Table(name = "riverpoint")
@NamedQueries({
    @NamedQuery(name = "Riverpoint.findAll", query = "SELECT r FROM Riverpoint r"),
    @NamedQuery(name = "Riverpoint.findByRiverPointID", query = "SELECT r FROM Riverpoint r WHERE r.riverPointID = :riverPointID"),
    @NamedQuery(name = "Riverpoint.findByLatitude", query = "SELECT r FROM Riverpoint r WHERE r.latitude = :latitude"),
    @NamedQuery(name = "Riverpoint.findByEverything", 
            query = "SELECT r FROM Riverpoint r WHERE r.iprivID = :iprivID and r.longitude = :longitude and r.latitude = :latitude and r.riverPart.riverPartID = :riverPartID"),
    @NamedQuery(name = "Riverpoint.findByIprivID", query = "SELECT r FROM Riverpoint r WHERE r.iprivID = :iprivID")})
public class Riverpoint implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "riverPointID")
    private Integer riverPointID;
    @Basic(optional = false)
    @NotNull
    @Column(name = "latitude")
    private double latitude;
    @Basic(optional = false)
    @NotNull
    @Column(name = "longitude")
    private double longitude;
    @Basic(optional = false)
    @NotNull
    @Column(name = "iprivID")
    private int iprivID;
    @JoinColumn(name = "riverPartID", referencedColumnName = "riverPartID")
    @ManyToOne(optional = false, fetch = FetchType.LAZY)
    private Riverpart riverPart;

    public Riverpoint() {
    }

    public Riverpoint(Integer riverPointID) {
        this.riverPointID = riverPointID;
    }

    public Riverpoint(Integer riverPointID, double latitude, double longitude, int iprivID) {
        this.riverPointID = riverPointID;
        this.latitude = latitude;
        this.longitude = longitude;
        this.iprivID = iprivID;
    }

    public Integer getRiverPointID() {
        return riverPointID;
    }

    public void setRiverPointID(Integer riverPointID) {
        this.riverPointID = riverPointID;
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

    public int getIprivID() {
        return iprivID;
    }

    public void setIprivID(int iprivID) {
        this.iprivID = iprivID;
    }

    public Riverpart getRiverPart() {
        return riverPart;
    }

    public void setRiverPart(Riverpart riverPart) {
        this.riverPart = riverPart;
    }


    @Override
    public int hashCode() {
        int hash = 0;
        hash += (riverPointID != null ? riverPointID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Riverpoint)) {
            return false;
        }
        Riverpoint other = (Riverpoint) object;
        if ((this.riverPointID == null && other.riverPointID != null) || (this.riverPointID != null && !this.riverPointID.equals(other.riverPointID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.Riverpoint[ riverPointID=" + riverPointID + " ]";
    }
    
}
