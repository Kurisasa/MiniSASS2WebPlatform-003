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
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 *
 * @author CodeTribe1
 */
@Entity
@Table(name = "stream")
@NamedQueries({
    @NamedQuery(name = "Stream.findAll", query = "SELECT s FROM Stream s"),
    @NamedQuery(name = "Stream.findByStreamID", query = "SELECT s FROM Stream s WHERE s.streamID = :streamID"),
    @NamedQuery(name = "Stream.findByStreamName", query = "SELECT s FROM Stream s WHERE s.streamName = :streamName"),
    @NamedQuery(name = "Stream.findByLatitude", query = "SELECT s FROM Stream s WHERE s.latitude = :latitude"),
    @NamedQuery(name = "Stream.findByLongitude", query = "SELECT s FROM Stream s WHERE s.longitude = :longitude")})
public class Stream implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "streamID")
    private Integer streamID;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 45)
    @Column(name = "streamName")
    private String streamName;
    @Basic(optional = false)
    @NotNull
    @Column(name = "latitude")
    private double latitude;
    @Basic(optional = false)
    @NotNull
    @Column(name = "longitude")
    private double longitude;
    @JoinColumn(name = "riverID", referencedColumnName = "riverID")
    @ManyToOne(optional = false)
    private River river;

    public Stream() {
    }

    public Stream(Integer streamID) {
        this.streamID = streamID;
    }

    public Stream(Integer streamID, String streamName, double latitude, double longitude) {
        this.streamID = streamID;
        this.streamName = streamName;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public Integer getStreamID() {
        return streamID;
    }

    public void setStreamID(Integer streamID) {
        this.streamID = streamID;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
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

    public River getRiver() {
        return river;
    }

    public void setRiver(River river) {
        this.river = river;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (streamID != null ? streamID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Stream)) {
            return false;
        }
        Stream other = (Stream) object;
        if ((this.streamID == null && other.streamID != null) || (this.streamID != null && !this.streamID.equals(other.streamID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.Stream[ streamID=" + streamID + " ]";
    }

}
