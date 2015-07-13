/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.Riverpoint;
import java.io.Serializable;

/**
 *
 * @author CodeTribe1
 */
public class RiverPointDTO implements Serializable {
    private static final long serialVersionUID = 1L;
    private Integer riverPointID, riverPartID;
    private double latitude;
    private double longitude;
    private int iprivID;
    private RiverPartDTO riverPart;
    private Integer distance;

    public RiverPointDTO() {
    }

    public RiverPointDTO(Riverpoint c) {
        riverPointID = c.getRiverPointID();
        riverPartID = c.getRiverPart().getRiverPartID();
        latitude = c.getLatitude();
        longitude = c.getLongitude();
        iprivID = c.getIprivID();
    }

    public Integer getRiverPointID() {
        return riverPointID;
    }

    public void setRiverPointID(Integer riverPointID) {
        this.riverPointID = riverPointID;
    }

    public Integer getRiverPartID() {
        return riverPartID;
    }

    public void setRiverPartID(Integer riverPartID) {
        this.riverPartID = riverPartID;
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

    public RiverPartDTO getRiverPart() {
        return riverPart;
    }

    public void setRiverPart(RiverPartDTO riverPart) {
        this.riverPart = riverPart;
    }

    public Integer getDistance() {
        return distance;
    }

    public void setDistance(Integer distance) {
        this.distance = distance;
    }
    
    
    
}
