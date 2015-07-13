/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.Riverpart;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author CodeTribe1
 */
public class RiverPartDTO implements Serializable {
    private static final long serialVersionUID = 1L;
    private Integer riverPartID, riverID;
    private String riverName;
    private int fNode;
    private int tNode;
    private int iprivID;
    private double distanceToMouth;
    private List<RiverPointDTO> riverpointList = new ArrayList<>();
    private RiverDTO river;

    public RiverPartDTO() {
    }

    public RiverPartDTO(Riverpart c) {
        riverPartID = c.getRiverPartID();
        riverID = c.getRiver().getRiverID();
        riverName = c.getRiverName();
        fNode = c.getFNode();
        tNode = c.getTNode();
        iprivID = c.getIprivID();
        distanceToMouth = c.getDistanceToMouth();
        
    }

    public Integer getRiverPartID() {
        return riverPartID;
    }

    public void setRiverPartID(Integer riverPartID) {
        this.riverPartID = riverPartID;
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

    public List<RiverPointDTO> getRiverpointList() {
        return riverpointList;
    }

    public void setRiverpointList(List<RiverPointDTO> riverpointList) {
        this.riverpointList = riverpointList;
    }

    public RiverDTO getRiver() {
        return river;
    }

    public void setRiver(RiverDTO river) {
        this.river = river;
    }
    
    
    
}
