/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.River;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author CodeTribe1
 */
public class RiverDTO implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer riverID;
    private String riverName;
    private List<RiverPartDTO> riverpartList = new ArrayList<>();
    private List<EvaluationSiteDTO> evaluationsiteList = new ArrayList<>();
    private List<StreamDTO> streamList = new ArrayList<>();

    public RiverDTO() {
    }

    public RiverDTO(River c) {
        riverID = c.getRiverID();
        riverName = c.getRiverName();
    }

    public List<StreamDTO> getStreamList() {
        return streamList;
    }

    public void setStreamList(List<StreamDTO> streamList) {
        this.streamList = streamList;
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

    public List<RiverPartDTO> getRiverpartList() {
        return riverpartList;
    }

    public void setRiverpartList(List<RiverPartDTO> riverpartList) {
        this.riverpartList = riverpartList;
    }

    public List<EvaluationSiteDTO> getEvaluationsiteList() {
        return evaluationsiteList;
    }

    public void setEvaluationsiteList(List<EvaluationSiteDTO> evaluationsiteList) {
        this.evaluationsiteList = evaluationsiteList;
    }

}
