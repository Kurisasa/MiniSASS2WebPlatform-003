/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.Evaluationsite;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author CodeTribe1
 */
public class EvaluationSiteDTO implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer evaluationSiteID, categoryID, riverID;
    private String riverName;
    private double latitude;
    private double longitude;
    private float accuracy;
    private long dateRegistered;
    private List<EvaluationDTO> evaluationList = new ArrayList<>();
    private CategoryDTO category;
    private RiverDTO river;
    private StreamDTO stream;

    private String siteName;
    private Integer gID;
    private String description;
    private String riverName2;

    public EvaluationSiteDTO() {
    }

    public EvaluationSiteDTO(Evaluationsite c) {
        evaluationSiteID = c.getEvaluationSiteID();
        categoryID = c.getCategory().getCategoryId();
        riverID = c.getRiver().getRiverID();
        latitude = c.getLatitude();
        longitude = c.getLongitude();
        accuracy = c.getAccuracy();
        dateRegistered = c.getDateRegistered().getTime();
        categoryID = c.getCategory().getCategoryId();
        category = new CategoryDTO(c.getCategory());
        river = new RiverDTO(c.getRiver());
        riverName = c.getRiver().getRiverName();
        siteName = c.getSiteName();
        gID = c.getGID();
        description = c.getDescription();
        riverName2 = c.getRiverName();

    }

    public String getRiverName2() {
        return riverName2;
    }

    public void setRiverName2(String riverName2) {
        this.riverName2 = riverName2;
    }

    public String getSiteName() {
        return siteName;
    }

    public void setSiteName(String siteName) {
        this.siteName = siteName;
    }

    public Integer getgID() {
        return gID;
    }

    public void setgID(Integer gID) {
        this.gID = gID;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public StreamDTO getStream() {
        return stream;
    }

    public void setStream(StreamDTO stream) {
        this.stream = stream;
    }

    public String getRiverName() {
        return riverName;
    }

    public void setRiverName(String riverName) {
        this.riverName = riverName;
    }

    public Integer getRiverID() {
        return riverID;
    }

    public void setRiverID(Integer riverID) {
        this.riverID = riverID;
    }

    public RiverDTO getRiver() {
        return river;
    }

    public void setRiver(RiverDTO river) {
        this.river = river;
    }

    public Integer getEvaluationSiteID() {
        return evaluationSiteID;
    }

    public void setEvaluationSiteID(Integer evaluationSiteID) {
        this.evaluationSiteID = evaluationSiteID;
    }

    public Integer getCategoryID() {
        return categoryID;
    }

    public void setCategoryID(Integer categoryID) {
        this.categoryID = categoryID;
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

    public float getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(float accuracy) {
        this.accuracy = accuracy;
    }

    public long getDateRegistered() {
        return dateRegistered;
    }

    public void setDateRegistered(long dateRegistered) {
        this.dateRegistered = dateRegistered;
    }

    public List<EvaluationDTO> getEvaluationList() {
        return evaluationList;
    }

    public void setEvaluationList(List<EvaluationDTO> evaluationList) {
        this.evaluationList = evaluationList;
    }

    public CategoryDTO getCategory() {
        return category;
    }

    public void setCategory(CategoryDTO category) {
        this.category = category;
    }

}
