/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.Insectimage;
import com.boha.rivers.data.Insectimagelist;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author CodeTribe1
 */
public class InsectImageDTO implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer insectImageID, insectID;
    private String uri;
    private long dateRegistered;
    private InsectDTO insect;
    private List<InsectImageListDTO> insectimagelistList = new ArrayList<>();

    public InsectImageDTO() {
    }

    public InsectImageDTO(Insectimage c) {

        insectImageID = c.getInsectImageID();
        insectID = c.getInsect().getInsectID();
        uri = c.getUri();
        dateRegistered = c.getDateRegistered().getTime();
        insect = new InsectDTO(c.getInsect());
    }

    public Integer getInsectImageID() {
        return insectImageID;
    }

    public List<InsectImageListDTO> getInsectimagelistList() {
        return insectimagelistList;
    }

    public void setInsectimagelistList(List<InsectImageListDTO> insectimagelistList) {
        this.insectimagelistList = insectimagelistList;
    }

    public void setInsectImageID(Integer insectImageID) {
        this.insectImageID = insectImageID;
    }

    public Integer getInsectID() {
        return insectID;
    }

    public void setInsectID(Integer insectID) {
        this.insectID = insectID;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public long getDateRegistered() {
        return dateRegistered;
    }

    public void setDateRegistered(long dateRegistered) {
        this.dateRegistered = dateRegistered;
    }

    public InsectDTO getInsect() {
        return insect;
    }

    public void setInsect(InsectDTO insect) {
        this.insect = insect;
    }

}
