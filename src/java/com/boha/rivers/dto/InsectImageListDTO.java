/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.*;
import java.io.Serializable;

/**
 *
 * @author CodeTribe1
 */
public class InsectImageListDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer insectimagelistID;
    private String imageName;
    private String url;
    private InsectImageDTO insectimage;

    public InsectImageListDTO() {
    }

    public InsectImageListDTO(Insectimagelist i) {
        this.insectimagelistID = i.getInsectimagelistID();
        imageName = i.getImageName();
        url = i.getUrl();
        insectimage = new InsectImageDTO(i.getInsectimage());
    }

    public InsectImageListDTO(Integer insectimagelistID, String url) {
        this.insectimagelistID = insectimagelistID;
        this.url = url;
    }

    public Integer getInsectimagelistID() {
        return insectimagelistID;
    }

    public void setInsectimagelistID(Integer insectimagelistID) {
        this.insectimagelistID = insectimagelistID;
    }

    public String getImageName() {
        return imageName;
    }

    public void setImageName(String imageName) {
        this.imageName = imageName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public InsectImageDTO getInsectimage() {
        return insectimage;
    }

    public void setInsectimage(InsectImageDTO insectimage) {
        this.insectimage = insectimage;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (insectimagelistID != null ? insectimagelistID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof InsectImageListDTO)) {
            return false;
        }
        InsectImageListDTO other = (InsectImageListDTO) object;
        if ((this.insectimagelistID == null && other.insectimagelistID != null) || (this.insectimagelistID != null && !this.insectimagelistID.equals(other.insectimagelistID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.Insectimagelist[ insectimagelistID=" + insectimagelistID + " ]";
    }

}
