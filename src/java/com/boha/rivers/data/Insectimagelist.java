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
@Table(name = "insectimagelist")
@NamedQueries({
    @NamedQuery(name = "Insectimagelist.findAll", query = "SELECT i FROM Insectimagelist i"),
    @NamedQuery(name = "Insectimagelist.findByInsectimagelistID", query = "SELECT i FROM Insectimagelist i WHERE i.insectimagelistID = :insectimagelistID"),
    @NamedQuery(name = "Insectimagelist.findByImageName", query = "SELECT i FROM Insectimagelist i WHERE i.imageName = :imageName"),
    @NamedQuery(name = "Insectimagelist.findByUrl", query = "SELECT i FROM Insectimagelist i WHERE i.url = :url")})
public class Insectimagelist implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "insectimagelistID")
    private Integer insectimagelistID;
    @Size(max = 255)
    @Column(name = "imageName")
    private String imageName;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 255)
    @Column(name = "url")
    private String url;
    @JoinColumn(name = "insectimageID", referencedColumnName = "insectImageID")
    @ManyToOne(optional = false)
    private Insectimage insectimage;

    public Insectimagelist() {
    }

    public Insectimagelist(Integer insectimagelistID) {
        this.insectimagelistID = insectimagelistID;
    }

    public Insectimagelist(Integer insectimagelistID, String url) {
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

    public Insectimage getInsectimage() {
        return insectimage;
    }

    public void setInsectimage(Insectimage insectimage) {
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
        if (!(object instanceof Insectimagelist)) {
            return false;
        }
        Insectimagelist other = (Insectimagelist) object;
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
