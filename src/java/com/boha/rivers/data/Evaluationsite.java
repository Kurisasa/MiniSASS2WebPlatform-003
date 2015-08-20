/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.data;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 *
 * @author CodeTribe1
 */
@Entity
@Table(name = "evaluationsite")
@NamedQueries({
    @NamedQuery(name = "Evaluationsite.findAll", query = "SELECT e FROM Evaluationsite e"),
    @NamedQuery(name = "Evaluationsite.findByRiverID", query = "SELECT e FROM Evaluationsite e WHERE e.river.riverID =:riverID ORDER BY e.dateRegistered DESC"),
    @NamedQuery(name = "Evaluationsite.findByEvaluationSiteID", query = "SELECT e FROM Evaluationsite e WHERE e.evaluationSiteID = :evaluationSiteID"),
    @NamedQuery(name = "Evaluationsite.findByLatitude", query = "SELECT e FROM Evaluationsite e WHERE e.latitude = :latitude"),
    @NamedQuery(name = "Evaluationsite.findByLongitude", query = "SELECT e FROM Evaluationsite e WHERE e.longitude = :longitude"),
    @NamedQuery(name = "Evaluationsite.findByAccuracy", query = "SELECT e FROM Evaluationsite e WHERE e.accuracy = :accuracy"),
    @NamedQuery(name = "Evaluationsite.findByDateRegistered", query = "SELECT e FROM Evaluationsite e WHERE e.dateRegistered = :dateRegistered"),
    @NamedQuery(name = "Evaluationsite.findBySiteName", query = "SELECT e FROM Evaluationsite e WHERE e.siteName = :siteName"),
    @NamedQuery(name = "Evaluationsite.findByGID", query = "SELECT e FROM Evaluationsite e WHERE e.gID = :gID")})
public class Evaluationsite implements Serializable, Comparable<Evaluationsite> {

    @Size(max = 160)
    @Column(name = "siteName")
    private String siteName;
    @Column(name = "gID")
    private Integer gID;
    @Basic(optional = false)
    @NotNull
    @Lob
    @Size(min = 1, max = 65535)
    @Column(name = "description")
    private String description;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 160)
    @Column(name = "riverName")
    private String riverName;

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "evaluationSiteID")
    private Integer evaluationSiteID;
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
    @Column(name = "accuracy")
    private float accuracy;
    @Basic(optional = false)
    @NotNull
    @Column(name = "dateRegistered")
    @Temporal(TemporalType.TIMESTAMP)
    private Date dateRegistered;
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "evaluationSite")
    private List<Evaluation> evaluationList;
    @JoinColumn(name = "riverID", referencedColumnName = "riverID")
    @ManyToOne(optional = false)
    private River river;
    @JoinColumn(name = "categoryID", referencedColumnName = "categoryId")
    @ManyToOne(optional = false)
    private Category category;

    public Evaluationsite() {
    }

    public Evaluationsite(Integer evaluationSiteID) {
        this.evaluationSiteID = evaluationSiteID;
    }

    public Evaluationsite(Integer evaluationSiteID, double latitude, double longitude, float accuracy, Date dateRegistered) {
        this.evaluationSiteID = evaluationSiteID;
        this.latitude = latitude;
        this.longitude = longitude;
        this.accuracy = accuracy;
        this.dateRegistered = dateRegistered;
    }

    public Integer getEvaluationSiteID() {
        return evaluationSiteID;
    }

    public void setEvaluationSiteID(Integer evaluationSiteID) {
        this.evaluationSiteID = evaluationSiteID;
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

    public Date getDateRegistered() {
        return dateRegistered;
    }

    public void setDateRegistered(Date dateRegistered) {
        this.dateRegistered = dateRegistered;
    }

    public List<Evaluation> getEvaluationList() {
        return evaluationList;
    }

    public void setEvaluationList(List<Evaluation> evaluationList) {
        this.evaluationList = evaluationList;
    }

    public River getRiver() {
        return river;
    }

    public void setRiver(River river) {
        this.river = river;
    }

    public Category getCategory() {
        return category;
    }

    public void setCategory(Category category) {
        this.category = category;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (evaluationSiteID != null ? evaluationSiteID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Evaluationsite)) {
            return false;
        }
        Evaluationsite other = (Evaluationsite) object;
        if ((this.evaluationSiteID == null && other.evaluationSiteID != null) || (this.evaluationSiteID != null && !this.evaluationSiteID.equals(other.evaluationSiteID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.Evaluationsite[ evaluationSiteID=" + evaluationSiteID + " ]";
    }

    public String getSiteName() {
        return siteName;
    }

    public void setSiteName(String siteName) {
        this.siteName = siteName;
    }

    public Integer getGID() {
        return gID;
    }

    public void setGID(Integer gID) {
        this.gID = gID;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getRiverName() {
        return riverName;
    }

    public void setRiverName(String riverName) {
        this.riverName = riverName;
    }

    @Override
    public int compareTo(Evaluationsite o) {
        return this.riverName.compareTo(o.riverName);
    }

}
