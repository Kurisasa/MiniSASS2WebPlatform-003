/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.data;

import java.io.Serializable;
import java.util.Date;
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
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 *
 * @author CodeTribe1
 */
@Entity
@Table(name = "evaluationimage")
@NamedQueries({
    @NamedQuery(name = "Evaluationimage.findAll", query = "SELECT e FROM Evaluationimage e"),
    @NamedQuery(name = "Evaluationimage.findByEvaluationImageID", query = "SELECT e FROM Evaluationimage e WHERE e.evaluationImageID = :evaluationImageID"),
    @NamedQuery(name = "Evaluationimage.findByDateTaken", query = "SELECT e FROM Evaluationimage e WHERE e.dateTaken = :dateTaken"),
    @NamedQuery(name = "Evaluationimage.findByFileName", query = "SELECT e FROM Evaluationimage e WHERE e.fileName = :fileName"),
    @NamedQuery(name = "Evaluationimage.findByLongitude", query = "SELECT e FROM Evaluationimage e WHERE e.longitude = :longitude"),
    @NamedQuery(name = "Evaluationimage.findByLatitude", query = "SELECT e FROM Evaluationimage e WHERE e.latitude = :latitude"),
    @NamedQuery(name = "Evaluationimage.findByAccuracy", query = "SELECT e FROM Evaluationimage e WHERE e.accuracy = :accuracy")})
public class Evaluationimage implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "evaluationImageID")
    private Integer evaluationImageID;
    @Basic(optional = false)
    @NotNull
    @Column(name = "dateTaken")
    @Temporal(TemporalType.TIMESTAMP)
    private Date dateTaken;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 255)
    @Column(name = "fileName")
    private String fileName;
    @Basic(optional = false)
    @NotNull
    @Column(name = "longitude")
    private double longitude;
    @Basic(optional = false)
    @NotNull
    @Column(name = "latitude")
    private double latitude;
    @Basic(optional = false)
    @NotNull
    @Column(name = "accuracy")
    private float accuracy;
    @JoinColumn(name = "evaluationID", referencedColumnName = "evaluationID")
    @ManyToOne(optional = false)
    private Evaluation evaluation;

    public Evaluationimage() {
    }

    public Evaluationimage(Integer evaluationImageID) {
        this.evaluationImageID = evaluationImageID;
    }

    public Evaluationimage(Integer evaluationImageID, Date dateTaken, String fileName, double longitude, double latitude, float accuracy) {
        this.evaluationImageID = evaluationImageID;
        this.dateTaken = dateTaken;
        this.fileName = fileName;
        this.longitude = longitude;
        this.latitude = latitude;
        this.accuracy = accuracy;
    }

    public Integer getEvaluationImageID() {
        return evaluationImageID;
    }

    public void setEvaluationImageID(Integer evaluationImageID) {
        this.evaluationImageID = evaluationImageID;
    }

    public Date getDateTaken() {
        return dateTaken;
    }

    public void setDateTaken(Date dateTaken) {
        this.dateTaken = dateTaken;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public float getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(float accuracy) {
        this.accuracy = accuracy;
    }

    public Evaluation getEvaluation() {
        return evaluation;
    }

    public void setEvaluation(Evaluation evaluation) {
        this.evaluation = evaluation;
    }

   
    @Override
    public int hashCode() {
        int hash = 0;
        hash += (evaluationImageID != null ? evaluationImageID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Evaluationimage)) {
            return false;
        }
        Evaluationimage other = (Evaluationimage) object;
        if ((this.evaluationImageID == null && other.evaluationImageID != null) || (this.evaluationImageID != null && !this.evaluationImageID.equals(other.evaluationImageID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.Evaluationimage[ evaluationImageID=" + evaluationImageID + " ]";
    }
    
}
