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
@Table(name = "evaluation")
@NamedQueries({
    @NamedQuery(name = "Evaluation.findAll", query = "SELECT e FROM Evaluation e"),
     @NamedQuery(name = "Evaluation.findByevaluationSiteID", query = "SELECT e FROM Evaluation e WHERE e.evaluationSite.evaluationSiteID = :evaluationSiteID"),
    @NamedQuery(name = "Evaluation.findByEvaluationID", query = "SELECT e FROM Evaluation e WHERE e.evaluationID = :evaluationID"),
    @NamedQuery(name = "Evaluation.findByEvaluationDate", query = "SELECT e FROM Evaluation e WHERE e.evaluationDate = :evaluationDate"),
    @NamedQuery(name = "Evaluation.findByScore", query = "SELECT e FROM Evaluation e WHERE e.score = :score"),
    @NamedQuery(name = "Evaluation.findByPH", query = "SELECT e FROM Evaluation e WHERE e.pH = :pH"),
    @NamedQuery(name = "Evaluation.findByWaterTemperature", query = "SELECT e FROM Evaluation e WHERE e.waterTemperature = :waterTemperature"),
    @NamedQuery(name = "Evaluation.findByOxygen", query = "SELECT e FROM Evaluation e WHERE e.oxygen = :oxygen"),
    @NamedQuery(name = "Evaluation.findByWaterClarity", query = "SELECT e FROM Evaluation e WHERE e.waterClarity = :waterClarity"),
    @NamedQuery(name = "Evaluation.findByLatitude", query = "SELECT e FROM Evaluation e WHERE e.latitude = :latitude"),
    @NamedQuery(name = "Evaluation.findByLongitude", query = "SELECT e FROM Evaluation e WHERE e.longitude = :longitude")})
public class Evaluation implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "evaluationID")
    private Integer evaluationID;
    @Basic(optional = false)
    @NotNull
    @Column(name = "evaluationDate")
    @Temporal(TemporalType.TIMESTAMP)
    private Date evaluationDate;
    @Lob
    @Size(max = 65535)
    @Column(name = "remarks")
    private String remarks;
    // @Max(value=?)  @Min(value=?)//if you know range of your decimal fields consider using these annotations to enforce field validation
    @Column(name = "score")
    private Double score;
    @Column(name = "pH")
    private Double pH;
    @Column(name = "waterTemperature")
    private Double waterTemperature;
    @Column(name = "oxygen")
    private Double oxygen;
    @Column(name = "waterClarity")
    private Double waterClarity;
    @Basic(optional = false)
    @NotNull
    @Column(name = "latitude")
    private double latitude;
    @Basic(optional = false)
    @NotNull
    @Column(name = "longitude")
    private double longitude;
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "evaluation")
    private List<Evaluationimage> evaluationimageList;
    @JoinColumn(name = "teamMemberID", referencedColumnName = "teamMemberID")
    @ManyToOne(optional = false)
    private Teammember teamMember;
    @JoinColumn(name = "evaluationSiteID", referencedColumnName = "evaluationSiteID")
    @ManyToOne(optional = false)
    private Evaluationsite evaluationSite;
    @JoinColumn(name = "conditionsID", referencedColumnName = "conditionsID")
    @ManyToOne
    private Conditions conditions;
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "evaluation")
    private List<Evaluationinsect> evaluationinsectList;
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "evaluation")
    private List<Evaluationcomment> evaluationcommentList;

    public Evaluation() {
    }

    public Evaluation(Integer evaluationID) {
        this.evaluationID = evaluationID;
    }

    public Evaluation(Integer evaluationID, Date evaluationDate, double latitude, double longitude) {
        this.evaluationID = evaluationID;
        this.evaluationDate = evaluationDate;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public Integer getEvaluationID() {
        return evaluationID;
    }

    public void setEvaluationID(Integer evaluationID) {
        this.evaluationID = evaluationID;
    }

    public Date getEvaluationDate() {
        return evaluationDate;
    }

    public void setEvaluationDate(Date evaluationDate) {
        this.evaluationDate = evaluationDate;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public Double getPH() {
        return pH;
    }

    public void setPH(Double pH) {
        this.pH = pH;
    }

    public Double getWaterTemperature() {
        return waterTemperature;
    }

    public void setWaterTemperature(Double waterTemperature) {
        this.waterTemperature = waterTemperature;
    }

    public Double getOxygen() {
        return oxygen;
    }

    public void setOxygen(Double oxygen) {
        this.oxygen = oxygen;
    }

    public Double getWaterClarity() {
        return waterClarity;
    }

    public void setWaterClarity(Double waterClarity) {
        this.waterClarity = waterClarity;
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

    public List<Evaluationimage> getEvaluationimageList() {
        return evaluationimageList;
    }

    public void setEvaluationimageList(List<Evaluationimage> evaluationimageList) {
        this.evaluationimageList = evaluationimageList;
    }

    public Double getpH() {
        return pH;
    }

    public void setpH(Double pH) {
        this.pH = pH;
    }

    public Teammember getTeamMember() {
        return teamMember;
    }

    public void setTeamMember(Teammember teamMember) {
        this.teamMember = teamMember;
    }

    public Evaluationsite getEvaluationSite() {
        return evaluationSite;
    }

    public void setEvaluationSite(Evaluationsite evaluationSite) {
        this.evaluationSite = evaluationSite;
    }

    public Conditions getConditions() {
        return conditions;
    }

    public void setConditions(Conditions conditions) {
        this.conditions = conditions;
    }

    
    public List<Evaluationinsect> getEvaluationinsectList() {
        return evaluationinsectList;
    }

    public void setEvaluationinsectList(List<Evaluationinsect> evaluationinsectList) {
        this.evaluationinsectList = evaluationinsectList;
    }

    public List<Evaluationcomment> getEvaluationcommentList() {
        return evaluationcommentList;
    }

    public void setEvaluationcommentList(List<Evaluationcomment> evaluationcommentList) {
        this.evaluationcommentList = evaluationcommentList;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (evaluationID != null ? evaluationID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Evaluation)) {
            return false;
        }
        Evaluation other = (Evaluation) object;
        if ((this.evaluationID == null && other.evaluationID != null) || (this.evaluationID != null && !this.evaluationID.equals(other.evaluationID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.Evaluation[ evaluationID=" + evaluationID + " ]";
    }
    
}
