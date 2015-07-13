/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.data;

import java.io.Serializable;
import java.util.List;
import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 *
 * @author CodeTribe1
 */
@Entity
@Table(name = "insect")
@NamedQueries({
    @NamedQuery(name = "Insect.findAll", query = "SELECT i FROM Insect i"),
    @NamedQuery(name = "Insect.findByInsectID", query = "SELECT i FROM Insect i WHERE i.insectID = :insectID"),
    @NamedQuery(name = "Insect.findByGroupName", query = "SELECT i FROM Insect i WHERE i.groupName = :groupName"),
    @NamedQuery(name = "Insect.findBySensitivityScore", query = "SELECT i FROM Insect i WHERE i.sensitivityScore = :sensitivityScore")})
public class Insect implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "insectID")
    private Integer insectID;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 255)
    @Column(name = "groupName")
    private String groupName;
    @Basic(optional = false)
    @NotNull
    @Column(name = "sensitivityScore")
    private int sensitivityScore;
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "insect")
    private List<Evaluationinsect> evaluationinsectList;
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "insect")
    private List<Insectimage> insectimageList;

    public Insect() {
    }

    public Insect(Integer insectID) {
        this.insectID = insectID;
    }

    public Insect(Integer insectID, String groupName, int sensitivityScore) {
        this.insectID = insectID;
        this.groupName = groupName;
        this.sensitivityScore = sensitivityScore;
    }

    public Integer getInsectID() {
        return insectID;
    }

    public void setInsectID(Integer insectID) {
        this.insectID = insectID;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public int getSensitivityScore() {
        return sensitivityScore;
    }

    public void setSensitivityScore(int sensitivityScore) {
        this.sensitivityScore = sensitivityScore;
    }

    public List<Evaluationinsect> getEvaluationinsectList() {
        return evaluationinsectList;
    }

    public void setEvaluationinsectList(List<Evaluationinsect> evaluationinsectList) {
        this.evaluationinsectList = evaluationinsectList;
    }

    public List<Insectimage> getInsectimageList() {
        return insectimageList;
    }

    public void setInsectimageList(List<Insectimage> insectimageList) {
        this.insectimageList = insectimageList;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (insectID != null ? insectID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Insect)) {
            return false;
        }
        Insect other = (Insect) object;
        if ((this.insectID == null && other.insectID != null) || (this.insectID != null && !this.insectID.equals(other.insectID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.Insect[ insectID=" + insectID + " ]";
    }
    
}
