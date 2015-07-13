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
import javax.persistence.Lob;
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
@Table(name = "evaluationinsect")
@NamedQueries({
    @NamedQuery(name = "Evaluationinsect.findAll", query = "SELECT e FROM Evaluationinsect e"),
    @NamedQuery(name = "Evaluationinsect.findByEvaluationInsectID", query = "SELECT e FROM Evaluationinsect e WHERE e.evaluationInsectID = :evaluationInsectID"),
    @NamedQuery(name = "Evaluationinsect.findByEvaluationFlag", query = "SELECT e FROM Evaluationinsect e WHERE e.evaluationFlag = :evaluationFlag"),
    @NamedQuery(name = "Evaluationinsect.findByEvaluationColor", query = "SELECT e FROM Evaluationinsect e WHERE e.evaluationColor = :evaluationColor")})
public class Evaluationinsect implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "evaluationInsectID")
    private Integer evaluationInsectID;
    @Basic(optional = false)
    @NotNull
    @Column(name = "evaluationFlag")
    private int evaluationFlag;
    @Column(name = "evaluationColor")
    private Integer evaluationColor;
    @Lob
    @Size(max = 65535)
    @Column(name = "remarks")
    private String remarks;
    @JoinColumn(name = "evaluationID", referencedColumnName = "evaluationID")
    @ManyToOne(optional = false)
    private Evaluation evaluation;
    @JoinColumn(name = "insectID", referencedColumnName = "insectID")
    @ManyToOne(optional = false)
    private Insect insect;

    public Evaluationinsect() {
    }

    public Evaluationinsect(Integer evaluationInsectID) {
        this.evaluationInsectID = evaluationInsectID;
    }

    public Evaluationinsect(Integer evaluationInsectID, int evaluationFlag) {
        this.evaluationInsectID = evaluationInsectID;
        this.evaluationFlag = evaluationFlag;
    }

    public Integer getEvaluationInsectID() {
        return evaluationInsectID;
    }

    public void setEvaluationInsectID(Integer evaluationInsectID) {
        this.evaluationInsectID = evaluationInsectID;
    }

    public int getEvaluationFlag() {
        return evaluationFlag;
    }

    public void setEvaluationFlag(int evaluationFlag) {
        this.evaluationFlag = evaluationFlag;
    }

    public Integer getEvaluationColor() {
        return evaluationColor;
    }

    public void setEvaluationColor(Integer evaluationColor) {
        this.evaluationColor = evaluationColor;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    public Evaluation getEvaluation() {
        return evaluation;
    }

    public void setEvaluation(Evaluation evaluation) {
        this.evaluation = evaluation;
    }

    public Insect getInsect() {
        return insect;
    }

    public void setInsect(Insect insect) {
        this.insect = insect;
    }


    @Override
    public int hashCode() {
        int hash = 0;
        hash += (evaluationInsectID != null ? evaluationInsectID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Evaluationinsect)) {
            return false;
        }
        Evaluationinsect other = (Evaluationinsect) object;
        if ((this.evaluationInsectID == null && other.evaluationInsectID != null) || (this.evaluationInsectID != null && !this.evaluationInsectID.equals(other.evaluationInsectID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.Evaluationinsect[ evaluationInsectID=" + evaluationInsectID + " ]";
    }
    
}
