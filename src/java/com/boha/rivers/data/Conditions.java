/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.data;

import java.io.Serializable;
import java.util.List;
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
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 *
 * @author CodeTribe1
 */
@Entity
@Table(name = "conditions")
@NamedQueries({
    @NamedQuery(name = "Conditions.findAll", query = "SELECT c FROM Conditions c"),
    @NamedQuery(name = "Conditions.findByConditionsID", query = "SELECT c FROM Conditions c WHERE c.conditionsID = :conditionsID"),
    @NamedQuery(name = "Conditions.findByConditionName", query = "SELECT c FROM Conditions c WHERE c.conditionName = :conditionName"),
    @NamedQuery(name = "Conditions.findByLow", query = "SELECT c FROM Conditions c WHERE c.low = :low"),
    @NamedQuery(name = "Conditions.findByHigh", query = "SELECT c FROM Conditions c WHERE c.high = :high")})
public class Conditions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "conditionsID")
    private Integer conditionsID;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 255)
    @Column(name = "conditionName")
    private String conditionName;
    @Basic(optional = false)
    @NotNull
    @Column(name = "low")
    private double low;
    @Basic(optional = false)
    @NotNull
    @Column(name = "high")
    private double high;
    @OneToMany(mappedBy = "conditions")
    private List<Evaluation> evaluationList;
    @JoinColumn(name = "categoryID", referencedColumnName = "categoryId")
    @ManyToOne(optional = false)
    private Category category;

    public Conditions() {
    }

    public Conditions(Integer conditionsID) {
        this.conditionsID = conditionsID;
    }

    public Conditions(Integer conditionsID, String conditionName, double low, double high) {
        this.conditionsID = conditionsID;
        this.conditionName = conditionName;
        this.low = low;
        this.high = high;
    }

    public Integer getConditionsID() {
        return conditionsID;
    }

    public void setConditionsID(Integer conditionsID) {
        this.conditionsID = conditionsID;
    }

    public String getConditionName() {
        return conditionName;
    }

    public void setConditionName(String conditionName) {
        this.conditionName = conditionName;
    }

    public double getLow() {
        return low;
    }

    public void setLow(double low) {
        this.low = low;
    }

    public double getHigh() {
        return high;
    }

    public void setHigh(double high) {
        this.high = high;
    }

    public List<Evaluation> getEvaluationList() {
        return evaluationList;
    }

    public void setEvaluationList(List<Evaluation> evaluationList) {
        this.evaluationList = evaluationList;
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
        hash += (conditionsID != null ? conditionsID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Conditions)) {
            return false;
        }
        Conditions other = (Conditions) object;
        if ((this.conditionsID == null && other.conditionsID != null) || (this.conditionsID != null && !this.conditionsID.equals(other.conditionsID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.Conditions[ conditionsID=" + conditionsID + " ]";
    }
    
}
