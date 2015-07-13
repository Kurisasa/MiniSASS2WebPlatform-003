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

/**
 *
 * @author CodeTribe1
 */
@Entity
@Table(name = "evaluationcomment")
@NamedQueries({
    @NamedQuery(name = "Evaluationcomment.findAll", query = "SELECT e FROM Evaluationcomment e"),
    @NamedQuery(name = "Evaluationcomment.findByEvaluationCommentID", query = "SELECT e FROM Evaluationcomment e WHERE e.evaluationCommentID = :evaluationCommentID")})
public class Evaluationcomment implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "evaluationCommentID")
    private Integer evaluationCommentID;
    @JoinColumn(name = "evaluationID", referencedColumnName = "evaluationID")
    @ManyToOne(optional = false)
    private Evaluation evaluation;
    @JoinColumn(name = "commentID", referencedColumnName = "commentID")
    @ManyToOne(optional = false)
    private Comment comment;

    public Evaluationcomment() {
    }

    public Evaluationcomment(Integer evaluationCommentID) {
        this.evaluationCommentID = evaluationCommentID;
    }

    public Integer getEvaluationCommentID() {
        return evaluationCommentID;
    }

    public void setEvaluationCommentID(Integer evaluationCommentID) {
        this.evaluationCommentID = evaluationCommentID;
    }

    public Evaluation getEvaluation() {
        return evaluation;
    }

    public void setEvaluation(Evaluation evaluation) {
        this.evaluation = evaluation;
    }

    public Comment getComment() {
        return comment;
    }

    public void setComment(Comment comment) {
        this.comment = comment;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (evaluationCommentID != null ? evaluationCommentID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Evaluationcomment)) {
            return false;
        }
        Evaluationcomment other = (Evaluationcomment) object;
        if ((this.evaluationCommentID == null && other.evaluationCommentID != null) || (this.evaluationCommentID != null && !this.evaluationCommentID.equals(other.evaluationCommentID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.Evaluationcomment[ evaluationCommentID=" + evaluationCommentID + " ]";
    }
    
}
