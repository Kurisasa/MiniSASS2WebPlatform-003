/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.Comment;
import com.boha.rivers.data.Evaluation;
import com.boha.rivers.data.Evaluationcomment;
import java.io.Serializable;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

/**
 *
 * @author CodeTribe1
 */
public class EvaluationCommentDTO implements Serializable {
    private static final long serialVersionUID = 1L;
    private Integer evaluationCommentID, commentID;
    private EvaluationDTO evaluation;
    private CommentDTO comment;

    public EvaluationCommentDTO() {
    }

    public EvaluationCommentDTO(Evaluationcomment c) {
        evaluationCommentID = c.getEvaluationCommentID();
        comment = new CommentDTO(c.getComment());
        commentID = c.getComment().getCommentID();
    }

    public Integer getEvaluationCommentID() {
        return evaluationCommentID;
    }

    public void setEvaluationCommentID(Integer evaluationCommentID) {
        this.evaluationCommentID = evaluationCommentID;
    }

    public EvaluationDTO getEvaluation() {
        return evaluation;
    }

    public void setEvaluation(EvaluationDTO evaluation) {
        this.evaluation = evaluation;
    }

    public CommentDTO getComment() {
        return comment;
    }

    public void setComment(CommentDTO comment) {
        this.comment = comment;
    }

    public Integer getCommentID() {
        return commentID;
    }

    public void setCommentID(Integer commentID) {
        this.commentID = commentID;
    }
    
}
