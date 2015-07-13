/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.Comment;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author CodeTribe1
 */
public class CommentDTO implements Serializable {
    private static final long serialVersionUID = 1L;
    private Integer commentID;
    private String remarks;
    private List<EvaluationCommentDTO> evaluationcommentList = new ArrayList<>();

    public CommentDTO() {
    }

    public CommentDTO(Comment c) {
        commentID = c.getCommentID();
        remarks = c.getRemarks();
        
    }

    public Integer getCommentID() {
        return commentID;
    }

    public void setCommentID(Integer commentID) {
        this.commentID = commentID;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    public List<EvaluationCommentDTO> getEvaluationcommentList() {
        return evaluationcommentList;
    }

    public void setEvaluationcommentList(List<EvaluationCommentDTO> evaluationcommentList) {
        this.evaluationcommentList = evaluationcommentList;
    }
    
       
    
    
}
