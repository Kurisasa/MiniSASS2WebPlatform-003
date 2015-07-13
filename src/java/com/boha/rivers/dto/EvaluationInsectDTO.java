/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.Evaluationinsect;
import java.io.Serializable;

/**
 *
 * @author CodeTribe1
 */
public class EvaluationInsectDTO implements Serializable {
    private static final long serialVersionUID = 1L;
    private Integer evaluationInsectID, evaluationID, insectID;
    private int evaluationFlag;
    private Integer evaluationColor;
    private String remarks;
    private EvaluationDTO evaluation;
    private InsectDTO insect;

    public EvaluationInsectDTO() {
    }

    public EvaluationInsectDTO(Evaluationinsect c) {
        evaluationInsectID = c.getEvaluationInsectID();
        evaluationFlag = c.getEvaluationFlag();
        evaluationColor = c.getEvaluationColor();
        remarks = c.getRemarks();
        evaluation = new EvaluationDTO(c.getEvaluation());
        insect = new InsectDTO(c.getInsect());
    }

    public Integer getEvaluationInsectID() {
        return evaluationInsectID;
    }

    public void setEvaluationInsectID(Integer evaluationInsectID) {
        this.evaluationInsectID = evaluationInsectID;
    }

    public Integer getEvaluationID() {
        return evaluationID;
    }

    public void setEvaluationID(Integer evaluationID) {
        this.evaluationID = evaluationID;
    }

    public Integer getInsectID() {
        return insectID;
    }

    public void setInsectID(Integer insectID) {
        this.insectID = insectID;
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

    public EvaluationDTO getEvaluation() {
        return evaluation;
    }

    public void setEvaluation(EvaluationDTO evaluation) {
        this.evaluation = evaluation;
    }

    public InsectDTO getInsect() {
        return insect;
    }

    public void setInsect(InsectDTO insect) {
        this.insect = insect;
    }
    
    
    
}
