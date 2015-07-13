/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.Insect;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author CodeTribe1
 */
public class InsectDTO implements Serializable {
    private static final long serialVersionUID = 1L;
    private Integer insectID;
    private String groupName;
    private int sensitivityScore;
    private List<EvaluationInsectDTO> evaluationinsectList = new ArrayList<>();
    private List<InsectImageDTO> insectimageDTOList = new ArrayList<>();

    public InsectDTO() {
    }

    public InsectDTO(Insect c) {
        insectID = c.getInsectID();
        groupName = c.getGroupName();
        sensitivityScore = c.getSensitivityScore();
        
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

    public List<EvaluationInsectDTO> getEvaluationinsectList() {
        return evaluationinsectList;
    }

    public void setEvaluationinsectList(List<EvaluationInsectDTO> evaluationinsectList) {
        this.evaluationinsectList = evaluationinsectList;
    }

    public List<InsectImageDTO> getInsectimageDTOList() {
        return insectimageDTOList;
    }

    public void setInsectimageDTOList(List<InsectImageDTO> insectimageDTOList) {
        this.insectimageDTOList = insectimageDTOList;
    }

    
    
    
    
}
