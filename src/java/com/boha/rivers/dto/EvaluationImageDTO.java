/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.Evaluationimage;
import java.io.Serializable;
import java.util.Date;

/**
 *
 * @author CodeTribe1
 */
public class EvaluationImageDTO implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer evaluationImageID, evaluationID;
    private long dateTaken;
    private String fileName;
    private double longitude;
    private double latitude;
    private float accuracy;
    private EvaluationDTO evaluation;

    public EvaluationImageDTO() {
    }

    public EvaluationImageDTO(Evaluationimage c) {
        evaluationImageID = c.getEvaluationImageID();
        dateTaken = c.getDateTaken().getTime();
        fileName = c.getFileName();
        longitude = c.getLongitude();
        latitude = c.getLatitude();
        accuracy = c.getAccuracy();
        evaluationID = c.getEvaluation().getEvaluationID();
        evaluation = new EvaluationDTO(c.getEvaluation());

    }

    public Integer getEvaluationImageID() {
        return evaluationImageID;
    }

    public void setEvaluationImageID(Integer evaluationImageID) {
        this.evaluationImageID = evaluationImageID;
    }

    public Integer getEvaluationID() {
        return evaluationID;
    }

    public void setEvaluationID(Integer evaluationID) {
        this.evaluationID = evaluationID;
    }

    public long getDateTaken() {
        return dateTaken;
    }

    public void setDateTaken(long dateTaken) {
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

    public EvaluationDTO getEvaluation() {
        return evaluation;
    }

    public void setEvaluation(EvaluationDTO evaluation) {
        this.evaluation = evaluation;
    }

}
