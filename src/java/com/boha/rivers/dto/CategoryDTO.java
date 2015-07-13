/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.Category;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author CodeTribe1
 */
public class CategoryDTO implements Serializable {
    private static final long serialVersionUID = 1L;
    private Integer categoryId;
    private String categoryName;
    private List<EvaluationSiteDTO> evaluationsiteList = new ArrayList<>();
    private List<ConditionsDTO> conditionsList = new ArrayList<>();

    public CategoryDTO() {
    }

    public CategoryDTO(Category c) {
        categoryId = c.getCategoryId();
        categoryName = c.getCategoryName();
        
    }

    public Integer getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Integer categoryId) {
        this.categoryId = categoryId;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public List<EvaluationSiteDTO> getEvaluationsiteList() {
        return evaluationsiteList;
    }

    public void setEvaluationsiteList(List<EvaluationSiteDTO> evaluationsiteList) {
        this.evaluationsiteList = evaluationsiteList;
    }

    public List<ConditionsDTO> getConditionsList() {
        return conditionsList;
    }

    public void setConditionsList(List<ConditionsDTO> conditionsList) {
        this.conditionsList = conditionsList;
    }
    
    
    
    
}
