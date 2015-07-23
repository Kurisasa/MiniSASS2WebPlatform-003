/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.dto;

import com.boha.rivers.data.*;
import java.io.Serializable;

public class GcmdeviceDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer gcmDeviceID, teamMemberID;
    private String registrationID;
    private String manufacturer;
    private String model;
    private String product;
    private Integer messageCount;
    private long dateRegistered;
    private String serialNumber;
    private String androidVersion;
    private TeamMemberDTO teamMember;

    public GcmdeviceDTO() {
    }

    public GcmdeviceDTO(Gcmdevice g) {
        this.gcmDeviceID = g.getGcmDeviceID();
        this.registrationID = g.getRegistrationID();
        this.dateRegistered = g.getDateRegistered().getTime();
        manufacturer = g.getManufacturer();
        model = g.getModel();
        product = g.getProduct();
        serialNumber = g.getSerialNumber();
        messageCount = g.getMessageCount();
        androidVersion = g.getAndroidVersion();
        teamMemberID = g.getTeamMember().getTeamMemberID();
        teamMember = new TeamMemberDTO(g.getTeamMember());
    }

    public Integer getGcmDeviceID() {
        return gcmDeviceID;
    }

    public void setGcmDeviceID(Integer gcmDeviceID) {
        this.gcmDeviceID = gcmDeviceID;
    }

    public String getRegistrationID() {
        return registrationID;
    }

    public void setRegistrationID(String registrationID) {
        this.registrationID = registrationID;
    }

    public String getManufacturer() {
        return manufacturer;
    }

    public void setManufacturer(String manufacturer) {
        this.manufacturer = manufacturer;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public Integer getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(Integer messageCount) {
        this.messageCount = messageCount;
    }

    public String getSerialNumber() {
        return serialNumber;
    }

    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber;
    }

    public String getAndroidVersion() {
        return androidVersion;
    }

    public void setAndroidVersion(String androidVersion) {
        this.androidVersion = androidVersion;
    }

    public Integer getTeamMemberID() {
        return teamMemberID;
    }

    public void setTeamMemberID(Integer teamMemberID) {
        this.teamMemberID = teamMemberID;
    }

    public long getDateRegistered() {
        return dateRegistered;
    }

    public void setDateRegistered(long dateRegistered) {
        this.dateRegistered = dateRegistered;
    }

    public TeamMemberDTO getTeamMember() {
        return teamMember;
    }

    public void setTeamMember(TeamMemberDTO teamMember) {
        this.teamMember = teamMember;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (gcmDeviceID != null ? gcmDeviceID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof GcmdeviceDTO)) {
            return false;
        }
        GcmdeviceDTO other = (GcmdeviceDTO) object;
        if ((this.gcmDeviceID == null && other.gcmDeviceID != null) || (this.gcmDeviceID != null && !this.gcmDeviceID.equals(other.gcmDeviceID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.boha.rivers.data.Gcmdevice[ gcmDeviceID=" + gcmDeviceID + " ]";
    }

}
