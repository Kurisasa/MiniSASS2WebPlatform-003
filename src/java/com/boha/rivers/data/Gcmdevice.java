/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.data;

import java.io.Serializable;
import java.util.Date;
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
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 *
 * @author sifiso
 */
@Entity
@Table(name = "gcmdevice")
@NamedQueries({
    @NamedQuery(name = "Gcmdevice.findAll", query = "SELECT g FROM Gcmdevice g"),
    @NamedQuery(name = "Gcmdevice.findTeamMember", query = "SELECT g FROM Gcmdevice g WHERE g.teamMember.teamMemberID = :teamMemberID"),
    @NamedQuery(name = "Gcmdevice.findByGcmDeviceID", query = "SELECT g FROM Gcmdevice g WHERE g.gcmDeviceID = :gcmDeviceID"),
    @NamedQuery(name = "Gcmdevice.findByManufacturer", query = "SELECT g FROM Gcmdevice g WHERE g.manufacturer = :manufacturer"),
    @NamedQuery(name = "Gcmdevice.findByModel", query = "SELECT g FROM Gcmdevice g WHERE g.model = :model"),
    @NamedQuery(name = "Gcmdevice.findByProduct", query = "SELECT g FROM Gcmdevice g WHERE g.product = :product"),
    @NamedQuery(name = "Gcmdevice.findByMessageCount", query = "SELECT g FROM Gcmdevice g WHERE g.messageCount = :messageCount"),
    @NamedQuery(name = "Gcmdevice.findByDateRegistered", query = "SELECT g FROM Gcmdevice g WHERE g.dateRegistered = :dateRegistered"),
    @NamedQuery(name = "Gcmdevice.findBySerialNumber", query = "SELECT g FROM Gcmdevice g WHERE g.serialNumber = :serialNumber"),
    @NamedQuery(name = "Gcmdevice.findByAndroidVersion", query = "SELECT g FROM Gcmdevice g WHERE g.androidVersion = :androidVersion")})
public class Gcmdevice implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "gcmDeviceID")
    private Integer gcmDeviceID;
    @Basic(optional = false)
    @NotNull
    @Lob
    @Size(min = 1, max = 65535)
    @Column(name = "registrationID")
    private String registrationID;
    @Size(max = 100)
    @Column(name = "manufacturer")
    private String manufacturer;
    @Size(max = 100)
    @Column(name = "model")
    private String model;
    @Size(max = 100)
    @Column(name = "product")
    private String product;
    @Column(name = "messageCount")
    private Integer messageCount;
    @Basic(optional = false)
    @NotNull
    @Column(name = "dateRegistered")
    @Temporal(TemporalType.TIMESTAMP)
    private Date dateRegistered;
    @Size(max = 255)
    @Column(name = "serialNumber")
    private String serialNumber;
    @Size(max = 100)
    @Column(name = "androidVersion")
    private String androidVersion;
    @JoinColumn(name = "teamMemberID", referencedColumnName = "teamMemberID")
    @ManyToOne(optional = false)
    private Teammember teamMember;

    public Gcmdevice() {
    }

    public Gcmdevice(Integer gcmDeviceID) {
        this.gcmDeviceID = gcmDeviceID;
    }

    public Gcmdevice(Integer gcmDeviceID, String registrationID, Date dateRegistered) {
        this.gcmDeviceID = gcmDeviceID;
        this.registrationID = registrationID;
        this.dateRegistered = dateRegistered;
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

    public Date getDateRegistered() {
        return dateRegistered;
    }

    public void setDateRegistered(Date dateRegistered) {
        this.dateRegistered = dateRegistered;
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

    public Teammember getTeamMember() {
        return teamMember;
    }

    public void setTeamMember(Teammember teamMember) {
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
        if (!(object instanceof Gcmdevice)) {
            return false;
        }
        Gcmdevice other = (Gcmdevice) object;
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
