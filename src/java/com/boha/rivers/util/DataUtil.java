package com.boha.rivers.util;

import com.boha.rivers.data.Category;
import com.boha.rivers.data.Comment;
import com.boha.rivers.data.Conditions;
import com.boha.rivers.data.Country;
import com.boha.rivers.data.Evaluation;
import com.boha.rivers.data.Evaluationimage;
import com.boha.rivers.data.Evaluationinsect;
import com.boha.rivers.data.Evaluationsite;
import com.boha.rivers.data.Gcmdevice;
import com.boha.rivers.data.Insect;
import com.boha.rivers.data.Insectimage;
import com.boha.rivers.data.Organisationtype;
import com.boha.rivers.data.River;
import com.boha.rivers.data.Stream;
import com.boha.rivers.data.Team;
import com.boha.rivers.data.Teammember;
import com.boha.rivers.data.Tmember;
import com.boha.rivers.dto.CategoryDTO;
import com.boha.rivers.dto.CommentDTO;
import com.boha.rivers.dto.ConditionsDTO;
import com.boha.rivers.dto.CountryDTO;
import com.boha.rivers.dto.EvaluationDTO;
import com.boha.rivers.dto.EvaluationImageDTO;
import com.boha.rivers.dto.EvaluationInsectDTO;
import com.boha.rivers.dto.EvaluationSiteDTO;
import com.boha.rivers.dto.GcmdeviceDTO;
import com.boha.rivers.dto.InsectDTO;
import com.boha.rivers.dto.InsectImageDTO;
import com.boha.rivers.dto.OrganisationtypeDTO;
import com.boha.rivers.dto.RiverDTO;
import com.boha.rivers.dto.StreamDTO;
import com.boha.rivers.dto.TeamDTO;
import com.boha.rivers.dto.TeamMemberDTO;
import com.boha.rivers.dto.TmemberDTO;
import com.boha.rivers.transfer.ResponseDTO;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.Stateless;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import org.joda.time.DateTime;

/**
 *
 * @author CodeTribe1
 */
@Stateless
@TransactionManagement(TransactionManagementType.CONTAINER)
public class DataUtil {

    @PersistenceContext
    EntityManager em;

    public EntityManager getEm() {
        return em;
    }

    public ResponseDTO loginTeamMember(String email,
            String pin, GcmdeviceDTO gcmdevice, ListUtil listUtil) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Query q = em.createNamedQuery("Teammember.signin", Teammember.class);
            q.setParameter("email", email);
            q.setParameter("pin", pin);
            q.setMaxResults(1);
            Teammember cs = (Teammember) q.getSingleResult();

            addDevice(gcmdevice, cs.getTeamMemberID());

            TeamMemberDTO teamM = new TeamMemberDTO(cs);
            for (Tmember t1 : cs.getTmemberList()) {

                TmemberDTO dTO = new TmemberDTO(t1);
                TeamDTO dTO2 = new TeamDTO(t1.getTeam());
                if (!cs.getTeam().getTeamName().equals(t1.getTeam().getTeamName())) {
                    for (Teammember mt : t1.getTeam().getTeammemberList()) {
                        if (!cs.getEmail().equals(mt.getEmail())) {
                            dTO2.getTeammemberList().add(new TeamMemberDTO(mt));
                        }
                        dTO.setTeam(dTO2);
                        teamM.getTmemberList().add(dTO);
                    }
                }
            }
            TeamDTO team = new TeamDTO(cs.getTeam());
            for (Teammember t : cs.getTeam().getTeammemberList()) {
                if (!t.getEmail().equals(cs.getEmail())) {
                    team.getTeammemberList().add(new TeamMemberDTO(t));
                }
            }
            teamM.setTeam(team);
            teamM.setEvaluationCount(cs.getEvaluationList().size());

            resp.setTeamMember(teamM);

            resp.setOrganisationtypeList(listUtil.getOrganisationtypeList());
            resp.setCountryList(listUtil.getCountryList());
        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed", e);
            throw new DataException("Failed");
        }
        return resp;
    }

    private GcmdeviceDTO addDevice(GcmdeviceDTO gcmdevice, Integer teamMemberID) throws DataException {
        GcmdeviceDTO dto = null;
        try {
            Gcmdevice ct = new Gcmdevice();
            log.log(Level.OFF, null, new Gson().toJson(gcmdevice));
            ct.setAndroidVersion(gcmdevice.getAndroidVersion());
            ct.setDateRegistered(new Date());
            ct.setManufacturer(gcmdevice.getManufacturer());
            ct.setMessageCount(gcmdevice.getMessageCount());
            ct.setModel(gcmdevice.getModel());
            ct.setProduct(gcmdevice.getProduct());
            ct.setRegistrationID(gcmdevice.getRegistrationID());
            ct.setSerialNumber(gcmdevice.getSerialNumber());
            ct.setTeamMember(em.find(Teammember.class, teamMemberID));

            em.persist(ct);
            em.flush();
            //resp.getCountryList().add(new CountryDTO(ct));
            dto = new GcmdeviceDTO(ct);
            log.log(Level.OFF, "Township has been added for: {0} ",
                    new Object[]{ct.getRegistrationID()});

        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed", e);
            throw new DataException("Failed");
        }
        return dto;
    }

    // newly created method to invited member
    public ResponseDTO inviteMembers(TmemberDTO tmember) throws DataException {
        TmemberDTO dto = null;
        ResponseDTO resp = new ResponseDTO();
        try {
            Query q = em.createNamedQuery("Tmember.findInvite", Tmember.class);
            q.setParameter("teamMemberID", tmember.getTeamMemberID());
            q.setParameter("teamID", tmember.getTeamID());

            try {
                Tmember tInvite = (Tmember) q.getSingleResult();

                tInvite.setAcceptInvite(tmember.getAcceptInvite());
                tInvite.setDateCreated(new Date());
                em.merge(tInvite);
                em.flush();
                dto = new TmemberDTO(tInvite);
                resp.settMember(dto);
            } catch (NoResultException e) {
                Tmember t = new Tmember();
                t.setTeam(em.find(Team.class, tmember.getTeamID()));
                t.setTeamMember(em.find(Teammember.class, tmember.getTeamMemberID()));
                t.setDateCreated(new Date());
                t.setAcceptInvite(tmember.getAcceptInvite());

                em.persist(t);
                em.flush();

                dto = new TmemberDTO(t);
                resp.settMember(dto);
                log.log(Level.OFF, "Team Member has been registered for: {0} ",
                        new Object[]{t.getTmemberID()});
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed", e);
            throw new DataException("Failed");
        }
        return resp;
    }

    //method to declined the invite
    public void declineMember(TmemberDTO tmember) {
        Tmember t = new Tmember();
        t.setTeam(em.find(Team.class, tmember.getTeamID()));
        t.setTeamMember(em.find(Teammember.class, tmember.getTeamMemberID()));
        t.setDateCreated(new Date());
        t.setAcceptInvite(tmember.getAcceptInvite());

        em.persist(t);
        em.flush();

        log.log(Level.OFF, "Team Member has been registered for: {0} ",
                new Object[]{t.getTmemberID()});
    }

    public ResponseDTO addTeam(TeamDTO team, Integer teamMemberID) {
        ResponseDTO resp = new ResponseDTO();
        try {
            Team t = new Team();
            t.setTeamImage(team.getTeamImage());
            t.setTeamName(team.getTeamName());
            t.setDateRegistered(new Date());
            t.setCountry(em.find(Country.class, team.getCountryID()));
            t.setOrganisationType(em.find(Organisationtype.class, team.getOrganisationTypeID()));

            em.persist(t);
            em.flush();

            Teammember t1 = em.find(Teammember.class, teamMemberID);
            t1.setTeam(em.find(Team.class, t.getTeamID()));

            em.merge(t1);
            em.flush();
            TeamDTO dTO = new TeamDTO(t);

            resp.setTeam(dTO);
        } catch (Exception e) {

        }
        return resp;
    }

    public ResponseDTO registerTeamMember(TeamMemberDTO member, ListUtil listUtil) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            log.log(Level.OFF, "Something: {0}", new Gson().toJson(member));
            Teammember tm = new Teammember();
            tm.setTeam(em.find(Team.class, member.getTeamID()));

            tm.setFirstName(member.getFirstName());
            tm.setLastName(member.getLastName());
            tm.setEmail(member.getEmail());
            tm.setCellphone(member.getCellphone());
            tm.setActiveFlag(member.getActiveFlag());
            tm.setDateRegistered(new Date());
            tm.setPin(member.getPin());

            em.persist(tm);
            em.flush();

            Query q = em.createNamedQuery("Teammember.findByTeamMemberID", Teammember.class);
            q.setParameter("teamMemberID", member.getTeamMemberID());
            q.setMaxResults(1);
            Teammember cs = (Teammember) q.getSingleResult();

            TeamMemberDTO teamM = new TeamMemberDTO(cs);
            for (Tmember t1 : cs.getTmemberList()) {

                TmemberDTO dTO = new TmemberDTO(t1);
                TeamDTO dTO2 = new TeamDTO(t1.getTeam());
                if (!cs.getTeam().getTeamName().equals(t1.getTeam().getTeamName())) {
                    for (Teammember mt : t1.getTeam().getTeammemberList()) {
                        if (!cs.getEmail().equals(mt.getEmail())) {
                            dTO2.getTeammemberList().add(new TeamMemberDTO(mt));
                        }
                        dTO.setTeam(dTO2);
                        teamM.getTmemberList().add(dTO);
                    }
                }
            }
            TeamDTO team = new TeamDTO(cs.getTeam());
            for (Teammember t : cs.getTeam().getTeammemberList()) {
                if (!t.getEmail().equals(cs.getEmail())) {
                    team.getTeammemberList().add(new TeamMemberDTO(t));
                }
            }
            teamM.setTeam(team);
            teamM.setEvaluationCount(cs.getEvaluationList().size());
            resp.setTeamMember(teamM);
            resp.setOrganisationtypeList(listUtil.getOrganisationtypeList());
            resp.setCountryList(listUtil.getCountryList());

            log.log(Level.OFF, "Team Member has been registered for: {0} ",
                    new Object[]{tm.getFirstName()});

        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed", e);
            throw new DataException("Failed");
        }
        return resp;
    }

    public TeamDTO getMyTeamWithMembers(Integer teamID, String email) {
        TeamDTO dto = new TeamDTO();
        Query q = em.createNamedQuery("Tmember.findByTeamID", Tmember.class);
        q.setParameter("teamID", teamID);

        List<Tmember> list = q.getResultList();
        for (Tmember t : list) {
            if (!email.equals(t.getTeamMember().getEmail())) {
                dto.getTeammemberList().add(new TeamMemberDTO(t.getTeamMember()));
            }
        }

        return dto;
    }

    /*EmailUtil.sendMail(tm.getEmail(), "Minisass Registration", "Hi, " + tm.getFirstName()
     + "\n You've Succesfully Registered on Minisass Under Team " + tm.getTeam().getTeamName()
     + ", Here are your Siging in details:\n"
     + "email : " + tm.getEmail() + "\nPassword : " + tm.getPin()
     + ".\n Thank you and Enjoy....", new CASessionBean());*/
    public ResponseDTO addCountry(CountryDTO country) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Country ct = new Country();
            ct.setCountryID(country.getCountryID());
            ct.setCountryName(country.getCountryName());
            ct.setLatitude(country.getLatitude());
            ct.setLongitude(country.getLongitude());

            em.persist(ct);
            em.flush();
            //resp.getCountryList().add(new CountryDTO(ct));

            log.log(Level.OFF, "Township has been added for: {0} ",
                    new Object[]{ct.getCountryName()});

        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed", e);
            throw new DataException("Failed");
        }
        return resp;
    }

    public ResponseDTO addRiver(RiverDTO riv) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            River ri = new River();
            ri.setRiverName(riv.getRiverName());

            em.persist(ri);
            em.flush();
            resp.getRiverList().add(new RiverDTO(ri));
            log.log(Level.OFF, " River has been successfully added: {0}", ri.getRiverName());

        } catch (PersistenceException e) {
            log.log(Level.SEVERE, "Failed to add river", e);
            resp.setStatusCode(301);
            resp.setMessage("Duplicate detected, request ignored./nPlease try again");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed to add river", e);
            throw new DataException("Failed\n");
        }
        return resp;
    }

    public ResponseDTO addStream(StreamDTO stream) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Stream s = new Stream();
            s.setRiver(em.find(River.class, stream.getRiverID()));

            s.setStreamName(stream.getStreamName());
            s.setLatitude(stream.getLatitude());
            s.setLongitude(stream.getLongitude());

            em.persist(s);
            em.flush();
            resp.getStreamList().add(new StreamDTO(s));
            log.log(Level.OFF, " Stream has been successfully added: {0}", s.getStreamName());

        } catch (PersistenceException e) {
            log.log(Level.SEVERE, "Failed to add Stream", e);
            resp.setStatusCode(301);
            resp.setMessage("Duplicate detected, request ignored./nPlease try again");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed to add stream", e);
            throw new DataException("Failed\n");
        }
        return resp;
    }

    public ResponseDTO addOrganisationType(OrganisationtypeDTO type) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Organisationtype org = new Organisationtype();
            org.setOrganisationName(type.getOrganisationName());

            em.persist(org);
            em.flush();
            resp(new OrganisationtypeDTO(org));
            log.log(Level.OFF, " Organisation has been successfully added: {0}", org.getOrganisationName());

        } catch (PersistenceException e) {
            log.log(Level.SEVERE, "Failed to add river", e);
            resp.setStatusCode(301);
            resp.setMessage("Duplicate detected, request ignored./nPlease try again");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed to add river", e);
            throw new DataException("Failed\n");
        }
        return resp;
    }
//TODO Edited method

    public ResponseDTO registerTeam(TeamDTO team) throws DataException {
        ResponseDTO resp = new ResponseDTO();

        try {
            log.log(Level.INFO, new Gson().toJson(team));
            Team t = new Team();
            t.setTeamImage(team.getTeamImage());
            t.setTeamName(team.getTeamName());
            t.setDateRegistered(new Date());
            t.setCountry(em.find(Country.class, team.getCountryID()));
            t.setOrganisationType(em.find(Organisationtype.class, team.getOrganisationTypeID()));

            em.persist(t);
            em.flush();

            TeamDTO teamDTO = new TeamDTO(t);

            if (team.getTeammemberList() != null) {

                for (TeamMemberDTO tms : team.getTeammemberList()) {
                    Teammember tm = new Teammember();
                    tm.setTeam(em.find(Team.class, t.getTeamID()));
                    tm.setFirstName(tms.getFirstName());
                    tm.setLastName(tms.getLastName());
                    tm.setEmail(tms.getEmail());
                    tm.setCellphone(tms.getCellphone());
                    tm.setActiveFlag(tms.getActiveFlag());
                    tm.setDateRegistered(new Date());
                    tm.setPin(tms.getPin());

                    em.persist(tm);
                    em.flush();

                    teamDTO.getTeammemberList().add(new TeamMemberDTO(tm));

                    log.log(Level.OFF, "Team Membber has been registered for: {0} ",
                            new Object[]{tms.getFirstName()});
                }
            }

            if (team.getTmemberList() != null) {
                for (TmemberDTO tms : team.getTmemberList()) {
                    Teammember tmInvite = new Teammember();

                    tmInvite.setFirstName(tms.getTeamMember().getFirstName());
                    tmInvite.setLastName(tms.getTeamMember().getLastName());
                    tmInvite.setEmail(tms.getTeamMember().getEmail());
                    tmInvite.setCellphone(tms.getTeamMember().getCellphone());
                    tmInvite.setActiveFlag(tms.getTeamMember().getActiveFlag());
                    tmInvite.setDateRegistered(new Date());
                    tmInvite.setPin(tms.getTeamMember().getPin());

                    em.persist(tmInvite);
                    em.flush();

                    TmemberDTO tmember = new TmemberDTO();
                    tmember.setAcceptInvite(1);
                    tmember.setTeamID(t.getTeamID());
                    tmember.setTeamMemberID(tmInvite.getTeamMemberID());
                    teamDTO.getTmemberList().add(inviteMembers(tmember).gettMember());
                    /*EmailUtil.sendMail(tms.getEmail(), "Minisass Registration", "Hi, " + tms.getFirstName()
                     + "\n You've Succesfully Registered on Minisass Under Team " + team.getTeamName()
                     + ", Here are your Siging in details:\n"
                     + "email : " + tms.getEmail() + "\nPassword : " + tm.getPin()
                     + ".\n Thank you and Enjoy....", new CASessionBean());*/

                    log.log(Level.OFF, "Team Membber has been registered for: {0} ",
                            new Object[]{tmInvite.getFirstName()});
                }
            }
            resp.getTeamList().add(teamDTO);

            log.log(Level.OFF, "Team has been registered for: {0} ",
                    new Object[]{team.getTeamName()});

        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed", e);
            throw new DataException("Failed");
        }
        return resp;
    }

    public ResponseDTO addEvaluationSite(EvaluationSiteDTO site) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {

            Evaluationsite ts = new Evaluationsite();
            ts.setRiver(em.find(River.class, site.getRiverID()));
            ts.setCategory(em.find(Category.class, site.getCategoryID()));

            ts.setDateRegistered(new Date());
            ts.setLatitude(site.getLatitude());
            ts.setLongitude(site.getLongitude());

            em.persist(ts);
            em.flush();

            resp.getEvaluationSiteList().add(new EvaluationSiteDTO(ts));

            log.log(Level.OFF, "Evaluation site has been registered for: {0} ",
                    new Object[]{ts.getDateRegistered()});

        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed", e);
            throw new DataException("Failed");
        }
        return resp;
    }

    public ResponseDTO addInsect(InsectDTO insect) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Insect i = new Insect();
            i.setGroupName(insect.getGroupName());
            i.setSensitivityScore(insect.getSensitivityScore());

            em.persist(i);
            em.flush();

            resp.getInsectList().add(new InsectDTO(i));

            log.log(Level.OFF, "province has been added for: {0} ",
                    new Object[]{i.getGroupName()});

        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed", e);
            throw new DataException("Failed");
        }
        return resp;
    }

    public ResponseDTO addEvaluationInsect(EvaluationInsectDTO evi) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {

            Evaluationinsect ei = new Evaluationinsect();

            ei.setEvaluation(em.find(Evaluation.class, evi.getEvaluationID()));
            ei.setInsect(em.find(Insect.class, evi.getInsectID()));

            ei.setRemarks(evi.getRemarks());
            em.persist(ei);
            em.flush();

            resp.getEvaluationInsectList().add(new EvaluationInsectDTO(ei));

            log.log(Level.OFF, "evaluation insect hass been successfully  added",
                    new Object[]{ei.getRemarks()});

        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed to get evaluation insect", e);
            throw new DataException("Failed");
        }
        return resp;
    }

    public ResponseDTO updateComment(CommentDTO com) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Comment c = em.find(Comment.class, com.getCommentID());

            if (com.getRemarks() != null) {
                c.setRemarks(com.getRemarks());

                em.merge(c);
                log.log(Level.INFO, "Comment updated");
            }
        } catch (Exception e) {
            log.log(Level.OFF, null, e);
            throw new DataException("Failed to update comment\n" + getErrorString(e));
        }

        return resp;
    }

    public ResponseDTO updateTeam(TeamDTO tea) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Team t = em.find(Team.class, tea.getTeamID());

            if (tea.getTeamName() != null) {
                t.setTeamName(tea.getTeamName());

                em.merge(t);
                log.log(Level.INFO, "Team updated");
            }
        } catch (Exception e) {
            log.log(Level.OFF, null, e);
            throw new DataException("Failed to update Team\n" + getErrorString(e));
        }

        return resp;
    }

    public ResponseDTO updateEvaluation(EvaluationDTO ev) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Evaluation e = em.find(Evaluation.class, ev.getEvaluationID());
            e.setOxygen(ev.getOxygen());
            e.setPH(ev.getpH());
            e.setScore(ev.getScore());
            e.setWaterClarity(ev.getWaterClarity());
            e.setWaterTemperature(ev.getWaterTemperature());
            // e.setRemarks(ev.getRemarks());

            em.merge(e);
            em.flush();
            resp.setEvaluation(new EvaluationDTO(e));
            log.log(Level.INFO, "Evaluation updated");

        } catch (Exception e) {
            log.log(Level.OFF, null, e);
            throw new DataException("Failed to update Evaluation\n" + getErrorString(e));
        }

        return resp;
    }

    public ResponseDTO updateEvaluationImage(EvaluationImageDTO evi) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Evaluationimage ei = em.find(Evaluationimage.class, evi.getEvaluationImageID());

            if (evi.getFileName() != null) {
                ei.setFileName(evi.getFileName());

                em.merge(ei);
                em.flush();

                log.log(Level.INFO, "Evaluation image updated");
            }
        } catch (Exception e) {
            log.log(Level.OFF, null, e);
            throw new DataException("Failed to update evaluation image\n" + getErrorString(e));
        }

        return resp;
    }

    public ResponseDTO updateConditions(ConditionsDTO con) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Conditions c = em.find(Conditions.class, con.getConditionsID());

            if (con.getConditionName() != null) {
                c.setConditionName(con.getConditionName());

                em.merge(c);
                log.log(Level.INFO, "Condition updated");
            }
        } catch (Exception e) {
            log.log(Level.OFF, null, e);
            throw new DataException("Failed to update condition\n" + getErrorString(e));
        }

        return resp;
    }

    public ResponseDTO updateTeamMember(TeamMemberDTO tem) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Teammember tm = em.find(Teammember.class, tem.getTeamMemberID());

            tm.setCellphone(tem.getCellphone());

            tm.setFirstName(tem.getFirstName());

            tm.setLastName(tem.getLastName());

            tm.setTeamMemberImage(tem.getTeamMemberImage());

            em.merge(tm);
            em.flush();
            TeamMemberDTO teamM = new TeamMemberDTO(tm);
            for (Tmember t1 : tm.getTmemberList()) {

                TmemberDTO dTO = new TmemberDTO(t1);
                TeamDTO dTO2 = new TeamDTO(t1.getTeam());
                if (!tm.getTeam().getTeamName().equals(t1.getTeam().getTeamName())) {
                    for (Teammember mt : t1.getTeam().getTeammemberList()) {
                        if (!tm.getEmail().equals(mt.getEmail())) {
                            dTO2.getTeammemberList().add(new TeamMemberDTO(mt));
                        }
                        dTO.setTeam(dTO2);
                        teamM.getTmemberList().add(dTO);
                    }
                }
            }
            TeamDTO team = new TeamDTO(tm.getTeam());
            for (Teammember t : tm.getTeam().getTeammemberList()) {
                if (!t.getEmail().equals(tm.getEmail())) {
                    team.getTeammemberList().add(new TeamMemberDTO(t));
                }
            }
            teamM.setTeam(team);
            teamM.setEvaluationCount(tm.getEvaluationList().size());

            resp.setTeamMember(teamM);

            log.log(Level.INFO, "Team member updated");

        } catch (Exception e) {
            log.log(Level.OFF, null, e.getLocalizedMessage());
            throw new DataException("Failed to update team member\n" + getErrorString(e));
        }

        return resp;
    }

    public ResponseDTO addCategory(CategoryDTO category) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Category c = new Category();
            c.setCategoryId(category.getCategoryId());
            c.setCategoryName(category.getCategoryName());

            em.persist(c);
            em.flush();

            resp.getCategoryList().add(new CategoryDTO(c));

            log.log(Level.OFF, "category has been added for: {0} ",
                    new Object[]{c.getCategoryName()});

        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed", e);
            throw new DataException("Failed");
        }
        return resp;
    }

    public ResponseDTO addComment(CommentDTO comment) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Comment cm = new Comment();
            cm.setRemarks(comment.getRemarks());

            em.persist(cm);
            em.flush();

            resp.getCommentList().add(new CommentDTO(cm));

            log.log(Level.OFF, "comment has been added for: {0} ",
                    new Object[]{cm.getCommentID()});

        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed", e);
            throw new DataException("Failed");
        }
        return resp;
    }

    //adding stream
    public ResponseDTO addEvaluation(EvaluationDTO evaluation, List<InsectImageDTO> insectImages, CloudMsgUtil cloudMsgUtil, PlatformUtil platformUtil) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {

            Evaluationsite ev = new Evaluationsite();
            ev.setAccuracy(evaluation.getEvaluationSite().getAccuracy());
            ev.setLongitude(evaluation.getEvaluationSite().getLongitude());
            ev.setLatitude(evaluation.getEvaluationSite().getLatitude());
            ev.setDateRegistered(new Date());

            if (evaluation.getEvaluationSite().getStream() != null) {
                Stream newStream = new Stream();
                newStream.setLongitude(evaluation.getEvaluationSite().getLongitude());
                newStream.setLatitude(evaluation.getEvaluationSite().getLatitude());
                newStream.setStreamName(evaluation.getEvaluationSite().getStream().getStreamName());
                newStream.setRiver(em.find(River.class, evaluation.getEvaluationSite().getRiverID()));
                em.persist(newStream);
                em.flush();
            }

            ev.setRiver(em.find(River.class, evaluation.getEvaluationSite().getRiverID()));
            ev.setCategory(em.find(Category.class, evaluation.getEvaluationSite().getCategoryID()));
            em.persist(ev);
            em.flush();

            Evaluation e = new Evaluation();

            e.setTeamMember(em.find(Teammember.class, evaluation.getTeamMemberID()));
            e.setEvaluationSite(em.find(Evaluationsite.class, ev.getEvaluationSiteID()));
            log.log(Level.WARNING, "Test{0}", 1);
            e.setConditions(em.find(Conditions.class, evaluation.getConditionsID()));
            log.log(Level.WARNING, "Test{0}", 2);
            e.setOxygen(evaluation.getOxygen());
            log.log(Level.WARNING, "Test{0}", 3);
            e.setPH(evaluation.getpH());
            log.log(Level.WARNING, "Test{0}", 4);
            e.setRemarks(evaluation.getRemarks());
            log.log(Level.WARNING, "Test{0}", 5);
            e.setScore(evaluation.getScore());
            log.log(Level.WARNING, "Test{0}", 5);
            e.setWaterClarity(evaluation.getWaterClarity());
            log.log(Level.WARNING, "Test{0}", 5);
            e.setWaterTemperature(evaluation.getWaterTemperature());

            e.setEvaluationDate(new Date(evaluation.getEvaluationDate()));
            em.persist(e);
            em.flush();

            for (InsectImageDTO in : insectImages) {
                Evaluationinsect ei = new Evaluationinsect();
                ei.setEvaluation(em.find(Evaluation.class, e.getEvaluationID()));
                ei.setInsect(em.find(Insect.class, in.getInsectID()));
                ei.setEvaluationColor(Integer.MAX_VALUE);
                ei.setEvaluationFlag(0);
                ei.setRemarks(evaluation.getRemarks());
                em.persist(ei);
                em.flush();
            }

            for (EvaluationImageDTO ei : evaluation.getEvaluationimageList()) {
                Evaluationimage e1 = new Evaluationimage();
                e1.setAccuracy(ei.getAccuracy());
                e1.setDateTaken(e1.getDateTaken());
                e1.setEvaluation(em.find(Evaluation.class, e.getEvaluationID()));
                e1.setFileName(e1.getFileName());
                e1.setLatitude(e1.getLatitude());
                e1.setLongitude(e1.getLongitude());
                em.persist(e1);
                em.flush();
            }
            resp.getEvaluationList().add(new EvaluationDTO(e));

            log.log(Level.OFF, "evaluation has been added for: {0} ",
                    new Object[]{e.getEvaluationDate()});
            Query q = em.createNamedQuery("Gcmdevice.findTeamMember", Gcmdevice.class);
            q.setParameter("teamMemberID", evaluation.getTeamMemberID());
            List<Gcmdevice> list = q.getResultList();
            List<String> registrationIDs = new ArrayList<>();
            for (Gcmdevice g : list) {
                registrationIDs.add(g.getRegistrationID());
            }
            int OK = cloudMsgUtil.sendMessage(registrationIDs, platformUtil);
            if (OK == 0) {
                resp.setMessage("Observation created and GCM message sent");
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed", e);
            throw new DataException("Failed");
        }
        return resp;
    }

    public ResponseDTO addCondition(ConditionsDTO condition) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Conditions c = new Conditions();

            c.setCategory(em.find(Category.class, condition.getCategoryID()));
            c.setConditionName(condition.getConditionName());
            c.setHigh(condition.getHigh());
            c.setLow(condition.getLow());

            em.persist(c);
            em.flush();

            resp.getConditionsList().add(new ConditionsDTO(c));

            log.log(Level.OFF, "condition has been added for: {0} ",
                    new Object[]{c.getConditionName()});

        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed", e);
            throw new DataException("Failed");
        }
        return resp;
    }

    public ResponseDTO addInsertImage(InsectImageDTO image) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Insectimage i = new Insectimage();
            i.setInsect(em.find(Insect.class, image.getInsectID()));

            i.setDateRegistered(new Date());
            i.setUri(image.getUri());

            em.persist(i);
            em.flush();
            resp.getInsectList().add(new InsectDTO());

            log.log(Level.OFF, "Township has been added for: {0} ",
                    new Object[]{i.getUri()});

        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed", e);
            throw new DataException("Failed");
        }
        return resp;
    }

    public ResponseDTO updateStream(StreamDTO us) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Stream s = new Stream();
            River r = em.find(River.class, us.getRiverID());

            s.setStreamName(us.getStreamName());
            s.setLatitude(us.getLatitude());
            s.setLongitude(us.getLongitude());

            em.merge(s);
            em.flush();

            log.log(Level.INFO, "Stream updated");

        } catch (Exception e) {
            log.log(Level.OFF, null, e);
            throw new DataException("Failed to update Stream\n" + getErrorString(e));
        }
        return resp;
    }

    public void updateRiver(RiverDTO dto) throws DataException {
        try {
            River r = em.find(River.class, dto.getRiverID());
            r.setRiverName(dto.getRiverName());

            em.merge(r);
            log.log(Level.INFO, "River updated");

        } catch (Exception e) {
            log.log(Level.OFF, null, e);
            throw new DataException("Failed to update river\n" + getErrorString(e));
        }

    }

    public void updateEvaluationSite(EvaluationSiteDTO dto) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Evaluationsite ev = em.find(Evaluationsite.class, dto.getEvaluationSiteID());
            if (dto.getLatitude() > 0) {
                ev.setLatitude(dto.getLatitude());
            }
            if (dto.getLatitude() > 0) {
                ev.setLongitude(dto.getLongitude());
            }
            em.merge(ev);
            log.log(Level.INFO, "Evaluation site updated");

        } catch (Exception e) {
            log.log(Level.OFF, null, e);
            throw new DataException("Failed to update Evaluation site\n" + getErrorString(e));
        }

    }

    public ResponseDTO updateInsert(InsectDTO dto) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Insect i = em.find(Insect.class, dto.getInsectID());
            i.setGroupName(dto.getGroupName());
            em.merge(i);
            log.log(Level.INFO, "Insect updated");

        } catch (Exception e) {
            log.log(Level.OFF, null, e);
            throw new DataException("Failed to update Insect\n" + getErrorString(e));
        }

        return resp;
    }

    public ResponseDTO importMembers(int teamID, List<TeamMemberDTO> members) {
        ResponseDTO resp = new ResponseDTO();
        List<TeamMemberDTO> list = new ArrayList<>();

        for (TeamMemberDTO t : members) {
            Teammember team = em.find(Teammember.class, t.getTeamMemberID());
            team.setTeam(em.find(Team.class, teamID));
            em.merge(team);
            em.flush();
            list.add(new TeamMemberDTO(team));
        }
        resp.setTeamMemberList(list);
        return resp;
    }

    public ResponseDTO updateInsertImage(InsectImageDTO dto) throws DataException {
        ResponseDTO resp = new ResponseDTO();
        try {
            Insectimage ii = em.find(Insectimage.class, dto.getInsectImageID());
            ii.setDateRegistered(new Date());
            ii.setInsect(em.find(Insect.class, dto.getInsectID()));
            ii.setUri(dto.getUri());

            em.merge(ii);
            log.log(Level.INFO, "Insert Image updated");

        } catch (Exception e) {
            log.log(Level.OFF, null, e);
            throw new DataException("Failed to update Insect Image\n" + getErrorString(e));
        }

        return resp;
    }

    public ResponseDTO getServerErrors(
            long startDate, long endDate) throws DataException {
        ResponseDTO r = new ResponseDTO();
        if (startDate == 0) {
            DateTime ed = new DateTime();
            DateTime sd = ed.minusMonths(3);
            startDate = sd.getMillis();
            endDate = ed.getMillis();
        }
        return r;
    }

    public String getErrorString(Exception e) {
        StringBuilder sb = new StringBuilder();
        if (e.getMessage() != null) {
            sb.append(e.getMessage()).append("\n\n");
        }
        if (e.toString() != null) {
            sb.append(e.toString()).append("\n\n");
        }
        StackTraceElement[] s = e.getStackTrace();
        if (s.length > 0) {
            StackTraceElement ss = s[0];
            String method = ss.getMethodName();
            String cls = ss.getClassName();
            int line = ss.getLineNumber();
            sb.append("Class: ").append(cls).append("\n");
            sb.append("Method: ").append(method).append("\n");
            sb.append("Line Number: ").append(line).append("\n");
        }

        return sb.toString();
    }

    public void confirmLocation(Integer EvaluationSiteID, double latitude, double longitude, Float accuracy) throws DataException {
        try {
            Evaluationsite ps = em.find(Evaluationsite.class, EvaluationSiteID);
            if (ps != null) {
                // ps.setLocationConfirmed(1);
                ps.setLatitude(latitude);
                ps.setLongitude(longitude);
                // ps.setAccuracy(accuracy);
                em.merge(ps);
                log.log(Level.INFO, "Evaluation Site location confirmed");
            }
        } catch (Exception e) {
            log.log(Level.OFF, null, e);
            throw new DataException("Failed to confirm location\n" + getErrorString(e));
        }
    }
    static final Logger log = Logger.getLogger(DataUtil.class.getSimpleName());

    public void addEvaluationImage(EvaluationImageDTO dto) {
        log.log(Level.OFF, "------ adding evaluation image, message: {0} origin: {1}", new Object[]{dto.getFileName(), dto.getEvaluationID()});
        try {
            Evaluationimage t = new Evaluationimage();
            t.setDateTaken(new Date());
            t.setEvaluation(em.find(Evaluation.class, dto.getEvaluationID()));
            t.setFileName(dto.getFileName());

            em.persist(t);
            log.log(Level.INFO, "####### evaluation image row added, origin {0} \nmessage: {1}",
                    new Object[]{dto.getFileName(), dto.getEvaluationID()});
        } catch (Exception e) {
            log.log(Level.SEVERE, "####### Failed to add evaluation image from " + dto.getFileName() + "\n" + dto.getEvaluationID(), e);
        }
    }

    public void updateTeamImage(Integer teamID, String uri) {
        try {
            Team t = em.find(Team.class, teamID);
            t.setTeamImage(uri);
            em.merge(t);
            log.log(Level.INFO, "Team row added");
        } catch (Exception e) {
            log.log(Level.SEVERE, "####### Failed to add team image from \n {0}", e);

        }
    }

    public void updateTeamMemberImage(Integer teamMemberID, String uri) {
        try {
            Teammember t = em.find(Teammember.class, teamMemberID);
            t.setTeamMemberImage(uri);
            em.merge(t);
            log.log(Level.INFO, "Team member row added");
        } catch (Exception e) {
            log.log(Level.SEVERE, "####### Failed to add team member image from \n {0}", e);

        }
    }

    private String getRandomPin() {
        StringBuilder sb = new StringBuilder();
        Random rand = new Random(System.currentTimeMillis());
        int x = rand.nextInt(9);
        if (x == 0) {
            x = 3;
        }
        sb.append(x);
        sb.append(rand.nextInt(9));
        sb.append(rand.nextInt(9));
        sb.append(rand.nextInt(9));
        sb.append(rand.nextInt(9));
        sb.append(rand.nextInt(9));
        return sb.toString();
    }

    private void resp(OrganisationtypeDTO organisationtypeDTO) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
