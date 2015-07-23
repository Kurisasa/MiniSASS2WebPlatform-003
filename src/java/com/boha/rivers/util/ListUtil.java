package com.boha.rivers.util;

import com.boha.rivers.data.Category;
import com.boha.rivers.data.Comment;
import com.boha.rivers.data.Conditions;
import com.boha.rivers.data.Country;
import com.boha.rivers.data.Evaluation;
import com.boha.rivers.data.Evaluationcomment;
import com.boha.rivers.data.Evaluationimage;
import com.boha.rivers.data.Evaluationinsect;
import com.boha.rivers.data.Evaluationsite;
import com.boha.rivers.data.Insect;
import com.boha.rivers.data.Insectimage;
import com.boha.rivers.data.Insectimagelist;
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
import com.boha.rivers.dto.EvaluationCommentDTO;
import com.boha.rivers.dto.EvaluationDTO;
import com.boha.rivers.dto.EvaluationImageDTO;
import com.boha.rivers.dto.EvaluationInsectDTO;
import com.boha.rivers.dto.EvaluationSiteDTO;
import com.boha.rivers.dto.InsectDTO;
import com.boha.rivers.dto.InsectImageDTO;
import com.boha.rivers.dto.InsectImageListDTO;
import com.boha.rivers.dto.OrganisationtypeDTO;
import com.boha.rivers.dto.RiverDTO;
import com.boha.rivers.dto.StreamDTO;
import com.boha.rivers.dto.TeamDTO;
import com.boha.rivers.dto.TeamMemberDTO;
import com.boha.rivers.dto.TmemberDTO;
import com.boha.rivers.transfer.ResponseDTO;
import static com.boha.rivers.util.DataUtil.log;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

/**
 *
 * @author CodeTribe1
 */
@Stateless
@TransactionManagement(TransactionManagementType.CONTAINER)
public class ListUtil {

    @PersistenceContext
    EntityManager em;

    @EJB
    RiverDataWorker dataWorker;

    public ResponseDTO getStartData(String countryCode) {
        ResponseDTO resp = new ResponseDTO();

        return resp;
    }

    public ResponseDTO getData() {
        try {
            ResponseDTO resp = new ResponseDTO();
            resp.setCategoryList(getCategoryDTOs());
            resp.setConditionsList(getConditionsDTOs());
            resp.setInsectimageDTOList(getInsectImageList());
            resp.setRiverList(getRiverDTOs());
            return resp;
        } catch (Exception ex) {
            Logger.getLogger(ListUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public ResponseDTO getRiverData(double currentLatitude, double currentLongitude, int radius, int type, int flag) {
        try {
            ResponseDTO resp = new ResponseDTO();
            resp.setEvaluationSiteList(getEvaluationSiteDTOs());
            resp.setCategoryList(getCategoryDTOs());
            resp.setCommentList(getCommentDTOs());
            resp.setConditionsList(getConditionsDTOs());
            resp.setInsectimageDTOList(getInsectImageList());
            resp.setRiverList(dataWorker.getRiversWithinRadius(currentLatitude, currentLongitude, radius, type, flag));
            return resp;
        } catch (Exception ex) {
            Logger.getLogger(ListUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    private List<RiverDTO> getRiverDTOs() {
        return getRiverList().getRiverList();
    }

    private List<InsectDTO> getInsectDTOs() {
        List<InsectDTO> result = new ArrayList<>();
        Query q = em.createNamedQuery("Insect.findAll", Insect.class);
        List<Insect> cate = q.getResultList();
        for (Insect c : cate) {
            InsectDTO insect = new InsectDTO(c);
            for (Insectimage ii : c.getInsectimageList()) {
                InsectImageDTO insectImage = new InsectImageDTO(ii);
                insect.getInsectimageDTOList().add(insectImage);
                for (Insectimagelist iil : ii.getInsectimagelistList()) {
                    insectImage.getInsectimagelistList().add(new InsectImageListDTO(iil));
                }
            }
            log.log(Level.INFO, insect.getGroupName());
            result.add(insect);
        }
        return result;
    }

    private List<InsectImageDTO> getInsectImageList() {
        List<InsectImageDTO> list = new ArrayList<>();
        Query q = em.createNamedQuery("Insectimage.findAll", Insectimage.class);
        List<Insectimage> cate = q.getResultList();
        for (Insectimage c : cate) {
            InsectImageDTO insect = new InsectImageDTO(c);
            for (Insectimagelist iil : c.getInsectimagelistList()) {
                insect.getInsectimagelistList().add(new InsectImageListDTO(iil));
            }

            log.log(Level.INFO, insect.getInsect().getGroupName());
            list.add(insect);
        }
        return list;
    }

    private List<CategoryDTO> getCategoryDTOs() {
        List<CategoryDTO> result = new ArrayList<>();
        Query q = em.createNamedQuery("Category.findAll", Category.class);
        List<Category> cate = q.getResultList();
        for (Category c : cate) {
            CategoryDTO cdto = new CategoryDTO(c);
            for (Conditions cd : c.getConditionsList()) {
                cdto.getConditionsList().add(new ConditionsDTO(cd));
            }
            result.add(cdto);
        }
        return result;
    }

    private List<EvaluationSiteDTO> getEvaluationSiteDTOs() {
        List<EvaluationSiteDTO> result = new ArrayList<>();
        Query q = em.createNamedQuery("Evaluationsite.findAll", Evaluationsite.class);
        List<Evaluationsite> cate = q.getResultList();
        for (Evaluationsite c : cate) {
            EvaluationSiteDTO cdto = new EvaluationSiteDTO(c);
            result.add(cdto);
        }
        return result;
    }

    private List<CommentDTO> getCommentDTOs() {
        List<CommentDTO> result = new ArrayList<>();
        Query q = em.createNamedQuery("Comment.findAll", Comment.class);
        List<Comment> cate = q.getResultList();
        for (Comment c : cate) {
            result.add(new CommentDTO(c));
        }
        return result;
    }

    private List<ConditionsDTO> getConditionsDTOs() {
        List<ConditionsDTO> result = new ArrayList<>();
        Query q = em.createNamedQuery("Conditions.findAll", Conditions.class);
        List<Conditions> cate = q.getResultList();
        for (Conditions c : cate) {
            ConditionsDTO conditionsDTO = new ConditionsDTO(c);
            for (Evaluation evaluation : c.getEvaluationList()) {
                EvaluationDTO evaluationDTO = new EvaluationDTO(evaluation);

                conditionsDTO.getEvaluationList().add(evaluationDTO);
            }
            result.add(new ConditionsDTO(c));
        }
        return result;
    }

    public ResponseDTO getInsectList() {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Insect.findAll", Insect.class);
        List<Insect> list = q.getResultList();
        for (Insect ins : list) {
            resp.getInsectList().add(new InsectDTO(ins));
        }

        return resp;
    }

    public ResponseDTO getEvaluationByTeamMember(Integer teamMemberID) {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Evaluation.findByTeamMemberID", Evaluation.class);
        q.setParameter("teamMemberID", teamMemberID);
        List<Evaluation> list = q.getResultList();
        for (Evaluation e : list) {
            resp.getEvaluationList().add(new EvaluationDTO(e));

        }

        return resp;
    }

    public ResponseDTO getTeamByTown(Integer townID) {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Team.findByTownID", Team.class);
        q.setParameter("townID", townID);
        List<Team> list = q.getResultList();
        for (Team te : list) {
            resp.getTeamList().add(new TeamDTO(te));
        }

        return resp;
    }

    public ResponseDTO getEvaluationInsectByEvaluation(Integer evaluationID) {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Evaluationinsect.findByEvaluationID", Evaluationinsect.class);
        q.setParameter("evaluationID", evaluationID);
        List<Evaluationinsect> list = q.getResultList();
        for (Evaluationinsect ei : list) {
            resp.getEvaluationInsectList().add(new EvaluationInsectDTO(ei));
        }

        return resp;
    }

    public ResponseDTO getEvaluationSiteByCategory(Integer categoryID) {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Evaluationsite.findByCategoryID", Evaluationsite.class);
        q.setParameter("categoryID", categoryID);
        List<Evaluationsite> list = q.getResultList();
        for (Evaluationsite es : list) {
            resp.getEvaluationSiteList().add(new EvaluationSiteDTO(es));
        }

        return resp;
    }

    public ResponseDTO getEvaluationByCondtions(Integer conditionsID) {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Evaluation.findByConditionsID", Evaluation.class);
        q.setParameter("conditionsID", conditionsID);
        List<Evaluation> list = q.getResultList();
        for (Evaluation e : list) {
            resp.getEvaluationList().add(new EvaluationDTO(e));
        }

        return resp;
    }

    public ResponseDTO getTeamList(Integer teamID) {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Team.findByTeamID", Team.class);
        q.setParameter("teamID", teamID);
        List<Team> list = q.getResultList();
        for (Team tea : list) {
            resp.getTeamList().add(new TeamDTO(tea));
        }

        return resp;
    }

    public ResponseDTO getTeamMemberProfileData(Integer teamMemberID, ListUtil listUtil, DataUtil dataUtil) {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Teammember.findByTeamMemberID", Teammember.class);
        q.setParameter("teamMemberID", teamMemberID);
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
        for(Teammember t:cs.getTeam().getTeammemberList()){
            if(!t.getEmail().equals(cs.getEmail())){
                team.getTeammemberList().add(new TeamMemberDTO(t));
            }
        }
        teamM.setTeam(team);
        teamM.setEvaluationCount(cs.getEvaluationList().size());

        resp.setTeamMember(teamM);
        return resp;
    }

    public ResponseDTO getTeamMemberList() {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Teammember.findAll", Teammember.class);
        List<Teammember> list = q.getResultList();
        for (Teammember tm : list) {
            resp.getTeamMemberList().add(new TeamMemberDTO(tm));
        }

        return resp;
    }

    public ResponseDTO getStreamByStreamName(String StreamName) {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Stream.findByStreamName", Stream.class);
        q.setParameter("StreamName", StreamName);
        List<Stream> list = q.getResultList();
        resp.setStreamList(new ArrayList<StreamDTO>());
        for (Stream s : list) {
            resp.getStreamList().add(new StreamDTO(s));
        }

        return resp;
    }

    public ResponseDTO getStream() {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Stream.findAll", Stream.class);
        List<Stream> list = q.getResultList();
        //resp.setStreamList(new ArrayList<StreamDTO>());
        for (Stream s : list) {
            resp.getStreamList().add(new StreamDTO(s));
        }

        return resp;
    }

    public ResponseDTO getRiverByRiverName() {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Teammember.findAll", Teammember.class);
        List<Teammember> list = q.getResultList();
        for (Teammember tm : list) {
            resp.getTeamMemberList().add(new TeamMemberDTO(tm));
        }

        return resp;
    }

    public ResponseDTO registrationData() {
        ResponseDTO resp = new ResponseDTO();
        resp.setOrganisationtypeList(getOrganisationtypeList());
        resp.setCountryList(getCountryList());
        return resp;
    }

    public List<OrganisationtypeDTO> getOrganisationtypeList() {
        List<OrganisationtypeDTO> list = new ArrayList<>();
        Query q = em.createNamedQuery("Organisationtype.findAll", Organisationtype.class);
        List<Organisationtype> o = q.getResultList();
        for (Organisationtype tm : o) {
            list.add(new OrganisationtypeDTO(tm));
        }
        return list;
    }

    public List<CountryDTO> getCountryList() {
        List<CountryDTO> list = new ArrayList<>();
        Query q = em.createNamedQuery("Country.findAll", Country.class);
        List<Country> l = q.getResultList();
        for (Country cp : l) {
            list.add(new CountryDTO(cp));
        }

        return list;
    }

    public ResponseDTO getRiverList() {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("River.findAll", River.class);
        List<River> list = q.getResultList();
        for (River riv : list) {
            RiverDTO riverDTO = new RiverDTO(riv);
            //river list
            for (Evaluationsite evaluationsite : riv.getEvaluationsiteList()) {
                EvaluationSiteDTO evaluationSiteDTO = new EvaluationSiteDTO(evaluationsite);

                //evaluation site list
                for (Evaluation evaluation : evaluationsite.getEvaluationList()) {
                    EvaluationDTO evaluationDTO = new EvaluationDTO(evaluation);

                    //evaluation list
                    for (Evaluationimage evaluationimage : evaluation.getEvaluationimageList()) {
                        //evaluation image list                        
                        evaluationDTO.getEvaluationimageList().add(new EvaluationImageDTO(evaluationimage));
                    }

                    //Evaluation comment List 
                    for (Evaluationcomment evaluationcomment : evaluation.getEvaluationcommentList()) {
                        evaluationDTO.getEvaluationcommentList().add(new EvaluationCommentDTO(evaluationcomment));
                    }

                    //Evaluation insect list
                    for (Evaluationinsect evaluationinsect : evaluation.getEvaluationinsectList()) {
                        evaluationDTO.getEvaluationinsectList().add(new EvaluationInsectDTO(evaluationinsect));
                    }
                    evaluationSiteDTO.getEvaluationList().add(evaluationDTO);
                }

                riverDTO.getEvaluationsiteList().add(evaluationSiteDTO);
            }
            for (Stream s : riv.getStreamList()) {
                riverDTO.getStreamList().add(new StreamDTO(s));
            }
            resp.getRiverList().add(riverDTO);

        }

        return resp;
    }

    public ResponseDTO getCommentList() {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Comment.findAll", Comment.class);
        List<Comment> list = q.getResultList();
        for (Comment com : list) {
            resp.getCommentList().add(new CommentDTO(com));
        }

        return resp;
    }

    public ResponseDTO getEvaluationList() {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Evaluation.findAll", Evaluation.class);
        List<Evaluation> list = q.getResultList();
        for (Evaluation eva : list) {
            resp.getEvaluationList().add(new EvaluationDTO(eva));
        }

        return resp;
    }

    public ResponseDTO getEvaluationSiteByRiver(Integer riverID) {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Evaluationsite.findByRiverID", Evaluationsite.class);
        q.setParameter("riverID", riverID);
        List<Evaluationsite> list = q.getResultList();
        for (Evaluationsite e : list) {
            EvaluationSiteDTO siteDTO = new EvaluationSiteDTO(e);
            for (Evaluation ev : e.getEvaluationList()) {
                EvaluationDTO ei = new EvaluationDTO(ev);
                for (Evaluationinsect ea : ev.getEvaluationinsectList()) {
                    EvaluationInsectDTO eid = new EvaluationInsectDTO(ea);
                    ei.getEvaluationinsectList().add(eid);
                }
                siteDTO.getEvaluationList().add(ei);

            }
            resp.getEvaluationSiteList().add(siteDTO);
        }

        return resp;
    }

    public ResponseDTO getCategoryList() {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Category.findAll", Category.class);
        List<Category> list = q.getResultList();
        for (Category cat : list) {
            CategoryDTO categoryDTO = new CategoryDTO(cat);

            for (Evaluationsite evaluationsite : cat.getEvaluationsiteList()) {
                EvaluationSiteDTO evaluationSiteDTO = new EvaluationSiteDTO(evaluationsite);

                for (Evaluation evaluation : evaluationsite.getEvaluationList()) {

                    evaluationSiteDTO.getEvaluationList().add(new EvaluationDTO(evaluation));
                }
                categoryDTO.getEvaluationsiteList().add(evaluationSiteDTO);
            }

            resp.getCategoryList().add(categoryDTO);
        }

        return resp;
    }

    public ResponseDTO getTeamList() {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Team.findAll", Team.class);
        List<Team> list = q.getResultList();
        for (Team tea : list) {
            TeamDTO teamDTO = new TeamDTO(tea);

            for (Teammember teammember : tea.getTeammemberList()) {
                TeamMemberDTO memberDTO = new TeamMemberDTO(teammember);

                for (Evaluation evaluation : teammember.getEvaluationList()) {

                    memberDTO.getEvaluationList().add(new EvaluationDTO(evaluation));
                }
                teamDTO.getTeammemberList().add(memberDTO);
            }
            resp.getTeamList().add(teamDTO);
        }
        return resp;
    }

    public ResponseDTO getEvaluationSiteList() {
        ResponseDTO resp = new ResponseDTO();
        Query q = em.createNamedQuery("Evaluationsite.findAll", Evaluationsite.class
        );
        List<Evaluationsite> list = q.getResultList();
        for (Evaluationsite es : list) {
            resp.getEvaluationSiteList().add(new EvaluationSiteDTO(es));
        }

        return resp;
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

}
