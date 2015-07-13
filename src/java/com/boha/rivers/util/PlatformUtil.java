/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.util;

import com.boha.rivers.transfer.RequestDTO;
import java.util.logging.Logger;
import javax.ejb.Stateless;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

/**
 *
 * @author aubreyM
 */
@Stateless
@TransactionManagement(TransactionManagementType.CONTAINER)
public class PlatformUtil {

    @PersistenceContext
    EntityManager em;

    public void addTimeElapsedWarning(long start, long end, RequestDTO dto, String origin) {
        if (dto == null) {
            dto = new RequestDTO();
        }
        if (end - start > (1000 * THRESHOLD_SECONDS)) {
            StringBuilder sb = new StringBuilder();
            sb.append("Servlet took more than ").append(THRESHOLD_SECONDS)
                    .append(" seconds to process request\nRequest type is ")
                    .append(dto.getRequestType()).append("\n");
            sb.append("Elapsed time in seconds: ").append(Elapsed.getElapsed(start, end));
            
        }
    }

    static final int THRESHOLD_SECONDS = 6;
    public static final int ERROR_DATABASE = 111, ERROR_SERVER = 112,
            SIGNIFICANT_EVENT = 0, ERROR_WEBSOCKET = 113, ERROR_UNKNOWN_REQUEST = 114;
    static final Logger log = Logger.getLogger(PlatformUtil.class.getName());
}
