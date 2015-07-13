/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Aubrey Malabie
 */
public class RDPProperties {

    public static File getTemporaryDir() {
        getProperties();
        File d = new File(props.getProperty("tempDir"));
        if (!d.exists()) {
            d.mkdir();
        }
        return d;
    }

    public static File getTemporaryZippedDir() {
        getProperties();
        File d = new File(props.getProperty("zipped"));
        if (!d.exists()) {
            d.mkdir();
        }
        return d;
    }

    public static File getImageDir() {
        getProperties();
        File d = new File(props.getProperty("images"));
        if (!d.exists()) {
            d.mkdir();
        }
        return d;
    }

    public static File getLogDir() {
        getProperties();
        File d = new File(props.getProperty("logs"));
        if (!d.exists()) {
            d.mkdir();
        }
        return d;
    }

    private static void getProperties() {
        if (props != null) {
            return;
        }
        props = new Properties();
        try {
            File f = null;
            f = new File("/Properties/rdp.properties");
            if (!f.exists()) {
                f = new File("/opt/properties/rdp.properties");
            }
            if (!f.exists()) {
                logger.log(Level.SEVERE, "rdp Properties File not found");
            } else {
                logger.log(Level.INFO, "rdp Properties: {0}\n\n\n", f.getAbsolutePath());
                props.load(new FileInputStream(f));
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Properties file rdp.properties not found or corrupted");
        }

    }
    private static final Logger logger = Logger.getLogger(RDPProperties.class.getName());
    private static Properties props;
}
