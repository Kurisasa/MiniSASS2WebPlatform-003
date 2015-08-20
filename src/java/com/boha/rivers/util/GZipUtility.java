/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.boha.rivers.util;

/**
 *
 * @author aubreymalabie
 */
import com.boha.rivers.transfer.ResponseDTO;
import com.google.gson.Gson;
import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.*;
import org.apache.commons.io.IOUtils;

public class GZipUtility {

    static final Gson gson = new Gson();
    static final int ZIP_THRESHOLD = 0;
    private static final Logger logger = Logger.getLogger(GZipUtility.class.getName());
    
     public static ByteBuffer getZippedResponse(ResponseDTO resp) throws IOException {
        long start = System.currentTimeMillis();
        String json = gson.toJson(resp);
        byte[] bytes = null;
        if (json.length() < ZIP_THRESHOLD) {
            bytes = json.getBytes();
            logger.log(Level.INFO, "Creating byteBuffer, No need to zip this, size: {0}", json.length());
        } else {
            bytes = getZippedBytes(json);
            logger.log(Level.INFO, "Creating zipped byteBuffer, unpacked size: {0} packed size: {1}", 
                    new Object[]{getKilobytes(json.length()), getKilobytes(bytes.length)});
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        long end = System.currentTimeMillis();
        logger.log(Level.WARNING, "Elapsed time for zipping up: {0} seconds", Elapsed.getElapsed(start, end));
        return buf;
    }

    public static ByteBuffer getZippedByteBuffer(ResponseDTO resp) throws IOException {
        String json = gson.toJson(resp);

        byte[] bytes = getZippedBytes(json);
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        return buf;
    }

    public static String getZippedString(ResponseDTO resp) throws IOException {
        String json = gson.toJson(resp);
        byte[] bytes = getZippedBytes(json);

        String buf = new String(bytes);
        return buf;
    }

    public static byte[] getZippedByteArray(ResponseDTO resp) throws IOException {
        String json = gson.toJson(resp);
        return getZippedBytes(json);
    }

    public static String getKilobytes(int bytes) {
        BigDecimal m = new BigDecimal(bytes).divide(new BigDecimal(1024));
        return df.format(m.doubleValue()) + " KB";
    }
    static final DecimalFormat df = new DecimalFormat("###,###,###,###,###,###,###,##0.00");

    private static byte[] getZippedBytes(String json)
            throws IOException {

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GZIPOutputStream zos = null;
        try {

            zos = new GZIPOutputStream(byteArrayOutputStream);
            zos.write(json.getBytes());
            IOUtils.closeQuietly(zos);
            

            byte[] bytes = byteArrayOutputStream.toByteArray();
            IOUtils.closeQuietly(byteArrayOutputStream);
            logger.log(Level.OFF, "getZippedBytes, bytes: {0}", bytes.length);
            return bytes;
        } finally {
            
            byteArrayOutputStream.close();
            byteArrayOutputStream.flush();
        }

    }

    public static File getZipped(String data) {
        String gzipFileName = null;
        File zip = null;
        try {
            File dir = RDPProperties.getTemporaryDir();
            if (!dir.exists()) {
                dir.mkdir();
            }
            Random rand = new Random(System.currentTimeMillis());
            gzipFileName = "minisass" + System.currentTimeMillis()
                    + "_" + rand.nextInt(99999999) + ".txt.gz";
            zip = new File(dir, gzipFileName);
            ZipOutputStream out = null;
            try {
                try {
                    out = new ZipOutputStream(new FileOutputStream(zip));
                } catch (FileNotFoundException ex) {
                    logger.log(Level.SEVERE,
                            "Cannot create ZipOutputStream ", ex);
                }
            } catch (Exception ex) {
                logger.log(Level.SEVERE,
                        "Cannot create ZipOutputStream, unknown exception", ex);
            }
            // Create the input file to be compressed
            File file = FileUtility.getFile(data);
            FileInputStream in = null;
            try {
                in = new FileInputStream(file);
            } catch (FileNotFoundException ex) {
                logger.log(Level.SEVERE, "Where the fuck is the file?", ex);
            }
            ZipEntry ze = new ZipEntry("data.zip");
            try {
                out.putNextEntry(ze);
                int c = -1;
                while ((c = in.read()) != -1) {
                    out.write(c);
                }

                out.closeEntry();
                in.close();
            } catch (IOException e) {
                logger.log(Level.SEVERE, "ZipEntry shit bugging out!", e);
            }

            out.close();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Duh! DUH!!", e);
        }
        logger.log(Level.INFO, "Zipped file length: {0}", zip.length());
        return zip;

    }
    private static final int BUFFER = 1024;

//    public static void unzip(File zippedFile) {
//        File dir = zippedFile.getParentFile();
//        try {
//            BufferedOutputStream dest = null;
//            FileInputStream fis = new FileInputStream(zippedFile);
//            ZipInputStream zis = new ZipInputStream(new BufferedInputStream(fis));
//            ZipEntry entry;
//            while ((entry = zis.getNextEntry()) != null) {
//                int count;
//                byte data[] = new byte[BUFFER];
//                // write the files to the disk
//                File imgFile = new File(dir, entry.getName());
//                FileOutputStream fos = new FileOutputStream(imgFile);
//                dest = new BufferedOutputStream(fos, BUFFER);
//                while ((count = zis.read(data, 0, BUFFER)) != -1) {
//                    dest.write(data, 0, count);
//                }
//                dest.flush();
//                dest.close();
//                logger.log(Level.INFO, "\n### File de-compressed: {0}", imgFile.getAbsolutePath());
//            }
//            zis.close();
//        } catch (IOException e) {
//            logger.log(Level.SEVERE, "Unable to de-compress zipped file", e);
//        }
//
//    }
    public static String unzip(InputStream is) throws IOException {
        StringBuilder sb;
        try (GZIPInputStream gzipStream = new GZIPInputStream(is)) {
            InputStreamReader streamReader = new InputStreamReader(gzipStream);
            BufferedReader bufferedReader = new BufferedReader(streamReader);
            String content;
            sb = new StringBuilder();
            while ((content = bufferedReader.readLine()) != null) {
                sb.append(content);
            }
        }

        System.out.println("##### data has been un-compressed OK");
        return sb.toString();

    }

}
