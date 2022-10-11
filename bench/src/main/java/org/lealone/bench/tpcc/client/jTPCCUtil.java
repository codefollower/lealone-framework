package org.lealone.bench.tpcc.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class jTPCCUtil implements jTPCCConfig {
    private static Connection dbConn = null;
    private static PreparedStatement stmtGetConfig = null;

    public static String getSysProp(String inSysProperty, String defaultValue) {

        String outPropertyValue = null;

        try {
            outPropertyValue = System.getProperty(inSysProperty, defaultValue);
        } catch (Exception e) {
            System.err.println("Error Reading Required System Property '" + inSysProperty + "'");
        }

        return (outPropertyValue);

    } // end getSysProp

    public static String randomStr(long strLen) {

        char freshChar;
        String freshString;
        freshString = "";

        while (freshString.length() < (strLen - 1)) {

            freshChar = (char) (Math.random() * 128);
            if (Character.isLetter(freshChar)) {
                freshString += freshChar;
            }
        }

        return (freshString);

    } // end randomStr

    public static String getCurrentTime() {
        return dateFormat.format(new java.util.Date());
    }

    public static String formattedDouble(double d) {
        String dS = "" + d;
        return dS.length() > 6 ? dS.substring(0, 6) : dS;
    }

    public static String getConfig(String db, Properties dbProps, String option) throws Exception {
        ResultSet rs;
        String value;

        if (dbConn == null) {
            dbConn = DriverManager.getConnection(db, dbProps);
            stmtGetConfig = dbConn.prepareStatement("SELECT cfg_value FROM bmsql_config " + " WHERE cfg_name = ?");
        }
        stmtGetConfig.setString(1, option);
        rs = stmtGetConfig.executeQuery();
        if (!rs.next())
            throw new Exception("DB Load configuration parameter '" + option + "' not found");
        value = rs.getString("cfg_value");
        rs.close();

        return value;
    }

} // end jTPCCUtil
