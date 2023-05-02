package org.lealone.plugins.bench.tpcc.benchmarksql.client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Formatter;
import java.util.Properties;
import java.util.regex.Pattern;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.plugins.bench.tpcc.benchmarksql.OSCollector.OSCollector;

@SuppressWarnings({ "unused", "resource" })
public class jTPCC implements jTPCCConfig {
    private static Logger log = LoggerFactory.getLogger(jTPCC.class);
    private static String resultDirName = null;
    private static BufferedWriter resultCSV = null;
    private static BufferedWriter runInfoCSV = null;
    private static int runID = 0;

    private int dbType = DB_UNKNOWN;
    private int currentlyDisplayedTerminal;

    private jTPCCTerminal[] terminals;
    private String[] terminalNames;
    private boolean terminalsBlockingExit = false;
    private long terminalsStarted = 0, sessionCount = 0, transactionCount = 0;
    private Object counterLock = new Object();

    private long newOrderCounter = 0, sessionStartTimestamp, sessionEndTimestamp,
            sessionNextTimestamp = Long.MAX_VALUE, sessionNextKounter = 0;
    private long sessionEndTargetTime = -1, fastNewOrderCounter, recentTpmC = 0, recentTpmTotal = 0;
    private boolean signalTerminalsRequestEndSent = false, databaseDriverLoaded = false;

    private FileOutputStream fileOutputStream;
    private PrintStream printStreamReport;
    private String sessionStart, sessionEnd;
    private int limPerMin_Terminal;

    private double tpmC;
    private jTPCCRandom rnd;
    private OSCollector osCollector = null;

    public static void main(String args[]) {
        new jTPCC();
    }

    private String getProp(Properties p, String pName) {
        String prop = p.getProperty(pName);
        log.info("Term-00, " + pName + "=" + prop);
        return (prop);
    }

    private String getProp(Properties p, String pName, String defVal) {
        String prop = p.getProperty(pName);
        if (prop == null)
            prop = defVal;
        log.info("Term-00, " + pName + "=" + prop);
        return (prop);
    }

    public jTPCC() {

        // load the ini file
        Properties ini = new Properties();
        try {
            ini.load(new FileInputStream(System.getProperty("prop")));
        } catch (IOException e) {
            errorMessage("Term-00, could not load properties file");
        }

        log.info("Term-00, ");
        log.info("Term-00, +-------------------------------------------------------------+");
        log.info("Term-00,      BenchmarkSQL v" + JTPCCVERSION);
        log.info("Term-00, +-------------------------------------------------------------+");
        log.info("Term-00,  (c) 2003, Raul Barbosa");
        log.info("Term-00,  (c) 2004-2016, Denis Lussier");
        log.info("Term-00,  (c) 2016, Jan Wieck");
        log.info("Term-00, +-------------------------------------------------------------+");
        log.info("Term-00, ");
        String iDB = getProp(ini, "db");
        String iDriver = getProp(ini, "driver");
        String iConn = getProp(ini, "conn");
        String iUser = getProp(ini, "user");
        String iPassword = ini.getProperty("password");

        log.info("Term-00, ");
        String iWarehouses = getProp(ini, "warehouses");
        String iTerminals = getProp(ini, "terminals");

        String iRunTxnsPerTerminal = ini.getProperty("runTxnsPerTerminal");
        String iRunMins = ini.getProperty("runMins");
        if (Integer.parseInt(iRunTxnsPerTerminal) == 0 && Integer.parseInt(iRunMins) != 0) {
            log.info("Term-00, runMins" + "=" + iRunMins);
        } else if (Integer.parseInt(iRunTxnsPerTerminal) != 0 && Integer.parseInt(iRunMins) == 0) {
            log.info("Term-00, runTxnsPerTerminal" + "=" + iRunTxnsPerTerminal);
        } else {
            errorMessage(
                    "Term-00, Must indicate either transactions per terminal or number of run minutes!");
        }
        ;
        String limPerMin = getProp(ini, "limitTxnsPerMin");
        String iTermWhseFixed = getProp(ini, "terminalWarehouseFixed");
        String iUseStoredProcs = getProp(ini, "useStoredProcedures");
        log.info("Term-00, ");

        String iNewOrderWeight = getProp(ini, "newOrderWeight", "43.47826");
        String iPaymentWeight = getProp(ini, "paymentWeight", "43.47826");
        String iOrderStatusWeight = getProp(ini, "orderStatusWeight", "4.347827");
        String iDeliveryWeight = getProp(ini, "deliveryWeight", "4.347826");
        String iStockLevelWeight = getProp(ini, "stockLevelWeight", "4.347827");

        log.info("Term-00, ");
        String resultDirectory = getProp(ini, "resultDirectory");
        String osCollectorScript = getProp(ini, "osCollectorScript");

        log.info("Term-00, ");

        if (iDB.equals("firebird"))
            dbType = DB_FIREBIRD;
        else if (iDB.equals("oracle"))
            dbType = DB_ORACLE;
        else if (iDB.equals("postgres"))
            dbType = DB_POSTGRES;
        else {
            log.error("unknown database type '" + iDB + "'");
            return;
        }

        if (Integer.parseInt(limPerMin) != 0) {
            limPerMin_Terminal = Integer.parseInt(limPerMin) / Integer.parseInt(iTerminals);
        } else {
            limPerMin_Terminal = -1;
        }

        boolean iRunMinsBool = false;

        try {
            String driver = iDriver;
            printMessage("Loading database driver: \'" + driver + "\'...");
            Class.forName(iDriver);
            databaseDriverLoaded = true;
        } catch (Exception ex) {
            errorMessage("Unable to load the database driver!");
            databaseDriverLoaded = false;
        }

        if (databaseDriverLoaded && resultDirectory != null) {
            StringBuffer sb = new StringBuffer();
            Formatter fmt = new Formatter(sb);
            Pattern p = Pattern.compile("%t");
            Calendar cal = Calendar.getInstance();

            String iRunID;

            iRunID = System.getProperty("runID");
            if (iRunID != null) {
                runID = Integer.parseInt(iRunID);
            }

            /*
             * Split the resultDirectory into strings around
             * patterns of %t and then insert date/time formatting
             * based on the current time. That way the resultDirectory
             * in the properties file can have date/time format
             * elements like in result_%tY-%tm-%td to embed the current
             * date in the directory name.
             */
            String[] parts = p.split(resultDirectory, -1);
            sb.append(parts[0]);
            for (int i = 1; i < parts.length; i++) {
                fmt.format("%t" + parts[i].substring(0, 1), cal);
                sb.append(parts[i].substring(1));
            }
            resultDirName = sb.toString();
            File resultDir = new File(resultDirName);
            File resultDataDir = new File(resultDir, "data");

            // Create the output directory structure.
            if (!resultDir.mkdir()) {
                log.error("Failed to create directory '" + resultDir.getPath() + "'");
                System.exit(1);
            }
            if (!resultDataDir.mkdir()) {
                log.error("Failed to create directory '" + resultDataDir.getPath() + "'");
                System.exit(1);
            }

            // Copy the used properties file into the resultDirectory.
            try {
                copyFile(new File(System.getProperty("prop")), new File(resultDir, "run.properties"));
            } catch (Exception e) {
                log.error(e.getMessage());
                System.exit(1);
            }
            log.info("Term-00, copied " + System.getProperty("prop") + " to "
                    + new File(resultDir, "run.properties").getPath());

            // Create the runInfo.csv file.
            String runInfoCSVName = new File(resultDataDir, "runInfo.csv").getPath();
            try {
                runInfoCSV = new BufferedWriter(new FileWriter(runInfoCSVName));
                runInfoCSV.write("run,driver,driverVersion,db,sessionStart," + "runMins,"
                        + "loadWarehouses,runWarehouses,numSUTThreads," + "limitTxnsPerMin,"
                        + "thinkTimeMultiplier,keyingTimeMultiplier\n");
            } catch (IOException e) {
                log.error(e.getMessage());
                System.exit(1);
            }
            log.info("Term-00, created " + runInfoCSVName + " for runID " + runID);

            // Open the per transaction result.csv file.
            String resultCSVName = new File(resultDataDir, "result.csv").getPath();
            try {
                resultCSV = new BufferedWriter(new FileWriter(resultCSVName));
                resultCSV.write("run,elapsed,latency,dblatency," + "ttype,rbk,dskipped,error\n");
            } catch (IOException e) {
                log.error(e.getMessage());
                System.exit(1);
            }
            log.info("Term-00, writing per transaction results to " + resultCSVName);

            if (osCollectorScript != null) {
                osCollector = new OSCollector(getProp(ini, "osCollectorScript"), runID,
                        Integer.parseInt(getProp(ini, "osCollectorInterval")),
                        getProp(ini, "osCollectorSSHAddr"), getProp(ini, "osCollectorDevices"),
                        resultDataDir, log);
            }

            log.info("Term-00,");
        }

        if (databaseDriverLoaded) {
            try {
                boolean limitIsTime = iRunMinsBool;
                int numTerminals = -1;
                int transactionsPerTerminal = -1;
                int numWarehouses = -1;
                int loadWarehouses = -1;
                double newOrderWeightValue = 43.47826;
                double paymentWeightValue = 43.47826;
                double orderStatusWeightValue = 4.347827;
                double deliveryWeightValue = 4.347826;
                double stockLevelWeightValue = 4.347827;
                long executionTimeMillis = -1;
                boolean terminalWarehouseFixed = true;
                boolean useStoredProcedures = false;
                long CLoad;

                Properties dbProps = new Properties();
                dbProps.setProperty("user", iUser);
                dbProps.setProperty("password", iPassword);

                /*
                 * Fine tuning of database conneciton parameters if needed.
                 */
                switch (dbType) {
                case DB_FIREBIRD:
                    /*
                     * Firebird needs no_rec_version for our load
                     * to work. Even with that some "deadlocks"
                     * occur. Note that the message "deadlock" in
                     * Firebird can mean something completely different,
                     * namely that there was a conflicting write to
                     * a row that could not be resolved.
                     */
                    dbProps.setProperty("TRANSACTION_READ_COMMITTED", "isc_tpb_read_committed,"
                            + "isc_tpb_no_rec_version," + "isc_tpb_write," + "isc_tpb_wait");
                    break;

                default:
                    break;
                }

                try {
                    loadWarehouses = Integer.parseInt(jTPCCUtil.getConfig(iConn, dbProps, "warehouses"));
                    CLoad = Long.parseLong(jTPCCUtil.getConfig(iConn, dbProps, "nURandCLast"));
                } catch (Exception e) {
                    errorMessage(e.getMessage());
                    throw e;
                }
                this.rnd = new jTPCCRandom(CLoad);
                log.info("Term-00, C value for C_LAST during load: " + CLoad);
                log.info("Term-00, C value for C_LAST this run:    " + rnd.getNURandCLast());
                log.info("Term-00, ");

                fastNewOrderCounter = 0;
                updateStatusLine();

                try {
                    if (Integer.parseInt(iRunMins) != 0 && Integer.parseInt(iRunTxnsPerTerminal) == 0) {
                        iRunMinsBool = true;
                    } else if (Integer.parseInt(iRunMins) == 0
                            && Integer.parseInt(iRunTxnsPerTerminal) != 0) {
                        iRunMinsBool = false;
                    } else {
                        throw new NumberFormatException();
                    }
                } catch (NumberFormatException e1) {
                    errorMessage(
                            "Must indicate either transactions per terminal or number of run minutes!");
                    throw new Exception();
                }

                try {
                    numWarehouses = Integer.parseInt(iWarehouses);
                    if (numWarehouses <= 0)
                        throw new NumberFormatException();
                } catch (NumberFormatException e1) {
                    errorMessage("Invalid number of warehouses!");
                    throw new Exception();
                }
                if (numWarehouses > loadWarehouses) {
                    errorMessage("numWarehouses cannot be greater "
                            + "than the warehouses loaded in the database");
                    throw new Exception();
                }

                try {
                    numTerminals = Integer.parseInt(iTerminals);
                    if (numTerminals <= 0 || numTerminals > 10 * numWarehouses)
                        throw new NumberFormatException();
                } catch (NumberFormatException e1) {
                    errorMessage("Invalid number of terminals!");
                    throw new Exception();
                }

                if (Long.parseLong(iRunMins) != 0 && Integer.parseInt(iRunTxnsPerTerminal) == 0) {
                    try {
                        executionTimeMillis = Long.parseLong(iRunMins) * 60000;
                        if (executionTimeMillis <= 0)
                            throw new NumberFormatException();
                    } catch (NumberFormatException e1) {
                        errorMessage("Invalid number of minutes!");
                        throw new Exception();
                    }
                } else {
                    try {
                        transactionsPerTerminal = Integer.parseInt(iRunTxnsPerTerminal);
                        if (transactionsPerTerminal <= 0)
                            throw new NumberFormatException();
                    } catch (NumberFormatException e1) {
                        errorMessage("Invalid number of transactions per terminal!");
                        throw new Exception();
                    }
                }

                terminalWarehouseFixed = Boolean.parseBoolean(iTermWhseFixed);
                useStoredProcedures = Boolean.parseBoolean(iUseStoredProcs);

                try {
                    newOrderWeightValue = Double.parseDouble(iNewOrderWeight);
                    paymentWeightValue = Double.parseDouble(iPaymentWeight);
                    orderStatusWeightValue = Double.parseDouble(iOrderStatusWeight);
                    deliveryWeightValue = Double.parseDouble(iDeliveryWeight);
                    stockLevelWeightValue = Double.parseDouble(iStockLevelWeight);

                    if (newOrderWeightValue < 0 || paymentWeightValue < 0 || orderStatusWeightValue < 0
                            || deliveryWeightValue < 0 || stockLevelWeightValue < 0)
                        throw new NumberFormatException();
                    else if (newOrderWeightValue == 0 && paymentWeightValue == 0
                            && orderStatusWeightValue == 0 && deliveryWeightValue == 0
                            && stockLevelWeightValue == 0)
                        throw new NumberFormatException();
                } catch (NumberFormatException e1) {
                    errorMessage("Invalid number in mix percentage!");
                    throw new Exception();
                }

                double sumWeight = newOrderWeightValue + paymentWeightValue + orderStatusWeightValue
                        + deliveryWeightValue + stockLevelWeightValue;
                sumWeight = Math.round(sumWeight * 100.0) / 100.0;
                if (sumWeight != 100.0) {
                    errorMessage(
                            "Sum of mix percentage parameters must equal 100%! Have %f" + sumWeight);
                    throw new Exception();
                }

                newOrderCounter = 0;
                printMessage("Session started!");
                if (!limitIsTime)
                    printMessage("Creating " + numTerminals + " terminal(s) with "
                            + transactionsPerTerminal + " transaction(s) per terminal...");
                else
                    printMessage("Creating " + numTerminals + " terminal(s) with "
                            + (executionTimeMillis / 60000) + " minute(s) of execution...");
                if (terminalWarehouseFixed)
                    printMessage("Terminal Warehouse is fixed");
                else
                    printMessage("Terminal Warehouse is NOT fixed");
                if (useStoredProcedures)
                    printMessage("Using Stored Procedures");
                else
                    printMessage("NOT using Stored Procedures");
                printMessage("Transaction Weights: " + newOrderWeightValue + "% New-Order, "
                        + paymentWeightValue + "% Payment, " + orderStatusWeightValue
                        + "% Order-Status, " + deliveryWeightValue + "% Delivery, "
                        + stockLevelWeightValue + "% Stock-Level");

                printMessage("Number of Terminals\t" + numTerminals);

                terminals = new jTPCCTerminal[numTerminals];
                terminalNames = new String[numTerminals];
                terminalsStarted = numTerminals;
                try {
                    String database = iConn;
                    String username = iUser;
                    String password = iPassword;

                    int[][] usedTerminals = new int[numWarehouses][10];
                    for (int i = 0; i < numWarehouses; i++)
                        for (int j = 0; j < 10; j++)
                            usedTerminals[i][j] = 0;

                    for (int i = 0; i < numTerminals; i++) {
                        int terminalWarehouseID;
                        int terminalDistrictID;
                        do {
                            terminalWarehouseID = rnd.nextInt(1, numWarehouses);
                            terminalDistrictID = rnd.nextInt(1, 10);
                        } while (usedTerminals[terminalWarehouseID - 1][terminalDistrictID - 1] == 1);
                        usedTerminals[terminalWarehouseID - 1][terminalDistrictID - 1] = 1;

                        String terminalName = "Term-" + (i >= 9 ? "" + (i + 1) : "0" + (i + 1));
                        Connection conn = null;
                        printMessage("Creating database connection for " + terminalName + "...");
                        conn = DriverManager.getConnection(database, dbProps);
                        conn.setAutoCommit(false);

                        jTPCCTerminal terminal = new jTPCCTerminal(terminalName, terminalWarehouseID,
                                terminalDistrictID, conn, dbType, transactionsPerTerminal,
                                terminalWarehouseFixed, useStoredProcedures, paymentWeightValue,
                                orderStatusWeightValue, deliveryWeightValue, stockLevelWeightValue,
                                numWarehouses, limPerMin_Terminal, this);

                        terminals[i] = terminal;
                        terminalNames[i] = terminalName;
                        printMessage(terminalName + "\t" + terminalWarehouseID);
                    }

                    sessionEndTargetTime = executionTimeMillis;
                    signalTerminalsRequestEndSent = false;

                    printMessage("Transaction\tWeight");
                    printMessage("% New-Order\t" + newOrderWeightValue);
                    printMessage("% Payment\t" + paymentWeightValue);
                    printMessage("% Order-Status\t" + orderStatusWeightValue);
                    printMessage("% Delivery\t" + deliveryWeightValue);
                    printMessage("% Stock-Level\t" + stockLevelWeightValue);

                    printMessage("Transaction Number\tTerminal\tType\tExecution Time (ms)\t\tComment");

                    printMessage("Created " + numTerminals + " terminal(s) successfully!");
                    boolean dummvar = true;

                    // Create Terminals, Start Transactions
                    sessionStart = getCurrentTime();
                    sessionStartTimestamp = System.currentTimeMillis();
                    sessionNextTimestamp = sessionStartTimestamp;
                    if (sessionEndTargetTime != -1)
                        sessionEndTargetTime += sessionStartTimestamp;

                    // Record run parameters in runInfo.csv
                    if (runInfoCSV != null) {
                        try {
                            StringBuffer infoSB = new StringBuffer();
                            Formatter infoFmt = new Formatter(infoSB);
                            infoFmt.format("%d,simple,%s,%s,%s,%s,%d,%d,%d,%d,1.0,1.0\n", runID,
                                    JTPCCVERSION, iDB,
                                    new java.sql.Timestamp(sessionStartTimestamp).toString(), iRunMins,
                                    loadWarehouses, numWarehouses, numTerminals,
                                    Integer.parseInt(limPerMin));
                            runInfoCSV.write(infoSB.toString());
                            runInfoCSV.close();
                        } catch (Exception e) {
                            log.error(e.getMessage());
                            System.exit(1);
                        }
                    }

                    synchronized (terminals) {
                        printMessage("Starting all terminals...");
                        transactionCount = 1;
                        for (int i = 0; i < terminals.length; i++)
                            (new Thread(terminals[i])).start();

                    }

                    printMessage("All terminals started executing " + sessionStart);
                }

                catch (Exception e1) {
                    errorMessage("This session ended with errors!");
                    printStreamReport.close();
                    fileOutputStream.close();

                    throw new Exception();
                }

            } catch (Exception ex) {
            }
        }
        updateStatusLine();
    }

    private void signalTerminalsRequestEnd(boolean timeTriggered) {
        synchronized (terminals) {
            if (!signalTerminalsRequestEndSent) {
                if (timeTriggered)
                    printMessage("The time limit has been reached.");
                printMessage("Signalling all terminals to stop...");
                signalTerminalsRequestEndSent = true;

                for (int i = 0; i < terminals.length; i++)
                    if (terminals[i] != null)
                        terminals[i].stopRunningWhenPossible();

                printMessage("Waiting for all active transactions to end...");
            }
        }
    }

    public void signalTerminalEnded(jTPCCTerminal terminal, long countNewOrdersExecuted) {
        synchronized (terminals) {
            boolean found = false;
            terminalsStarted--;
            for (int i = 0; i < terminals.length && !found; i++) {
                if (terminals[i] == terminal) {
                    terminals[i] = null;
                    terminalNames[i] = "(" + terminalNames[i] + ")";
                    newOrderCounter += countNewOrdersExecuted;
                    found = true;
                }
            }
        }

        if (terminalsStarted == 0) {
            sessionEnd = getCurrentTime();
            sessionEndTimestamp = System.currentTimeMillis();
            sessionEndTargetTime = -1;
            printMessage("All terminals finished executing " + sessionEnd);
            endReport();
            terminalsBlockingExit = false;
            printMessage("Session finished!");

            // If we opened a per transaction result file, close it.
            if (resultCSV != null) {
                try {
                    resultCSV.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
                ;
            }

            // Stop the OSCollector, if it is active.
            if (osCollector != null) {
                osCollector.stop();
                osCollector = null;
            }
        }
    }

    public void signalTerminalEndedTransaction(String terminalName, String transactionType,
            long executionTime, String comment, int newOrder) {
        synchronized (counterLock) {
            transactionCount++;
            fastNewOrderCounter += newOrder;
        }

        if (sessionEndTargetTime != -1 && System.currentTimeMillis() > sessionEndTargetTime) {
            signalTerminalsRequestEnd(true);
        }

        updateStatusLine();

    }

    public jTPCCRandom getRnd() {
        return rnd;
    }

    public void resultAppend(jTPCCTData term) {
        if (resultCSV != null) {
            try {
                resultCSV.write(runID + "," + term.resultLine(sessionStartTimestamp));
            } catch (IOException e) {
                log.error("Term-00, " + e.getMessage());
            }
        }
    }

    private void endReport() {
        long currTimeMillis = System.currentTimeMillis();
        long freeMem = Runtime.getRuntime().freeMemory() / (1024 * 1024);
        long totalMem = Runtime.getRuntime().totalMemory() / (1024 * 1024);
        double tpmC = (6000000 * fastNewOrderCounter / (currTimeMillis - sessionStartTimestamp)) / 100.0;
        double tpmTotal = (6000000 * transactionCount / (currTimeMillis - sessionStartTimestamp))
                / 100.0;

        System.out.println("");
        log.info("Term-00, ");
        log.info("Term-00, ");
        log.info("Term-00, Measured tpmC (NewOrders) = " + tpmC);
        log.info("Term-00, Measured tpmTOTAL = " + tpmTotal);
        log.info("Term-00, Session Start     = " + sessionStart);
        log.info("Term-00, Session End       = " + sessionEnd);
        log.info("Term-00, Transaction Count = " + (transactionCount - 1));

    }

    private void printMessage(String message) {
        log.trace("Term-00, " + message);
    }

    private void errorMessage(String message) {
        log.error("Term-00, " + message);
    }

    private void exit() {
        System.exit(0);
    }

    private String getCurrentTime() {
        return dateFormat.format(new java.util.Date());
    }

    private String getFileNameSuffix() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        return dateFormat.format(new java.util.Date());
    }

    synchronized private void updateStatusLine() {
        long currTimeMillis = System.currentTimeMillis();

        if (currTimeMillis > sessionNextTimestamp) {
            StringBuilder informativeText = new StringBuilder("");
            Formatter fmt = new Formatter(informativeText);
            double tpmC = (6000000 * fastNewOrderCounter / (currTimeMillis - sessionStartTimestamp))
                    / 100.0;
            double tpmTotal = (6000000 * transactionCount / (currTimeMillis - sessionStartTimestamp))
                    / 100.0;

            sessionNextTimestamp += 1000; /* update this every seconds */

            fmt.format("progress: %.1f, tpmTOTAL: %.1f, tpmC: %.1f",
                    (double) (currTimeMillis - sessionStartTimestamp) / 1000, tpmTotal, tpmC);

            recentTpmTotal = (transactionCount - sessionNextKounter) * 12;
            recentTpmC = (fastNewOrderCounter - sessionNextKounter) * 12;
            sessionNextKounter = fastNewOrderCounter;
            // XXX: This doesn't seem worth including
            // fmt.format(" Current tpmTOTAL: %d", recentTpmTotal);

            long freeMem = Runtime.getRuntime().freeMemory() / (1024 * 1024);
            long totalMem = Runtime.getRuntime().totalMemory() / (1024 * 1024);

            // XXX: This doesn't seem worth including
            // fmt.format(" Memory Usage: %dMB / %dMB ", (totalMem - freeMem), totalMem);

            System.out.println(informativeText);
        }
    }

    private void copyFile(File in, File out) throws FileNotFoundException, IOException {
        FileInputStream strIn = new FileInputStream(in);
        FileOutputStream strOut = new FileOutputStream(out);
        byte buf[] = new byte[65536];

        int len = strIn.read(buf);
        while (len > 0) {
            strOut.write(buf, 0, len);
            len = strIn.read(buf);
        }

        strOut.close();
        strIn.close();
    }
}
