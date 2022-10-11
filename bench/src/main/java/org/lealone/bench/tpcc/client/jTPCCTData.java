package org.lealone.bench.tpcc.client;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Formatter;
import java.util.Vector;

import org.lealone.common.logging.Logger;

@SuppressWarnings("unused")
public class jTPCCTData {
    protected int numWarehouses = 0;

    public final static int TT_NEW_ORDER = 0, TT_PAYMENT = 1, TT_ORDER_STATUS = 2, TT_STOCK_LEVEL = 3, TT_DELIVERY = 4,
            TT_DELIVERY_BG = 5, TT_NONE = 6, TT_DONE = 7;

    public final static String transTypeNames[] = { "NEW_ORDER", "PAYMENT", "ORDER_STATUS", "STOCK_LEVEL", "DELIVERY",
            "DELIVERY_BG", "NONE", "DONE" };

    public int sched_code;
    public long sched_fuzz;
    public jTPCCTData term_left;
    public jTPCCTData term_right;
    public int tree_height;

    private int transType;
    private long transDue;
    private long transStart;
    private long transEnd;
    private boolean transRbk;
    private String transError;

    private int terminalWarehouse = 0;
    private int terminalDistrict = 0;

    private NewOrderData newOrder = null;
    private PaymentData payment = null;
    private OrderStatusData orderStatus = null;
    private StockLevelData stockLevel = null;
    private DeliveryData delivery = null;
    private DeliveryBGData deliveryBG = null;

    private static Object traceLock = new Object();

    private StringBuffer resultSB = new StringBuffer();
    private Formatter resultFmt = new Formatter(resultSB);
    private boolean useStoredProcedures = false;
    private int dbType = jTPCCConfig.DB_UNKNOWN;

    public void setNumWarehouses(int num) {
        numWarehouses = num;
    }

    public void setWarehouse(int warehouse) {
        terminalWarehouse = warehouse;
    }

    public int getWarehouse() {
        return terminalWarehouse;
    }

    public void setDistrict(int district) {
        terminalDistrict = district;
    }

    public int getDistrict() {
        return terminalDistrict;
    }

    public void setUseStoredProcedures(boolean flag) {
        useStoredProcedures = flag;
    }

    public boolean getUseStoredProcedures() {
        return useStoredProcedures;
    }

    public void setDBType(int type) {
        dbType = type;
    }

    public int getDBType() {
        return dbType;
    }

    public void execute(Logger log, jTPCCConnection db) throws Exception {
        transStart = System.currentTimeMillis();
        if (transDue == 0)
            transDue = transStart;

        switch (transType) {
        case TT_NEW_ORDER:
            if (useStoredProcedures) {
                switch (dbType) {
                case jTPCCConfig.DB_POSTGRES:
                    executeNewOrderStoredProcPostgres(log, db);
                    break;

                case jTPCCConfig.DB_ORACLE:
                    executeNewOrderStoredProcOracle(log, db);
                    break;

                default:
                    throw new Exception("Stored Procedure for NEW_ORDER not implemented");
                }
            } else {
                executeNewOrder(log, db);
            }
            break;

        case TT_PAYMENT:
            if (useStoredProcedures) {
                switch (dbType) {
                case jTPCCConfig.DB_POSTGRES:
                    executePaymentStoredProcPostgres(log, db);
                    break;

                case jTPCCConfig.DB_ORACLE:
                    executePaymentStoredProcOracle(log, db);
                    break;

                default:
                    throw new Exception("Stored Procedure for PAYMENT not implemented");
                }
            } else {
                executePayment(log, db);
            }
            break;

        case TT_ORDER_STATUS:
            if (useStoredProcedures) {
                switch (dbType) {
                case jTPCCConfig.DB_POSTGRES:
                    executeOrderStatusStoredProcPostgres(log, db);
                    break;

                case jTPCCConfig.DB_ORACLE:
                    executeOrderStatusStoredProcOracle(log, db);
                    break;

                default:
                    throw new Exception("Stored Procedure for ORDER_STATUS not implemented");
                }
            } else {
                executeOrderStatus(log, db);
            }
            break;

        case TT_STOCK_LEVEL:
            if (useStoredProcedures) {
                switch (dbType) {
                case jTPCCConfig.DB_POSTGRES:
                    executeStockLevelStoredProcPostgres(log, db);
                    break;

                case jTPCCConfig.DB_ORACLE:
                    executeStockLevelStoredProcOracle(log, db);
                    break;

                default:
                    throw new Exception("Stored Procedure for STOCK_LEVEL not implemented");
                }
            } else {
                executeStockLevel(log, db);
            }

            break;

        case TT_DELIVERY:
            executeDelivery(log, db);
            break;

        case TT_DELIVERY_BG:
            if (useStoredProcedures) {
                switch (dbType) {
                case jTPCCConfig.DB_POSTGRES:
                    executeDeliveryBGStoredProcPostgres(log, db);
                    break;

                case jTPCCConfig.DB_ORACLE:
                    executeDeliveryBGStoredProcOracle(log, db);
                    break;

                default:
                    throw new Exception("Stored Procedure for DELIVERY_BG not implemented");
                }
            } else {
                executeDeliveryBG(log, db);
            }

            break;

        default:
            throw new Exception("Unknown transType " + transType);
        }

        transEnd = System.currentTimeMillis();
    }

    public void traceScreen(Logger log) throws Exception {
        StringBuffer sb = new StringBuffer();
        Formatter fmt = new Formatter(sb);

        StringBuffer screenSb[] = new StringBuffer[23];
        Formatter screenFmt[] = new Formatter[23];
        for (int i = 0; i < 23; i++) {
            screenSb[i] = new StringBuffer();
            screenFmt[i] = new Formatter(screenSb[i]);
        }

        if (!log.isTraceEnabled())
            return;

        if (transType < TT_NEW_ORDER || transType > TT_DONE)
            throw new Exception("Unknown transType " + transType);

        synchronized (traceLock) {
            fmt.format("==== %s %s ==== Terminal %d,%d =================================================",
                    transTypeNames[transType], (transEnd == 0) ? "INPUT" : "OUTPUT", terminalWarehouse,
                    terminalDistrict);
            sb.setLength(79);
            log.trace(sb.toString());
            sb.setLength(0);

            fmt.format("---- Due:   %s", (transDue == 0) ? "N/A" : new java.sql.Timestamp(transDue).toString());
            log.trace(sb.toString());
            sb.setLength(0);

            fmt.format("---- Start: %s", (transStart == 0) ? "N/A" : new java.sql.Timestamp(transStart).toString());
            log.trace(sb.toString());
            sb.setLength(0);

            fmt.format("---- End:   %s", (transEnd == 0) ? "N/A" : new java.sql.Timestamp(transEnd).toString());
            log.trace(sb.toString());
            sb.setLength(0);

            if (transError != null) {
                fmt.format("#### ERROR: %s", transError);
                log.trace(sb.toString());
                sb.setLength(0);
            }

            log.trace("-------------------------------------------------------------------------------");

            switch (transType) {
            case TT_NEW_ORDER:
                traceNewOrder(log, screenFmt);
                break;

            case TT_PAYMENT:
                tracePayment(log, screenFmt);
                break;

            case TT_ORDER_STATUS:
                traceOrderStatus(log, screenFmt);
                break;

            case TT_STOCK_LEVEL:
                traceStockLevel(log, screenFmt);
                break;

            case TT_DELIVERY:
                traceDelivery(log, screenFmt);
                break;

            case TT_DELIVERY_BG:
                traceDeliveryBG(log, screenFmt);
                break;

            default:
                throw new Exception("Unknown transType " + transType);
            }

            for (int i = 0; i < 23; i++) {
                if (screenSb[i].length() > 79)
                    screenSb[i].setLength(79);
                log.trace(screenSb[i].toString());
            }

            log.trace("-------------------------------------------------------------------------------");
            log.trace("");
        }
    }

    public String resultLine(long sessionStart) {
        String line;

        resultFmt.format("%d,%d,%d,%s,%d,%d,%d\n", transEnd - sessionStart, transEnd - transDue, transEnd - transStart,
                transTypeNames[transType], (transRbk) ? 1 : 0,
                (transType == TT_DELIVERY_BG) ? getSkippedDeliveries() : 0, (transError == null) ? 0 : 1);
        line = resultSB.toString();
        resultSB.setLength(0);
        return line;
    }

    /* **********************************************************************
     * **********************************************************************
     * ***** NEW_ORDER related methods and subclass. ************************
     * **********************************************************************
     * *********************************************************************/
    public void generateNewOrder(Logger log, jTPCCRandom rnd, long due) {
        int o_ol_cnt;
        int i = 0;

        transType = TT_NEW_ORDER;
        transDue = due;
        transStart = 0;
        transEnd = 0;
        transRbk = false;
        transError = null;

        newOrder = new NewOrderData();
        payment = null;
        orderStatus = null;
        stockLevel = null;
        delivery = null;
        deliveryBG = null;

        newOrder.w_id = terminalWarehouse; // 2.4.1.1
        newOrder.d_id = rnd.nextInt(1, 10); // 2.4.1.2
        newOrder.c_id = rnd.getCustomerID();
        o_ol_cnt = rnd.nextInt(5, 15); // 2.4.1.3

        while (i < o_ol_cnt) // 2.4.1.5
        {
            newOrder.ol_i_id[i] = rnd.getItemID();
            if (rnd.nextInt(1, 100) <= 99)
                newOrder.ol_supply_w_id[i] = terminalWarehouse;
            else
                newOrder.ol_supply_w_id[i] = rnd.nextInt(1, numWarehouses);
            newOrder.ol_quantity[i] = rnd.nextInt(1, 10);
            i++;
        }

        if (rnd.nextInt(1, 100) == 1) // 2.4.1.4
        {
            newOrder.ol_i_id[i - 1] += (rnd.nextInt(1, 9) * 1000000);
            transRbk = true;
        }

        // Zero out the remaining lines
        while (i < 15) {
            newOrder.ol_i_id[i] = 0;
            newOrder.ol_supply_w_id[i] = 0;
            newOrder.ol_quantity[i] = 0;
            i++;
        }
    }

    private void executeNewOrder(Logger log, jTPCCConnection db) throws Exception {
        PreparedStatement stmt;
        PreparedStatement insertOrderLineBatch;
        PreparedStatement updateStockBatch;
        ResultSet rs;

        int o_id;
        int o_all_local = 1;
        long o_entry_d;
        int ol_cnt;
        double total_amount = 0.0;

        int ol_seq[] = new int[15];

        // The o_entry_d is now.
        o_entry_d = System.currentTimeMillis();
        newOrder.o_entry_d = new java.sql.Timestamp(o_entry_d).toString();

        /*
         * When processing the order lines we must select the STOCK rows
         * FOR UPDATE. This is because we must perform business logic
         * (the juggling with the S_QUANTITY) here in the application
         * and cannot do that in an atomic UPDATE statement while getting
         * the original value back at the same time (UPDATE ... RETURNING
         * may not be vendor neutral). This can lead to possible deadlocks
         * if two transactions try to lock the same two stock rows in
         * opposite order. To avoid that we process the order lines in
         * the order of the order of ol_supply_w_id, ol_i_id.
         */
        for (ol_cnt = 0; ol_cnt < 15 && newOrder.ol_i_id[ol_cnt] != 0; ol_cnt++) {
            ol_seq[ol_cnt] = ol_cnt;

            // While looping we also determine o_all_local.
            if (newOrder.ol_supply_w_id[ol_cnt] != newOrder.w_id)
                o_all_local = 0;
        }

        for (int x = 0; x < ol_cnt - 1; x++) {
            for (int y = x + 1; y < ol_cnt; y++) {
                if (newOrder.ol_supply_w_id[ol_seq[y]] < newOrder.ol_supply_w_id[ol_seq[x]]) {
                    int tmp = ol_seq[x];
                    ol_seq[x] = ol_seq[y];
                    ol_seq[y] = tmp;
                } else if (newOrder.ol_supply_w_id[ol_seq[y]] == newOrder.ol_supply_w_id[ol_seq[x]]
                        && newOrder.ol_i_id[ol_seq[y]] < newOrder.ol_i_id[ol_seq[x]]) {
                    int tmp = ol_seq[x];
                    ol_seq[x] = ol_seq[y];
                    ol_seq[y] = tmp;
                }
            }
        }

        // The above also provided the output value for o_ol_cnt;
        newOrder.o_ol_cnt = ol_cnt;

        try {
            // Retrieve the required data from DISTRICT
            stmt = db.stmtNewOrderSelectDist;
            stmt.setInt(1, newOrder.w_id);
            stmt.setInt(2, newOrder.d_id);
            rs = stmt.executeQuery();
            if (!rs.next()) {
                rs.close();
                throw new SQLException(
                        "District for" + " W_ID=" + newOrder.w_id + " D_ID=" + newOrder.d_id + " not found");
            }
            newOrder.d_tax = rs.getDouble("d_tax");
            newOrder.o_id = rs.getInt("d_next_o_id");
            o_id = newOrder.o_id;
            rs.close();

            // Retrieve the required data from CUSTOMER and WAREHOUSE
            stmt = db.stmtNewOrderSelectWhseCust;
            stmt.setInt(1, newOrder.w_id);
            stmt.setInt(2, newOrder.d_id);
            stmt.setInt(3, newOrder.c_id);
            rs = stmt.executeQuery();
            if (!rs.next()) {
                rs.close();
                throw new SQLException("Warehouse or Customer for" + " W_ID=" + newOrder.w_id + " D_ID=" + newOrder.d_id
                        + " C_ID=" + newOrder.c_id + " not found");
            }
            newOrder.w_tax = rs.getDouble("w_tax");
            newOrder.c_last = rs.getString("c_last");
            newOrder.c_credit = rs.getString("c_credit");
            newOrder.c_discount = rs.getDouble("c_discount");
            rs.close();

            // Update the DISTRICT bumping the D_NEXT_O_ID
            stmt = db.stmtNewOrderUpdateDist;
            stmt.setInt(1, newOrder.w_id);
            stmt.setInt(2, newOrder.d_id);
            stmt.executeUpdate();

            // Insert the ORDER row
            stmt = db.stmtNewOrderInsertOrder;
            stmt.setInt(1, o_id);
            stmt.setInt(2, newOrder.d_id);
            stmt.setInt(3, newOrder.w_id);
            stmt.setInt(4, newOrder.c_id);
            stmt.setTimestamp(5, new java.sql.Timestamp(System.currentTimeMillis()));
            stmt.setInt(6, ol_cnt);
            stmt.setInt(7, o_all_local);
            stmt.executeUpdate();

            // Insert the NEW_ORDER row
            stmt = db.stmtNewOrderInsertNewOrder;
            stmt.setInt(1, o_id);
            stmt.setInt(2, newOrder.d_id);
            stmt.setInt(3, newOrder.w_id);
            stmt.executeUpdate();

            // Per ORDER_LINE
            insertOrderLineBatch = db.stmtNewOrderInsertOrderLine;
            updateStockBatch = db.stmtNewOrderUpdateStock;
            for (int i = 0; i < ol_cnt; i++) {
                int ol_number = i + 1;
                int seq = ol_seq[i];
                String i_data;

                stmt = db.stmtNewOrderSelectItem;
                stmt.setInt(1, newOrder.ol_i_id[seq]);
                rs = stmt.executeQuery();
                if (!rs.next()) {
                    rs.close();

                    /*
                     * 1% of NEW_ORDER transactions use an unused item
                     * in the last line to simulate user entry errors.
                     * Make sure this is precisely that case.
                     */
                    if (transRbk && (newOrder.ol_i_id[seq] < 1 || newOrder.ol_i_id[seq] > 100000)) {
                        /*
                         * Clause 2.4.2.3 mandates that the entire
                         * transaction profile up to here must be executed
                         * before we can roll back, except for retrieving
                         * the missing STOCK row and inserting this
                         * ORDER_LINE row. Note that we haven't updated
                         * STOCK rows or inserted any ORDER_LINE rows so
                         * far, we only batched them up. So we must do
                         * that now in order to satisfy 2.4.2.3.
                         */
                        insertOrderLineBatch.executeBatch();
                        insertOrderLineBatch.clearBatch();
                        updateStockBatch.executeBatch();
                        updateStockBatch.clearBatch();

                        db.rollback();

                        newOrder.total_amount = total_amount;
                        newOrder.execution_status = new String("Item number is not valid");
                        return;
                    }

                    // This ITEM should have been there.
                    throw new Exception("ITEM " + newOrder.ol_i_id[seq] + " not fount");
                }
                // Found ITEM
                newOrder.i_name[seq] = rs.getString("i_name");
                newOrder.i_price[seq] = rs.getDouble("i_price");
                i_data = rs.getString("i_data");
                rs.close();

                // Select STOCK for update.
                stmt = db.stmtNewOrderSelectStock;
                stmt.setInt(1, newOrder.ol_supply_w_id[seq]);
                stmt.setInt(2, newOrder.ol_i_id[seq]);
                rs = stmt.executeQuery();
                if (!rs.next()) {
                    throw new Exception("STOCK with" + " S_W_ID=" + newOrder.ol_supply_w_id[seq] + " S_I_ID="
                            + newOrder.ol_i_id[seq] + " not fount");
                }
                newOrder.s_quantity[seq] = rs.getInt("s_quantity");
                // Leave the ResultSet open ... we need it for the s_dist_NN.

                newOrder.ol_amount[seq] = newOrder.i_price[seq] * newOrder.ol_quantity[seq];
                if (i_data.contains("ORIGINAL") && rs.getString("s_data").contains("ORIGINAL"))
                    newOrder.brand_generic[seq] = new String("B");
                else
                    newOrder.brand_generic[seq] = new String("G");

                total_amount += newOrder.ol_amount[seq] * (1.0 - newOrder.c_discount)
                        * (1.0 + newOrder.w_tax + newOrder.d_tax);

                // Update the STOCK row.
                if (newOrder.s_quantity[seq] >= newOrder.ol_quantity[seq] + 10)
                    updateStockBatch.setInt(1, newOrder.s_quantity[seq] - newOrder.ol_quantity[seq]);
                else
                    updateStockBatch.setInt(1, newOrder.s_quantity[seq] + 91);
                updateStockBatch.setInt(2, newOrder.ol_quantity[seq]);
                if (newOrder.ol_supply_w_id[seq] == newOrder.w_id)
                    updateStockBatch.setInt(3, 0);
                else
                    updateStockBatch.setInt(3, 1);
                updateStockBatch.setInt(4, newOrder.ol_supply_w_id[seq]);
                updateStockBatch.setInt(5, newOrder.ol_i_id[seq]);
                updateStockBatch.addBatch();

                // Insert the ORDER_LINE row.
                insertOrderLineBatch.setInt(1, o_id);
                insertOrderLineBatch.setInt(2, newOrder.d_id);
                insertOrderLineBatch.setInt(3, newOrder.w_id);
                insertOrderLineBatch.setInt(4, seq + 1);
                insertOrderLineBatch.setInt(5, newOrder.ol_i_id[seq]);
                insertOrderLineBatch.setInt(6, newOrder.ol_supply_w_id[seq]);
                insertOrderLineBatch.setInt(7, newOrder.ol_quantity[seq]);
                insertOrderLineBatch.setDouble(8, newOrder.ol_amount[seq]);
                switch (newOrder.d_id) {
                case 1:
                    insertOrderLineBatch.setString(9, rs.getString("s_dist_01"));
                    break;
                case 2:
                    insertOrderLineBatch.setString(9, rs.getString("s_dist_02"));
                    break;
                case 3:
                    insertOrderLineBatch.setString(9, rs.getString("s_dist_03"));
                    break;
                case 4:
                    insertOrderLineBatch.setString(9, rs.getString("s_dist_04"));
                    break;
                case 5:
                    insertOrderLineBatch.setString(9, rs.getString("s_dist_05"));
                    break;
                case 6:
                    insertOrderLineBatch.setString(9, rs.getString("s_dist_06"));
                    break;
                case 7:
                    insertOrderLineBatch.setString(9, rs.getString("s_dist_07"));
                    break;
                case 8:
                    insertOrderLineBatch.setString(9, rs.getString("s_dist_08"));
                    break;
                case 9:
                    insertOrderLineBatch.setString(9, rs.getString("s_dist_09"));
                    break;
                case 10:
                    insertOrderLineBatch.setString(9, rs.getString("s_dist_10"));
                    break;
                }
                insertOrderLineBatch.addBatch();
            }
            rs.close();

            // All done ... execute the batches.
            updateStockBatch.executeBatch();
            updateStockBatch.clearBatch();
            insertOrderLineBatch.executeBatch();
            insertOrderLineBatch.clearBatch();

            newOrder.execution_status = new String("Order placed");
            newOrder.total_amount = total_amount;

            db.commit();

        } catch (SQLException se) {
            log.error("Unexpected SQLException in NEW_ORDER");
            for (SQLException x = se; x != null; x = x.getNextException())
                log.error(x.getMessage());
            se.printStackTrace();

            try {
                db.stmtNewOrderUpdateStock.clearBatch();
                db.stmtNewOrderInsertOrderLine.clearBatch();
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
        } catch (Exception e) {
            try {
                db.stmtNewOrderUpdateStock.clearBatch();
                db.stmtNewOrderInsertOrderLine.clearBatch();
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
            throw e;
        }
        /*
        log.info("Reached the point of creating one NEW_ORDER W_ID "+newOrder.w_id+" D_ID "+newOrder.d_id+" C_ID "+newOrder.c_id);
        System.exit(0);
        */
    }

    private void executeNewOrderStoredProcPostgres(Logger log, jTPCCConnection db) throws Exception {
        PreparedStatement stmt;
        ResultSet rs;
        Connection conn = db.getConnection();

        Integer[] ol_supply_w_id = new Integer[15];
        Integer[] ol_i_id = new Integer[15];
        Integer[] ol_quantity = new Integer[15];

        for (int i = 0; i < 15; i++) {
            ol_supply_w_id[i] = newOrder.ol_supply_w_id[i];
            ol_i_id[i] = newOrder.ol_i_id[i];
            ol_quantity[i] = newOrder.ol_quantity[i];
        }

        try {
            // Execute the stored procedure for NEW_ORDER
            stmt = db.stmtNewOrderStoredProc;
            stmt.setInt(1, newOrder.w_id);
            stmt.setInt(2, newOrder.d_id);
            stmt.setInt(3, newOrder.c_id);
            stmt.setArray(4, conn.createArrayOf("integer", ol_supply_w_id));
            stmt.setArray(5, conn.createArrayOf("integer", ol_i_id));
            stmt.setArray(6, conn.createArrayOf("integer", ol_quantity));
            rs = stmt.executeQuery();

            // The stored proc succeeded. Extract the results.
            rs.next();

            newOrder.w_tax = rs.getDouble("out_w_tax");
            newOrder.d_tax = rs.getDouble("out_d_tax");
            newOrder.o_id = rs.getInt("out_o_id");
            newOrder.o_entry_d = rs.getTimestamp("out_o_entry_d").toString();
            newOrder.o_ol_cnt = rs.getInt("out_ol_cnt");
            newOrder.total_amount = rs.getDouble("out_total_amount");
            newOrder.c_last = rs.getString("out_c_last");
            newOrder.c_credit = rs.getString("out_c_credit");
            newOrder.c_discount = rs.getDouble("out_c_discount");

            Array arr_ol_amount = rs.getArray("out_ol_amount");
            Array arr_i_name = rs.getArray("out_i_name");
            Array arr_i_price = rs.getArray("out_i_price");
            Array arr_s_quantity = rs.getArray("out_s_quantity");
            Array arr_bg = rs.getArray("out_brand_generic");
            BigDecimal[] ol_amount = (BigDecimal[]) arr_ol_amount.getArray();
            String[] i_name = (String[]) arr_i_name.getArray();
            BigDecimal[] i_price = (BigDecimal[]) arr_i_price.getArray();
            Integer[] s_quantity = (Integer[]) arr_s_quantity.getArray();
            String[] bg = (String[]) arr_bg.getArray();

            for (int i = 0; i < ol_amount.length; i++) {
                newOrder.ol_amount[i] = ol_amount[i].doubleValue();
                newOrder.i_name[i] = i_name[i];
                newOrder.i_price[i] = i_price[i].doubleValue();
                newOrder.s_quantity[i] = s_quantity[i];
                newOrder.brand_generic[i] = bg[i];
            }

            newOrder.execution_status = new String("Order placed");

            rs.close();
            db.commit();
        } catch (SQLException se) {
            if (transRbk && se.getMessage().startsWith("ERROR: Item number is not valid")) {
                newOrder.execution_status = new String("Item number is not valid");
            } else {
                log.error("Unexpected SQLException in NEW_ORDER");
                log.error("message: '" + se.getMessage() + "' transRbk=" + transRbk);
                for (SQLException x = se; x != null; x = x.getNextException())
                    log.error(x.getMessage());
                se.printStackTrace();
            }

            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
        } catch (Exception e) {
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
            throw e;
        }
        /*
        log.info("Reached the point of creating one NEW_ORDER W_ID "+newOrder.w_id+" D_ID "+newOrder.d_id+" C_ID "+newOrder.c_id);
        System.exit(0);
        */
    }

    private void executeNewOrderStoredProcOracle(Logger log, jTPCCConnection db) throws Exception {
        CallableStatement stmt;
        Connection conn = db.getConnection();

        try {
            // Execute the stored procedure for NEW_ORDER
            stmt = conn.prepareCall(db.stmtNewOrderStoredProcOracle);

            Integer[] ol_supply_w_id = new Integer[15];
            Integer[] ol_i_id = new Integer[15];
            Integer[] ol_quantity = new Integer[15];

            for (int i = 0; i < 15; i++) {
                ol_supply_w_id[i] = newOrder.ol_supply_w_id[i];
                ol_i_id[i] = newOrder.ol_i_id[i];
                ol_quantity[i] = newOrder.ol_quantity[i];
            }

            stmt.setInt(1, newOrder.w_id);
            stmt.setInt(2, newOrder.d_id);
            stmt.setInt(3, newOrder.c_id);
            stmt.setObject(4, conn.createArrayOf("integer", ol_supply_w_id));
            stmt.setObject(5, conn.createArrayOf("integer", ol_i_id));
            stmt.setObject(6, conn.createArrayOf("integer", ol_quantity));
            stmt.registerOutParameter(7, JDBCType.ARRAY, "NUM_ARRAY");
            stmt.registerOutParameter(8, JDBCType.ARRAY, "VARCHAR_ARRAY");
            stmt.registerOutParameter(9, JDBCType.ARRAY, "NUM_ARRAY");
            stmt.registerOutParameter(10, JDBCType.ARRAY, "INT_ARRAY");
            stmt.registerOutParameter(11, JDBCType.ARRAY, "CHAR_ARRAY");
            stmt.registerOutParameter(12, Types.DECIMAL);
            stmt.registerOutParameter(13, Types.DECIMAL);
            stmt.registerOutParameter(14, Types.INTEGER);
            stmt.registerOutParameter(15, Types.TIMESTAMP);
            stmt.registerOutParameter(16, Types.INTEGER);
            stmt.registerOutParameter(17, Types.DECIMAL);
            stmt.registerOutParameter(18, Types.VARCHAR);
            stmt.registerOutParameter(19, Types.VARCHAR);
            stmt.registerOutParameter(20, Types.DECIMAL);

            stmt.executeUpdate();

            newOrder.w_tax = stmt.getDouble(12);
            newOrder.d_tax = stmt.getDouble(13);
            newOrder.o_id = stmt.getInt(14);
            newOrder.o_entry_d = stmt.getTimestamp(15).toString();
            newOrder.o_ol_cnt = stmt.getInt(16);
            newOrder.total_amount = stmt.getDouble(17);
            newOrder.c_last = stmt.getString(18);
            newOrder.c_credit = stmt.getString(19);
            newOrder.c_discount = stmt.getDouble(20);

            double[] ol_amount_arr = (double[]) (stmt.getArray(7).getArray());
            String[] i_name_arr = (String[]) (stmt.getArray(8).getArray());
            double[] i_price_arr = (double[]) (stmt.getArray(9).getArray());
            int[] s_quantity_arr = (int[]) (stmt.getArray(10).getArray());
            String[] brand_generic_arr = (String[]) (stmt.getArray(11).getArray());

            for (int i = 0; i < 15; i++) {
                if (i < ol_amount_arr.length) {
                    newOrder.ol_amount[i] = ol_amount_arr[i];
                    newOrder.i_name[i] = i_name_arr[i];
                    newOrder.i_price[i] = i_price_arr[i];
                    newOrder.s_quantity[i] = s_quantity_arr[i];
                    newOrder.brand_generic[i] = brand_generic_arr[i];
                }
            }

            newOrder.execution_status = new String("Order placed");

            db.commit();
            stmt.close();

        } catch (SQLException se) {
            if (transRbk && se.getMessage().startsWith("ORA-20001: Item number is not valid")) {
                newOrder.execution_status = new String("Item number is not valid");
            } else {
                log.error("Unexpected SQLException in NEW_ORDER");
                log.error("message: '" + se.getMessage() + "' transRbk=" + transRbk);
                for (SQLException x = se; x != null; x = x.getNextException())
                    log.error(x.getMessage());
                se.printStackTrace();
            }

            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
        } catch (Exception e) {
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
            throw e;
        }
    }

    private void traceNewOrder(Logger log, Formatter fmt[]) {
        fmt[0].format("                                    New Order");

        if (transEnd == 0) {
            // NEW_ORDER INPUT screen
            fmt[1].format("Warehouse: %6d  District: %2d                       Date:", newOrder.w_id, newOrder.d_id);
            fmt[2].format("Customer:    %4d  Name:                    Credit:      %%Disc:", newOrder.c_id);
            fmt[3].format("Order Number:            Number of Lines:           W_tax:         D_tax:");

            fmt[5].format("Supp_W   Item_Id  Item Name                  Qty  Stock  B/G  Price    Amount");

            for (int i = 0; i < 15; i++) {
                if (newOrder.ol_i_id[i] != 0)
                    fmt[6 + i].format("%6d   %6d                              %2d", newOrder.ol_supply_w_id[i],
                            newOrder.ol_i_id[i], newOrder.ol_quantity[i]);
                else
                    fmt[6 + i].format("______   ______                              __");
            }

            fmt[21].format("Execution Status:                                             Total:  $");
        } else {
            // NEW_ORDER OUTPUT screen
            fmt[1].format("Warehouse: %6d  District: %2d                       Date: %19.19s", newOrder.w_id,
                    newOrder.d_id, newOrder.o_entry_d);
            fmt[2].format("Customer:    %4d  Name: %-16.16s   Credit: %2.2s   %%Disc: %5.2f", newOrder.c_id,
                    newOrder.c_last, newOrder.c_credit, newOrder.c_discount * 100.0);
            fmt[3].format("Order Number:  %8d  Number of Lines: %2d        W_tax: %5.2f   D_tax: %5.2f", newOrder.o_id,
                    newOrder.o_ol_cnt, newOrder.w_tax * 100.0, newOrder.d_tax * 100.0);

            fmt[5].format("Supp_W   Item_Id  Item Name                  Qty  Stock  B/G  Price    Amount");

            for (int i = 0; i < 15; i++) {
                if (newOrder.ol_i_id[i] != 0)
                    fmt[6 + i].format("%6d   %6d   %-24.24s   %2d    %3d    %1.1s   $%6.2f  $%7.2f",
                            newOrder.ol_supply_w_id[i], newOrder.ol_i_id[i], newOrder.i_name[i],
                            newOrder.ol_quantity[i], newOrder.s_quantity[i], newOrder.brand_generic[i],
                            newOrder.i_price[i], newOrder.ol_amount[i]);
            }

            fmt[21].format("Execution Status: %-24.24s                    Total:  $%8.2f", newOrder.execution_status,
                    newOrder.total_amount);
        }
    }

    private class NewOrderData {
        /* terminal input data */
        public int w_id;
        public int d_id;
        public int c_id;

        public int ol_supply_w_id[] = new int[15];
        public int ol_i_id[] = new int[15];
        public int ol_quantity[] = new int[15];

        /* terminal output data */
        public String c_last;
        public String c_credit;
        public double c_discount;
        public double w_tax;
        public double d_tax;
        public int o_ol_cnt;
        public int o_id;
        public String o_entry_d;
        public double total_amount;
        public String execution_status;

        public String i_name[] = new String[15];
        public int s_quantity[] = new int[15];
        public String brand_generic[] = new String[15];
        public double i_price[] = new double[15];
        public double ol_amount[] = new double[15];
    }

    /* **********************************************************************
     * **********************************************************************
     * ***** PAYMENT related methods and subclass. **************************
     * **********************************************************************
     * *********************************************************************/
    public void generatePayment(Logger log, jTPCCRandom rnd, long due) {
        transType = TT_PAYMENT;
        transDue = due;
        transStart = 0;
        transEnd = 0;
        transRbk = false;
        transError = null;

        newOrder = null;
        payment = new PaymentData();
        orderStatus = null;
        stockLevel = null;
        delivery = null;
        deliveryBG = null;

        payment.w_id = terminalWarehouse; // 2.5.1.1
        payment.d_id = rnd.nextInt(1, 10); // 2.5.1.2
        payment.c_w_id = payment.w_id;
        payment.c_d_id = payment.d_id;
        if (rnd.nextInt(1, 100) > 85) {
            payment.c_d_id = rnd.nextInt(1, 10);
            while (payment.c_w_id == payment.w_id && numWarehouses > 1)
                payment.c_w_id = rnd.nextInt(1, numWarehouses);
        }
        if (rnd.nextInt(1, 100) <= 60) {
            payment.c_last = rnd.getCLast();
            payment.c_id = 0;
        } else {
            payment.c_last = null;
            payment.c_id = rnd.getCustomerID();
        }

        // 2.5.1.3
        payment.h_amount = (rnd.nextLong(100, 500000)) / 100.0;
    }

    private void executePayment(Logger log, jTPCCConnection db) throws Exception {
        PreparedStatement stmt;
        ResultSet rs;
        Vector<Integer> c_id_list = new Vector<Integer>();

        long h_date = System.currentTimeMillis();

        try {
            // Update the DISTRICT.
            stmt = db.stmtPaymentUpdateDistrict;
            stmt.setDouble(1, payment.h_amount);
            stmt.setInt(2, payment.w_id);
            stmt.setInt(3, payment.d_id);
            stmt.executeUpdate();

            // Select the DISTRICT.
            stmt = db.stmtPaymentSelectDistrict;
            stmt.setInt(1, payment.w_id);
            stmt.setInt(2, payment.d_id);
            rs = stmt.executeQuery();
            if (!rs.next()) {
                rs.close();
                throw new Exception("District for" + " W_ID=" + payment.w_id + " D_ID=" + payment.d_id + " not found");
            }
            payment.d_name = rs.getString("d_name");
            payment.d_street_1 = rs.getString("d_street_1");
            payment.d_street_2 = rs.getString("d_street_2");
            payment.d_city = rs.getString("d_city");
            payment.d_state = rs.getString("d_state");
            payment.d_zip = rs.getString("d_zip");
            rs.close();

            // Update the WAREHOUSE.
            stmt = db.stmtPaymentUpdateWarehouse;
            stmt.setDouble(1, payment.h_amount);
            stmt.setInt(2, payment.w_id);
            stmt.executeUpdate();

            // Select the WAREHOUSE.
            stmt = db.stmtPaymentSelectWarehouse;
            stmt.setInt(1, payment.w_id);
            rs = stmt.executeQuery();
            if (!rs.next()) {
                rs.close();
                throw new Exception("Warehouse for" + " W_ID=" + payment.w_id + " not found");
            }
            payment.w_name = rs.getString("w_name");
            payment.w_street_1 = rs.getString("w_street_1");
            payment.w_street_2 = rs.getString("w_street_2");
            payment.w_city = rs.getString("w_city");
            payment.w_state = rs.getString("w_state");
            payment.w_zip = rs.getString("w_zip");
            rs.close();

            // If C_LAST is given instead of C_ID (60%), determine the C_ID.
            if (payment.c_last != null) {
                stmt = db.stmtPaymentSelectCustomerListByLast;
                stmt.setInt(1, payment.c_w_id);
                stmt.setInt(2, payment.c_d_id);
                stmt.setString(3, payment.c_last);
                rs = stmt.executeQuery();
                while (rs.next())
                    c_id_list.add(rs.getInt("c_id"));
                rs.close();

                if (c_id_list.size() == 0) {
                    throw new Exception("Customer(s) for" + " C_W_ID=" + payment.c_w_id + " C_D_ID=" + payment.c_d_id
                            + " C_LAST=" + payment.c_last + " not found");
                }

                payment.c_id = c_id_list.get((c_id_list.size() + 1) / 2 - 1);
            }

            // Select the CUSTOMER.
            stmt = db.stmtPaymentSelectCustomer;
            stmt.setInt(1, payment.c_w_id);
            stmt.setInt(2, payment.c_d_id);
            stmt.setInt(3, payment.c_id);
            rs = stmt.executeQuery();
            if (!rs.next()) {
                throw new Exception("Customer for" + " C_W_ID=" + payment.c_w_id + " C_D_ID=" + payment.c_d_id
                        + " C_ID=" + payment.c_id + " not found");
            }
            payment.c_first = rs.getString("c_first");
            payment.c_middle = rs.getString("c_middle");
            if (payment.c_last == null)
                payment.c_last = rs.getString("c_last");
            payment.c_street_1 = rs.getString("c_street_1");
            payment.c_street_2 = rs.getString("c_street_2");
            payment.c_city = rs.getString("c_city");
            payment.c_state = rs.getString("c_state");
            payment.c_zip = rs.getString("c_zip");
            payment.c_phone = rs.getString("c_phone");
            payment.c_since = rs.getTimestamp("c_since").toString();
            payment.c_credit = rs.getString("c_credit");
            payment.c_credit_lim = rs.getDouble("c_credit_lim");
            payment.c_discount = rs.getDouble("c_discount");
            payment.c_balance = rs.getDouble("c_balance");
            payment.c_data = new String("");
            rs.close();

            // Update the CUSTOMER.
            payment.c_balance -= payment.h_amount;
            if (payment.c_credit.equals("GC")) {
                // Customer with good credit, don't update C_DATA.
                stmt = db.stmtPaymentUpdateCustomer;
                stmt.setDouble(1, payment.h_amount);
                stmt.setDouble(2, payment.h_amount);
                stmt.setInt(3, payment.c_w_id);
                stmt.setInt(4, payment.c_d_id);
                stmt.setInt(5, payment.c_id);
                stmt.executeUpdate();
            } else {
                // Customer with bad credit, need to do the C_DATA work.
                stmt = db.stmtPaymentSelectCustomerData;
                stmt.setInt(1, payment.c_w_id);
                stmt.setInt(2, payment.c_d_id);
                stmt.setInt(3, payment.c_id);
                rs = stmt.executeQuery();
                if (!rs.next()) {
                    throw new Exception("Customer.c_data for" + " C_W_ID=" + payment.c_w_id + " C_D_ID="
                            + payment.c_d_id + " C_ID=" + payment.c_id + " not found");
                }
                payment.c_data = rs.getString("c_data");
                rs.close();

                stmt = db.stmtPaymentUpdateCustomerWithData;
                stmt.setDouble(1, payment.h_amount);
                stmt.setDouble(2, payment.h_amount);

                StringBuffer sbData = new StringBuffer();
                Formatter fmtData = new Formatter(sbData);
                fmtData.format("C_ID=%d C_D_ID=%d C_W_ID=%d " + "D_ID=%d W_ID=%d H_AMOUNT=%.2f   ", payment.c_id,
                        payment.c_d_id, payment.c_w_id, payment.d_id, payment.w_id, payment.h_amount);
                sbData.append(payment.c_data);
                if (sbData.length() > 500)
                    sbData.setLength(500);
                payment.c_data = sbData.toString();
                stmt.setString(3, payment.c_data);

                stmt.setInt(4, payment.c_w_id);
                stmt.setInt(5, payment.c_d_id);
                stmt.setInt(6, payment.c_id);
                stmt.executeUpdate();
            }

            // Insert the HISORY row.
            stmt = db.stmtPaymentInsertHistory;
            stmt.setInt(1, payment.c_id);
            stmt.setInt(2, payment.c_d_id);
            stmt.setInt(3, payment.c_w_id);
            stmt.setInt(4, payment.d_id);
            stmt.setInt(5, payment.w_id);
            stmt.setTimestamp(6, new java.sql.Timestamp(h_date));
            stmt.setDouble(7, payment.h_amount);
            stmt.setString(8, payment.w_name + "    " + payment.d_name);
            stmt.executeUpdate();

            payment.h_date = new java.sql.Timestamp(h_date).toString();

            db.commit();
        } catch (SQLException se) {
            log.error("Unexpected SQLException in PAYMENT");
            for (SQLException x = se; x != null; x = x.getNextException())
                log.error(x.getMessage());
            se.printStackTrace();

            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
        } catch (Exception e) {
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
            throw e;
        }
    }

    private void executePaymentStoredProcPostgres(Logger log, jTPCCConnection db) throws Exception {
        PreparedStatement stmt;
        ResultSet rs;
        Connection conn = db.getConnection();

        try {
            stmt = db.stmtPaymentStoredProc;
            stmt.setInt(1, payment.w_id);
            stmt.setInt(2, payment.d_id);
            stmt.setInt(3, payment.c_id);
            stmt.setInt(4, payment.c_d_id);
            stmt.setInt(5, payment.c_w_id);
            stmt.setString(6, payment.c_last);
            stmt.setBigDecimal(7, new BigDecimal(payment.h_amount));
            rs = stmt.executeQuery();

            // The stored proc succeeded. Extract the results.
            rs.next();

            payment.w_name = rs.getString("out_w_name");
            payment.w_street_1 = rs.getString("out_w_street_1");
            payment.w_street_2 = rs.getString("out_w_street_2");
            payment.w_city = rs.getString("out_w_city");
            payment.w_state = rs.getString("out_w_state");
            payment.w_zip = rs.getString("out_w_zip");
            payment.d_name = rs.getString("out_d_name");
            payment.d_street_1 = rs.getString("out_d_street_1");
            payment.d_street_2 = rs.getString("out_d_street_2");
            payment.d_city = rs.getString("out_d_city");
            payment.d_state = rs.getString("out_d_state");
            payment.d_zip = rs.getString("out_d_zip");
            payment.c_id = rs.getInt("in_c_id");
            payment.c_first = rs.getString("out_c_first");
            payment.c_middle = rs.getString("out_c_middle");
            payment.c_street_1 = rs.getString("out_c_street_1");
            payment.c_street_2 = rs.getString("out_c_street_2");
            payment.c_city = rs.getString("out_c_city");
            payment.c_state = rs.getString("out_c_state");
            payment.c_zip = rs.getString("out_c_zip");
            payment.c_phone = rs.getString("out_c_phone");
            payment.c_since = rs.getTimestamp("out_c_since").toString();
            payment.c_credit = rs.getString("out_c_credit");
            payment.c_credit_lim = rs.getDouble("out_c_credit_lim");
            payment.c_discount = rs.getDouble("out_c_discount");
            payment.c_balance = rs.getDouble("out_c_balance");
            payment.c_data = rs.getString("out_c_data");
            payment.h_date = rs.getTimestamp("out_h_date").toString();

            rs.close();
            db.commit();
        } catch (SQLException se) {
            log.error("Unexpected SQLException in PAYMENT");
            log.error("message: '" + se.getMessage() + "' transRbk=" + transRbk);
            for (SQLException x = se; x != null; x = x.getNextException())
                log.error(x.getMessage());
            se.printStackTrace();
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
        } catch (Exception e) {
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
            throw e;
        }
    }

    private void executePaymentStoredProcOracle(Logger log, jTPCCConnection db) throws Exception {
        CallableStatement stmt;
        Connection conn = db.getConnection();

        try {
            // Execute the stored procedure for STOCK_LEVEL
            stmt = conn.prepareCall(db.stmtPaymentStoredProcOracle);

            stmt.setInt(1, payment.w_id);
            stmt.setInt(2, payment.d_id);
            stmt.setInt(3, payment.c_id);
            stmt.setInt(4, payment.c_d_id);
            stmt.setInt(5, payment.c_w_id);
            stmt.setString(6, payment.c_last);
            stmt.setBigDecimal(7, new BigDecimal(payment.h_amount));
            stmt.registerOutParameter(3, Types.INTEGER);
            stmt.registerOutParameter(6, Types.VARCHAR);
            stmt.registerOutParameter(8, Types.VARCHAR);
            stmt.registerOutParameter(9, Types.VARCHAR);
            stmt.registerOutParameter(10, Types.VARCHAR);
            stmt.registerOutParameter(11, Types.VARCHAR);
            stmt.registerOutParameter(12, Types.VARCHAR);
            stmt.registerOutParameter(13, Types.VARCHAR);
            stmt.registerOutParameter(14, Types.VARCHAR);
            stmt.registerOutParameter(15, Types.VARCHAR);
            stmt.registerOutParameter(16, Types.VARCHAR);
            stmt.registerOutParameter(17, Types.VARCHAR);
            stmt.registerOutParameter(18, Types.VARCHAR);
            stmt.registerOutParameter(19, Types.VARCHAR);
            stmt.registerOutParameter(20, Types.VARCHAR);
            stmt.registerOutParameter(21, Types.VARCHAR);
            stmt.registerOutParameter(22, Types.VARCHAR);
            stmt.registerOutParameter(23, Types.VARCHAR);
            stmt.registerOutParameter(24, Types.VARCHAR);
            stmt.registerOutParameter(25, Types.VARCHAR);
            stmt.registerOutParameter(26, Types.VARCHAR);
            stmt.registerOutParameter(27, Types.VARCHAR);
            stmt.registerOutParameter(28, Types.TIMESTAMP);
            stmt.registerOutParameter(29, Types.VARCHAR);
            stmt.registerOutParameter(30, Types.DECIMAL);
            stmt.registerOutParameter(31, Types.DECIMAL);
            stmt.registerOutParameter(32, Types.DECIMAL);
            stmt.registerOutParameter(33, Types.VARCHAR);
            stmt.registerOutParameter(34, Types.TIMESTAMP);

            stmt.executeUpdate();

            // The stored proc succeded. Extract the results.
            payment.c_id = stmt.getInt(3);
            payment.c_last = stmt.getString(6);
            payment.w_name = stmt.getString(8);
            payment.w_street_1 = stmt.getString(9);
            payment.w_street_2 = stmt.getString(10);
            payment.w_city = stmt.getString(11);
            payment.w_state = stmt.getString(12);
            payment.w_zip = stmt.getString(13);
            payment.d_name = stmt.getString(14);
            payment.d_street_1 = stmt.getString(15);
            payment.d_street_2 = stmt.getString(16);
            payment.d_city = stmt.getString(17);
            payment.d_state = stmt.getString(18);
            payment.d_zip = stmt.getString(19);
            payment.c_first = stmt.getString(20);
            payment.c_middle = stmt.getString(21);
            payment.c_street_1 = stmt.getString(22);
            payment.c_street_2 = stmt.getString(23);
            payment.c_city = stmt.getString(24);
            payment.c_state = stmt.getString(25);
            payment.c_zip = stmt.getString(26);
            payment.c_phone = stmt.getString(27);
            payment.c_since = stmt.getTimestamp(28).toString();
            payment.c_credit = stmt.getString(29);
            payment.c_credit_lim = stmt.getDouble(30);
            payment.c_discount = stmt.getDouble(31);
            payment.c_balance = stmt.getDouble(32);
            payment.c_data = stmt.getString(33);
            payment.h_date = stmt.getTimestamp(34).toString();

            db.commit();
            stmt.close();

        } catch (SQLException se) {
            log.error("Unexpected SQLException in PAYMENT");
            log.error("message: '" + se.getMessage() + "' transRbk=" + transRbk);
            for (SQLException x = se; x != null; x = x.getNextException())
                log.error(x.getMessage());
            se.printStackTrace();

            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
        } catch (Exception e) {
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
            throw e;
        }
    }

    private void tracePayment(Logger log, Formatter fmt[]) {
        fmt[0].format("                                     Payment");

        if (transEnd == 0) {
            // PAYMENT INPUT screen
            fmt[1].format("Date: ");
            fmt[3].format("Warehouse: %6d                         District: %2d", payment.w_id, payment.d_id);

            if (payment.c_last == null) {
                fmt[8].format("Customer: %4d  Cust-Warehouse: %6d  Cust-District: %2d", payment.c_id, payment.c_w_id,
                        payment.c_d_id);
                fmt[9].format("Name:                       ________________       Since:");
            } else {
                fmt[8].format("Customer: ____  Cust-Warehouse: %6d  Cust-District: %2d", payment.c_w_id,
                        payment.c_d_id);
                fmt[9].format("Name:                       %-16.16s       Since:", payment.c_last);
            }
            fmt[10].format("                                                   Credit:");
            fmt[11].format("                                                   %%Disc:");
            fmt[12].format("                                                   Phone:");

            fmt[14].format("Amount Paid:          $%7.2f        New Cust-Balance:", payment.h_amount);
            fmt[15].format("Credit Limit:");
            fmt[17].format("Cust-Data:");
        } else {
            // PAYMENT OUTPUT screen
            fmt[1].format("Date: %-19.19s", payment.h_date);
            fmt[3].format("Warehouse: %6d                         District: %2d", payment.w_id, payment.d_id);
            fmt[4].format("%-20.20s                      %-20.20s", payment.w_street_1, payment.d_street_1);
            fmt[5].format("%-20.20s                      %-20.20s", payment.w_street_2, payment.d_street_2);
            fmt[6].format("%-20.20s %2.2s %5.5s-%4.4s        %-20.20s %2.2s %5.5s-%4.4s", payment.w_city,
                    payment.w_state, payment.w_zip.substring(0, 5), payment.w_zip.substring(5, 9), payment.d_city,
                    payment.d_state, payment.d_zip.substring(0, 5), payment.d_zip.substring(5, 9));

            fmt[8].format("Customer: %4d  Cust-Warehouse: %6d  Cust-District: %2d", payment.c_id, payment.c_w_id,
                    payment.c_d_id);
            fmt[9].format("Name:   %-16.16s %2.2s %-16.16s       Since:  %-10.10s", payment.c_first, payment.c_middle,
                    payment.c_last, payment.c_since);
            fmt[10].format("        %-20.20s                       Credit: %2s", payment.c_street_1, payment.c_credit);
            fmt[11].format("        %-20.20s                       %%Disc:  %5.2f", payment.c_street_2,
                    payment.c_discount * 100.0);
            fmt[12].format("        %-20.20s %2.2s %5.5s-%4.4s         Phone:  %6.6s-%3.3s-%3.3s-%4.4s", payment.c_city,
                    payment.c_state, payment.c_zip.substring(0, 5), payment.c_zip.substring(5, 9),
                    payment.c_phone.substring(0, 6), payment.c_phone.substring(6, 9), payment.c_phone.substring(9, 12),
                    payment.c_phone.substring(12, 16));

            fmt[14].format("Amount Paid:          $%7.2f        New Cust-Balance: $%14.2f", payment.h_amount,
                    payment.c_balance);
            fmt[15].format("Credit Limit:   $%13.2f", payment.c_credit_lim);
            if (payment.c_data.length() >= 200) {
                fmt[17].format("Cust-Data: %-50.50s", payment.c_data.substring(0, 50));
                fmt[18].format("           %-50.50s", payment.c_data.substring(50, 100));
                fmt[19].format("           %-50.50s", payment.c_data.substring(100, 150));
                fmt[20].format("           %-50.50s", payment.c_data.substring(150, 200));
            } else {
                fmt[17].format("Cust-Data:");
            }
        }
    }

    private class PaymentData {
        /* terminal input data */
        public int w_id;
        public int d_id;
        public int c_id;
        public int c_d_id;
        public int c_w_id;
        public String c_last;
        public double h_amount;

        /* terminal output data */
        public String w_name;
        public String w_street_1;
        public String w_street_2;
        public String w_city;
        public String w_state;
        public String w_zip;
        public String d_name;
        public String d_street_1;
        public String d_street_2;
        public String d_city;
        public String d_state;
        public String d_zip;
        public String c_first;
        public String c_middle;
        public String c_street_1;
        public String c_street_2;
        public String c_city;
        public String c_state;
        public String c_zip;
        public String c_phone;
        public String c_since;
        public String c_credit;
        public double c_credit_lim;
        public double c_discount;
        public double c_balance;
        public String c_data;
        public String h_date;
    }

    /* **********************************************************************
     * **********************************************************************
     * ***** ORDER_STATUS related methods and subclass. *********************
     * **********************************************************************
     * *********************************************************************/
    public void generateOrderStatus(Logger log, jTPCCRandom rnd, long due) {
        transType = TT_ORDER_STATUS;
        transDue = due;
        transStart = 0;
        transEnd = 0;
        transRbk = false;
        transError = null;

        newOrder = null;
        payment = null;
        orderStatus = new OrderStatusData();
        stockLevel = null;
        delivery = null;
        deliveryBG = null;

        orderStatus.w_id = terminalWarehouse;
        orderStatus.d_id = rnd.nextInt(1, 10);
        if (rnd.nextInt(1, 100) <= 60) {
            orderStatus.c_id = 0;
            orderStatus.c_last = rnd.getCLast();
        } else {
            orderStatus.c_id = rnd.getCustomerID();
            orderStatus.c_last = null;
        }
    }

    private void executeOrderStatus(Logger log, jTPCCConnection db) throws Exception {
        PreparedStatement stmt;
        ResultSet rs;
        Vector<Integer> c_id_list = new Vector<Integer>();
        int ol_idx = 0;

        try {
            // If C_LAST is given instead of C_ID (60%), determine the C_ID.
            if (orderStatus.c_last != null) {
                stmt = db.stmtOrderStatusSelectCustomerListByLast;
                stmt.setInt(1, orderStatus.w_id);
                stmt.setInt(2, orderStatus.d_id);
                stmt.setString(3, orderStatus.c_last);
                rs = stmt.executeQuery();
                while (rs.next())
                    c_id_list.add(rs.getInt("c_id"));
                rs.close();

                if (c_id_list.size() == 0) {
                    throw new Exception("Customer(s) for" + " C_W_ID=" + orderStatus.w_id + " C_D_ID="
                            + orderStatus.d_id + " C_LAST=" + orderStatus.c_last + " not found");
                }

                orderStatus.c_id = c_id_list.get((c_id_list.size() + 1) / 2 - 1);
            }

            // Select the CUSTOMER.
            stmt = db.stmtOrderStatusSelectCustomer;
            stmt.setInt(1, orderStatus.w_id);
            stmt.setInt(2, orderStatus.d_id);
            stmt.setInt(3, orderStatus.c_id);
            rs = stmt.executeQuery();
            if (!rs.next()) {
                throw new Exception("Customer for" + " C_W_ID=" + orderStatus.w_id + " C_D_ID=" + orderStatus.d_id
                        + " C_ID=" + orderStatus.c_id + " not found");
            }
            orderStatus.c_first = rs.getString("c_first");
            orderStatus.c_middle = rs.getString("c_middle");
            if (orderStatus.c_last == null)
                orderStatus.c_last = rs.getString("c_last");
            orderStatus.c_balance = rs.getDouble("c_balance");
            rs.close();

            // Select the last ORDER for this customer.
            stmt = db.stmtOrderStatusSelectLastOrder;
            stmt.setInt(1, orderStatus.w_id);
            stmt.setInt(2, orderStatus.d_id);
            stmt.setInt(3, orderStatus.c_id);
            stmt.setInt(4, orderStatus.w_id);
            stmt.setInt(5, orderStatus.d_id);
            stmt.setInt(6, orderStatus.c_id);
            rs = stmt.executeQuery();
            if (!rs.next()) {
                throw new Exception("Last Order for" + " W_ID=" + orderStatus.w_id + " D_ID=" + orderStatus.d_id
                        + " C_ID=" + orderStatus.c_id + " not found");
            }
            orderStatus.o_id = rs.getInt("o_id");
            orderStatus.o_entry_d = rs.getTimestamp("o_entry_d").toString();
            orderStatus.o_carrier_id = rs.getInt("o_carrier_id");
            if (rs.wasNull())
                orderStatus.o_carrier_id = -1;
            rs.close();

            stmt = db.stmtOrderStatusSelectOrderLine;
            stmt.setInt(1, orderStatus.w_id);
            stmt.setInt(2, orderStatus.d_id);
            stmt.setInt(3, orderStatus.o_id);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Timestamp ol_delivery_d;

                orderStatus.ol_i_id[ol_idx] = rs.getInt("ol_i_id");
                orderStatus.ol_supply_w_id[ol_idx] = rs.getInt("ol_supply_w_id");
                orderStatus.ol_quantity[ol_idx] = rs.getInt("ol_quantity");
                orderStatus.ol_amount[ol_idx] = rs.getDouble("ol_amount");
                ol_delivery_d = rs.getTimestamp("ol_delivery_d");
                if (ol_delivery_d != null)
                    orderStatus.ol_delivery_d[ol_idx] = ol_delivery_d.toString();
                else
                    orderStatus.ol_delivery_d[ol_idx] = null;
                ol_idx++;
            }
            rs.close();

            while (ol_idx < 15) {
                orderStatus.ol_i_id[ol_idx] = 0;
                orderStatus.ol_supply_w_id[ol_idx] = 0;
                orderStatus.ol_quantity[ol_idx] = 0;
                orderStatus.ol_amount[ol_idx] = 0.0;
                orderStatus.ol_delivery_d[ol_idx] = null;
                ol_idx++;
            }

            db.commit();
        } catch (SQLException se) {
            log.error("Unexpected SQLException in ORDER_STATUS");
            for (SQLException x = se; x != null; x = x.getNextException())
                log.error(x.getMessage());
            se.printStackTrace();

            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
        } catch (Exception e) {
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
            throw e;
        }
    }

    private void executeOrderStatusStoredProcPostgres(Logger log, jTPCCConnection db) throws Exception {
        PreparedStatement stmt;
        ResultSet rs;
        Connection conn = db.getConnection();

        try {
            // Execute the stored procedure for ORDER_STATUS
            stmt = db.stmtOrderStatusStoredProc;
            stmt.setInt(1, orderStatus.w_id);
            stmt.setInt(2, orderStatus.d_id);
            stmt.setInt(3, orderStatus.c_id);
            stmt.setString(4, orderStatus.c_last);
            rs = stmt.executeQuery();

            // The stored proc succeeded Extract the results.
            rs.next();

            orderStatus.c_first = rs.getString("out_c_first");
            orderStatus.c_middle = rs.getString("out_c_middle");
            orderStatus.c_balance = rs.getDouble("out_c_balance");
            orderStatus.o_id = rs.getInt("out_o_id");
            orderStatus.o_entry_d = rs.getTimestamp("out_o_entry_d").toString();
            orderStatus.o_carrier_id = rs.getInt("out_o_carrier_id");
            orderStatus.c_id = rs.getInt("in_c_id");
            Array arr_ol_supply_w_id = rs.getArray("out_ol_supply_w_id");
            Array arr_ol_i_id = rs.getArray("out_ol_i_id");
            Array arr_ol_quantity = rs.getArray("out_ol_quantity");
            Array arr_ol_amount = rs.getArray("out_ol_amount");
            Array arr_ol_delivery_d = rs.getArray("out_ol_delivery_d");
            Integer[] ol_i_id = (Integer[]) arr_ol_i_id.getArray();
            Integer[] ol_supply_w_id = (Integer[]) arr_ol_supply_w_id.getArray();
            Integer[] ol_quantity = (Integer[]) arr_ol_quantity.getArray();
            BigDecimal[] ol_amount = (BigDecimal[]) arr_ol_amount.getArray();
            Timestamp[] ol_delivery_x = (Timestamp[]) arr_ol_delivery_d.getArray();

            for (int i = 0; i < ol_amount.length; i++) {
                orderStatus.ol_supply_w_id[i] = ol_supply_w_id[i];
                orderStatus.ol_i_id[i] = ol_i_id[i];
                orderStatus.ol_quantity[i] = ol_quantity[i];
                orderStatus.ol_amount[i] = ol_amount[i].doubleValue();
                if (ol_delivery_x[i] == null)
                    orderStatus.ol_delivery_d[i] = " ";
                else
                    orderStatus.ol_delivery_d[i] = ol_delivery_x[i].toString();
            }

            rs.close();
            db.commit();

        } catch (SQLException se) {
            log.error("Unexpected SQLException in ORDER_STATUS");
            log.error("message: '" + se.getMessage() + "' transRbk=" + transRbk);
            for (SQLException x = se; x != null; x = x.getNextException())
                log.error(x.getMessage());
            se.printStackTrace();

            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
        } catch (Exception e) {
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
            throw e;
        }
    }

    private void executeOrderStatusStoredProcOracle(Logger log, jTPCCConnection db) throws Exception {
        CallableStatement stmt;
        Connection conn = db.getConnection();
        try {
            // Execute the stored procedure for ORDER_STATUS
            stmt = conn.prepareCall(db.stmtOrderStatusStoredProcOracle);
            stmt.setInt(1, orderStatus.w_id);
            stmt.setInt(2, orderStatus.d_id);
            stmt.setInt(3, orderStatus.c_id);
            stmt.setString(4, orderStatus.c_last);
            stmt.registerOutParameter(3, Types.INTEGER);
            stmt.registerOutParameter(4, Types.VARCHAR);
            stmt.registerOutParameter(5, Types.VARCHAR);
            stmt.registerOutParameter(6, Types.VARCHAR);
            stmt.registerOutParameter(7, Types.DECIMAL);
            stmt.registerOutParameter(8, Types.INTEGER);
            stmt.registerOutParameter(9, Types.TIMESTAMP);
            stmt.registerOutParameter(10, Types.INTEGER);
            stmt.registerOutParameter(11, JDBCType.ARRAY, "INT_ARRAY");
            stmt.registerOutParameter(12, JDBCType.ARRAY, "INT_ARRAY");
            stmt.registerOutParameter(13, JDBCType.ARRAY, "INT_ARRAY");
            stmt.registerOutParameter(14, JDBCType.ARRAY, "NUM_ARRAY");
            stmt.registerOutParameter(15, JDBCType.ARRAY, "VARCHAR16_ARRAY");

            stmt.executeUpdate();

            // The stored proc succeeded. Extract the results.
            orderStatus.c_id = stmt.getInt(3);
            orderStatus.c_last = stmt.getString(4);
            orderStatus.c_first = stmt.getString(5);
            orderStatus.c_middle = stmt.getString(6);
            orderStatus.c_balance = stmt.getDouble(7);
            orderStatus.o_id = stmt.getInt(8);
            orderStatus.o_entry_d = stmt.getTimestamp(9).toString();
            orderStatus.o_carrier_id = stmt.getInt(10);

            int[] supply_w_id_arr = (int[]) (stmt.getArray(11).getArray());
            int[] i_id_arr = (int[]) (stmt.getArray(12).getArray());
            int[] quantity_arr = (int[]) (stmt.getArray(13).getArray());
            double[] amount_arr = (double[]) (stmt.getArray(14).getArray());
            String[] delivery_d = (String[]) (stmt.getArray(15).getArray());

            for (int i = 0; i < amount_arr.length; i++) {
                orderStatus.ol_supply_w_id[i] = supply_w_id_arr[i];
                orderStatus.ol_i_id[i] = i_id_arr[i];
                orderStatus.ol_quantity[i] = quantity_arr[i];
                orderStatus.ol_amount[i] = amount_arr[i];
                if (delivery_d[i] == null)
                    orderStatus.ol_delivery_d[i] = " ";
                else
                    orderStatus.ol_delivery_d[i] = delivery_d[i];
            }

            db.commit();
            stmt.close();

        } catch (SQLException se) {
            log.error("Unexpected SQLException in ORDER_STATUS");
            log.error("message: '" + se.getMessage() + "' transRbk=" + transRbk);
            for (SQLException x = se; x != null; x = x.getNextException())
                log.error(x.getMessage());
            se.printStackTrace();

            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
        } catch (Exception e) {
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
            throw e;
        }
    }

    private void traceOrderStatus(Logger log, Formatter fmt[]) {
        fmt[0].format("                                  Order Status");

        if (transEnd == 0) {
            // ORDER_STATUS INPUT screen
            fmt[1].format("Warehouse: %6d   District: %2d", orderStatus.w_id, orderStatus.d_id);
            if (orderStatus.c_last == null)
                fmt[2].format("Customer: %4d   Name:                     ________________", orderStatus.c_id);
            else
                fmt[2].format("Customer: ____   Name:                     %-16.16s", orderStatus.c_last);
            fmt[3].format("Cust-Balance:");

            fmt[5].format("Order-Number:            Entry-Date:                       Carrier-Number:");
            fmt[6].format("Suppy-W      Item-Id     Qty    Amount        Delivery-Date");
        } else {
            // ORDER_STATUS OUTPUT screen
            fmt[1].format("Warehouse: %6d   District: %2d", orderStatus.w_id, orderStatus.d_id);
            fmt[2].format("Customer: %4d   Name: %-16.16s %2.2s %-16.16s", orderStatus.c_id, orderStatus.c_first,
                    orderStatus.c_middle, orderStatus.c_last);
            fmt[3].format("Cust-Balance: $%13.2f", orderStatus.c_balance);

            if (orderStatus.o_carrier_id >= 0)
                fmt[5].format("Order-Number: %8d   Entry-Date: %-19.19s   Carrier-Number: %2d", orderStatus.o_id,
                        orderStatus.o_entry_d, orderStatus.o_carrier_id);
            else
                fmt[5].format("Order-Number: %8d   Entry-Date: %-19.19s   Carrier-Number:", orderStatus.o_id,
                        orderStatus.o_entry_d);
            fmt[6].format("Suppy-W      Item-Id     Qty    Amount        Delivery-Date");
            for (int i = 0; i < 15 && orderStatus.ol_i_id[i] > 0; i++) {
                fmt[7 + i].format(" %6d      %6d     %3d     $%8.2f     %-10.10s", orderStatus.ol_supply_w_id[i],
                        orderStatus.ol_i_id[i], orderStatus.ol_quantity[i], orderStatus.ol_amount[i],
                        (orderStatus.ol_delivery_d[i] == null) ? "" : orderStatus.ol_delivery_d[i]);
            }
        }
    }

    private class OrderStatusData {
        /* terminal input data */
        public int w_id;
        public int d_id;
        public int c_id;
        public String c_last;

        /* terminal output data */
        public String c_first;
        public String c_middle;
        public double c_balance;
        public int o_id;
        public String o_entry_d;
        public int o_carrier_id;

        public int ol_supply_w_id[] = new int[15];
        public int ol_i_id[] = new int[15];
        public int ol_quantity[] = new int[15];
        public double ol_amount[] = new double[15];
        public String ol_delivery_d[] = new String[15];
    }

    /* **********************************************************************
     * **********************************************************************
     * ***** STOCK_LEVEL related methods and subclass. **********************
     * **********************************************************************
     * *********************************************************************/
    public void generateStockLevel(Logger log, jTPCCRandom rnd, long due) {
        transType = TT_STOCK_LEVEL;
        transDue = due;
        transStart = 0;
        transEnd = 0;
        transRbk = false;
        transError = null;

        newOrder = null;
        payment = null;
        orderStatus = null;
        stockLevel = new StockLevelData();
        delivery = null;
        deliveryBG = null;

        stockLevel.w_id = terminalWarehouse;
        stockLevel.d_id = terminalDistrict;
        stockLevel.threshold = rnd.nextInt(10, 20);
    }

    private void executeStockLevel(Logger log, jTPCCConnection db) throws Exception {
        PreparedStatement stmt;
        ResultSet rs;

        try {
            stmt = db.stmtStockLevelSelectLow;
            stmt.setInt(1, stockLevel.w_id);
            stmt.setInt(2, stockLevel.threshold);
            stmt.setInt(3, stockLevel.w_id);
            stmt.setInt(4, stockLevel.d_id);
            rs = stmt.executeQuery();
            if (!rs.next()) {
                throw new Exception(
                        "Failed to get low-stock for" + " W_ID=" + stockLevel.w_id + " D_ID=" + stockLevel.d_id);
            }
            stockLevel.low_stock = rs.getInt("low_stock");
            rs.close();

            db.commit();
        } catch (SQLException se) {
            log.error("Unexpected SQLException in STOCK_LEVEL");
            for (SQLException x = se; x != null; x = x.getNextException())
                log.error(x.getMessage());
            se.printStackTrace();

            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
        } catch (Exception e) {
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
            throw e;
        }
    }

    private void executeStockLevelStoredProcPostgres(Logger log, jTPCCConnection db) throws Exception {
        PreparedStatement stmt;
        ResultSet rs;
        Connection conn = db.getConnection();

        try {
            // Execute the stored procedure for STOCK_LEVEL
            stmt = db.stmtStockLevelStoredProc;
            stmt.setInt(1, stockLevel.w_id);
            stmt.setInt(2, stockLevel.d_id);
            stmt.setInt(3, stockLevel.threshold);
            rs = stmt.executeQuery();

            // The stored proc succeeded. Extract the results.
            rs.next();

            stockLevel.low_stock = rs.getInt("out_low_stock");

            rs.close();
            db.commit();
        } catch (SQLException se) {

            log.error("Unexpected SQLException in NEW_ORDER");
            log.error("message: '" + se.getMessage() + "' transRbk=" + transRbk);
            for (SQLException x = se; x != null; x = x.getNextException())
                log.error(x.getMessage());
            se.printStackTrace();

            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
        } catch (Exception e) {
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
            throw e;
        }
    }

    private void executeStockLevelStoredProcOracle(Logger log, jTPCCConnection db) throws Exception {
        Connection conn = db.getConnection();
        CallableStatement stmt;
        try {

            // Execute the stored procedure for STOCK_LEVEL
            stmt = conn.prepareCall(db.stmtStockLevelStoredProcOracle);
            stmt.setInt(1, stockLevel.w_id);
            stmt.setInt(2, stockLevel.d_id);
            stmt.setInt(3, stockLevel.threshold);
            stmt.registerOutParameter(4, Types.INTEGER);

            stmt.executeUpdate();

            // The stored proc succeeded. Extract the results.
            stockLevel.low_stock = stmt.getInt(4);

            db.commit();
            stmt.close();

        } catch (SQLException se) {
            log.error("Unexpected SQLException in NEW_ORDER");
            log.error("message: '" + se.getMessage() + "' transRbk=" + transRbk);
            for (SQLException x = se; x != null; x = x.getNextException())
                log.error(x.getMessage());
            se.printStackTrace();

            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
        } catch (Exception e) {
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
            throw e;
        }

    }

    private void traceStockLevel(Logger log, Formatter fmt[]) {
        fmt[0].format("                                  Stock-Level");

        fmt[1].format("Warehouse: %6d   District: %2d", stockLevel.w_id, stockLevel.d_id);
        fmt[3].format("Stock Level Threshold: %2d", stockLevel.threshold);

        if (transEnd == 0)
            fmt[5].format("Low Stock:");
        else
            fmt[5].format("Low Stock: %3d", stockLevel.low_stock);
    }

    private class StockLevelData {
        /* terminal input data */
        public int w_id;
        public int d_id;
        public int threshold;

        /* terminal output data */
        public int low_stock;
    }

    /* **********************************************************************
     * **********************************************************************
     * ***** DELIVERY related methods and subclass. *************************
     * **********************************************************************
     * *********************************************************************/
    public void generateDelivery(Logger log, jTPCCRandom rnd, long due) {
        transType = TT_DELIVERY;
        transDue = due;
        transStart = 0;
        transEnd = 0;
        transRbk = false;
        transError = null;

        newOrder = null;
        payment = null;
        orderStatus = null;
        stockLevel = null;
        delivery = new DeliveryData();
        deliveryBG = null;

        delivery.w_id = terminalWarehouse;
        delivery.o_carrier_id = rnd.nextInt(1, 10);
        delivery.execution_status = null;
        delivery.deliveryBG = null;
    }

    private void executeDelivery(Logger log, jTPCCConnection db) {
        long now = System.currentTimeMillis();

        /*
         * The DELIVERY transaction is different from all the others.
         * The foreground transaction, experienced by the user, does
         * not perform any interaction with the database. It only queues
         * a request to perform such a transaction in the background
         * (DeliveryBG). We store that TData object in the delivery
         * part for the caller to pick up and queue/execute.
         */
        delivery.deliveryBG = new jTPCCTData();
        delivery.deliveryBG.generateDeliveryBG(delivery.w_id, now, new java.sql.Timestamp(now).toString(), this);
        delivery.deliveryBG.setDBType(dbType);
        delivery.deliveryBG.setUseStoredProcedures(useStoredProcedures);
        delivery.execution_status = new String("Delivery has been queued");
    }

    private void traceDelivery(Logger log, Formatter fmt[]) {
        fmt[0].format("                                     Delivery");
        fmt[1].format("Warehouse: %6d", delivery.w_id);
        fmt[3].format("Carrier Number: %2d", delivery.o_carrier_id);
        if (transEnd == 0) {
            fmt[5].format("Execution Status: ");
        } else {
            fmt[5].format("Execution Status: %s", delivery.execution_status);
        }
    }

    public jTPCCTData getDeliveryBG() throws Exception {
        if (transType != TT_DELIVERY)
            throw new Exception("Not a DELIVERY");
        if (delivery.deliveryBG == null)
            throw new Exception("DELIVERY foreground not executed yet " + "or background part already consumed");

        jTPCCTData result = delivery.deliveryBG;
        delivery.deliveryBG = null;
        return result;
    }

    private class DeliveryData {
        /* terminal input data */
        public int w_id;
        public int o_carrier_id;

        /* terminal output data */
        public String execution_status;

        /*
         * executeDelivery() will store the background request
         * here for the caller to pick up and process as needed.
         */
        public jTPCCTData deliveryBG;
    }

    /* **********************************************************************
     * **********************************************************************
     * ***** DELIVERY_BG related methods and subclass. **********************
     * **********************************************************************
     * *********************************************************************/
    private void generateDeliveryBG(int w_id, long due, String ol_delivery_d, jTPCCTData parent) {
        /*
         * The DELIVERY_BG part is created as a result of executing the
         * foreground part of the DELIVERY transaction. Because of that
         * it inherits certain information from it.
         */
        numWarehouses = parent.numWarehouses;
        terminalWarehouse = parent.terminalWarehouse;
        terminalDistrict = parent.terminalDistrict;

        transType = TT_DELIVERY_BG;
        transDue = due;
        transStart = 0;
        transEnd = 0;
        transRbk = false;
        transError = null;

        newOrder = null;
        payment = null;
        orderStatus = null;
        stockLevel = null;
        delivery = null;
        deliveryBG = new DeliveryBGData();

        deliveryBG.w_id = parent.delivery.w_id;
        deliveryBG.o_carrier_id = parent.delivery.o_carrier_id;
        deliveryBG.ol_delivery_d = ol_delivery_d;

        deliveryBG.delivered_o_id = new int[10];
        for (int i = 0; i < 10; i++)
            deliveryBG.delivered_o_id[i] = -1;
    }

    private void executeDeliveryBG(Logger log, jTPCCConnection db) throws Exception {
        PreparedStatement stmt1;
        PreparedStatement stmt2;
        ResultSet rs;
        int rc;
        int d_id;
        int o_id;
        int c_id;
        double sum_ol_amount;
        long now = System.currentTimeMillis();

        try {
            for (d_id = 1; d_id <= 10; d_id++) {
                o_id = -1;

                stmt1 = db.stmtDeliveryBGSelectOldestNewOrder;
                stmt2 = db.stmtDeliveryBGDeleteOldestNewOrder;

                /*
                 * Try to find the oldest undelivered order for this
                 * DISTRICT. There may not be one, which is a case
                 * that needs to be reportd.
                 */
                while (o_id < 0) {
                    stmt1.setInt(1, deliveryBG.w_id);
                    stmt1.setInt(2, d_id);
                    rs = stmt1.executeQuery();
                    if (!rs.next()) {
                        rs.close();
                        break;
                    }
                    o_id = rs.getInt("no_o_id");
                    rs.close();

                    stmt2.setInt(1, deliveryBG.w_id);
                    stmt2.setInt(2, d_id);
                    stmt2.setInt(3, o_id);
                    rc = stmt2.executeUpdate();
                    if (rc == 0) {
                        /*
                         * Failed to delete the NEW_ORDER row. This is not
                         * an error since for concurrency reasons we did
                         * not select FOR UPDATE above. It is possible that
                         * another, concurrent DELIVERY_BG transaction just
                         * deleted this row and is working on it now. We
                         * simply got back and try to get the next one.
                         * This logic only works in READ_COMMITTED isolation
                         * level and will cause SQLExceptions in anything
                         * higher than that.
                         */
                        o_id = -1;
                    }
                }

                if (o_id < 0) {
                    // No undelivered NEW_ORDER found for this DISTRICT.
                    continue;
                }

                /*
                 * We found out oldest undelivered order for this DISTRICT
                 * and the NEW_ORDER line has been deleted. Process the
                 * rest of the DELIVERY_BG.
                 */

                // Update the ORDER setting the o_carrier_id.
                stmt1 = db.stmtDeliveryBGUpdateOrder;
                stmt1.setInt(1, deliveryBG.o_carrier_id);
                stmt1.setInt(2, deliveryBG.w_id);
                stmt1.setInt(3, d_id);
                stmt1.setInt(4, o_id);
                stmt1.executeUpdate();

                // Get the o_c_id from the ORDER.
                stmt1 = db.stmtDeliveryBGSelectOrder;
                stmt1.setInt(1, deliveryBG.w_id);
                stmt1.setInt(2, d_id);
                stmt1.setInt(3, o_id);
                rs = stmt1.executeQuery();
                if (!rs.next()) {
                    rs.close();
                    throw new Exception("ORDER in DELIVERY_BG for" + " O_W_ID=" + deliveryBG.w_id + " O_D_ID=" + d_id
                            + " O_ID=" + o_id + " not found");
                }
                c_id = rs.getInt("o_c_id");
                rs.close();

                // Update ORDER_LINE setting the ol_delivery_d.
                stmt1 = db.stmtDeliveryBGUpdateOrderLine;
                stmt1.setTimestamp(1, new java.sql.Timestamp(now));
                stmt1.setInt(2, deliveryBG.w_id);
                stmt1.setInt(3, d_id);
                stmt1.setInt(4, o_id);
                stmt1.executeUpdate();

                // Select the sum(ol_amount) from ORDER_LINE.
                stmt1 = db.stmtDeliveryBGSelectSumOLAmount;
                stmt1.setInt(1, deliveryBG.w_id);
                stmt1.setInt(2, d_id);
                stmt1.setInt(3, o_id);
                rs = stmt1.executeQuery();
                if (!rs.next()) {
                    rs.close();
                    throw new Exception("sum(OL_AMOUNT) for ORDER_LINEs with " + " OL_W_ID=" + deliveryBG.w_id
                            + " OL_D_ID=" + d_id + " OL_O_ID=" + o_id + " not found");
                }
                sum_ol_amount = rs.getDouble("sum_ol_amount");
                rs.close();

                // Update the CUSTOMER.
                stmt1 = db.stmtDeliveryBGUpdateCustomer;
                stmt1.setDouble(1, sum_ol_amount);
                stmt1.setInt(2, deliveryBG.w_id);
                stmt1.setInt(3, d_id);
                stmt1.setInt(4, c_id);
                stmt1.executeUpdate();

                // Recored the delivered O_ID in the DELIVERY_BG
                deliveryBG.delivered_o_id[d_id - 1] = o_id;
            }

            db.commit();
        } catch (SQLException se) {
            log.error("Unexpected SQLException in DELIVERY_BG");
            for (SQLException x = se; x != null; x = x.getNextException())
                log.error(x.getMessage());
            se.printStackTrace();

            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
        } catch (Exception e) {
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
            throw e;
        }
    }

    private void executeDeliveryBGStoredProcPostgres(Logger log, jTPCCConnection db) throws Exception {
        PreparedStatement stmt;
        ResultSet rs;
        Connection conn = db.getConnection();

        try {
            // Execute the stored procedure for DELIVERY_BG
            stmt = db.stmtDeliveryBGStoredProc;
            stmt.setInt(1, deliveryBG.w_id);
            stmt.setInt(2, deliveryBG.o_carrier_id);
            stmt.setTimestamp(3, Timestamp.valueOf(deliveryBG.ol_delivery_d));
            rs = stmt.executeQuery();

            // The stored proc succeeded. Extract the results.
            rs.next();

            Array arr_delivered_o_id = rs.getArray("out_delivered_o_id");

            if (arr_delivered_o_id != null) {
                Integer[] delivered_o_id = (Integer[]) arr_delivered_o_id.getArray();

                for (int i = 0; i < delivered_o_id.length; i++)
                    deliveryBG.delivered_o_id[i] = delivered_o_id[i];
            }

            rs.close();
            db.commit();
        } catch (SQLException se) {
            log.error("Unexpected SQLException in DELIVERY_BG");
            log.error("message: '" + se.getMessage() + "' transRbk=" + transRbk);
            for (SQLException x = se; x != null; x = x.getNextException())
                log.error(x.getMessage());
            se.printStackTrace();
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
        } catch (Exception e) {
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
            throw e;
        }
    }

    private void executeDeliveryBGStoredProcOracle(Logger log, jTPCCConnection db) throws Exception {
        CallableStatement stmt;
        Connection conn = db.getConnection();
        try {
            // Execute the stored procedure for DELIVERY_BG
            stmt = conn.prepareCall(db.stmtDeliveryBGStoredProcOracle);
            stmt.setInt(1, deliveryBG.w_id);
            stmt.setInt(2, deliveryBG.o_carrier_id);
            stmt.setTimestamp(3, Timestamp.valueOf(deliveryBG.ol_delivery_d));
            stmt.registerOutParameter(4, JDBCType.ARRAY, "INT_ARRAY");

            stmt.executeUpdate();

            // The stored proc succeeded. Extract the results.
            // Array ora_array = stmt.getArray(4);
            int[] delivery_array = (int[]) (stmt.getArray(4).getArray());
            for (int i = 0; i < 10; i++) {
                deliveryBG.delivered_o_id[i] = delivery_array[i];
            }
            db.commit();
            stmt.close();
        } catch (SQLException se) {
            log.error("Unexpected SQLException in DELIVERY_BG");
            log.error("message: '" + se.getMessage() + "' transRbk=" + transRbk);
            for (SQLException x = se; x != null; x = x.getNextException())
                log.error(x.getMessage());
            se.printStackTrace();

            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
        } catch (Exception e) {
            try {
                db.rollback();
            } catch (SQLException se2) {
                throw new Exception("Unexpected SQLException on rollback: " + se2.getMessage());
            }
            throw e;
        }

    }

    private void traceDeliveryBG(Logger log, Formatter fmt[]) {
        fmt[0].format("                                    DeliveryBG");
        fmt[1].format("Warehouse: %6d", deliveryBG.w_id);
        fmt[2].format("Carrier Number: %2d", deliveryBG.o_carrier_id);
        fmt[3].format("Delivery Date: %-19.19s", deliveryBG.ol_delivery_d);

        if (transEnd != 0) {
            for (int d_id = 1; d_id <= 10; d_id++) {
                fmt[4 + d_id].format("District %02d: delivered O_ID: %8d", d_id, deliveryBG.delivered_o_id[d_id - 1]);
            }
        }
    }

    public int[] getDeliveredOrderIDs() {
        return deliveryBG.delivered_o_id;
    }

    public int getSkippedDeliveries() {
        int numSkipped = 0;

        for (int i = 0; i < 10; i++) {
            if (deliveryBG.delivered_o_id[i] < 0)
                numSkipped++;
        }

        return numSkipped;
    }

    private class DeliveryBGData {
        /* DELIVERY_BG data */
        public int w_id;
        public int o_carrier_id;
        public String ol_delivery_d;

        public int delivered_o_id[];
    }
}
