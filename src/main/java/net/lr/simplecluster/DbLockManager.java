/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.lr.simplecluster;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes sure only one lock manager instance is running
 * Needs at least a DataSource and a lock table to work.
 * 
 * The current status can be retrieved with isActive()
 */
public class DbLockManager {
    private static Logger LOG = LoggerFactory.getLogger(DbLockManager.class);
    private DataSource dataSource;
    private int sleepTime = 1000;
    private String lockTableName = "lockTable";
    private FailoverHandler handler;

    private boolean active;

    private Thread thread;
    private boolean shouldStop;
    private boolean reconnect;
    private String lastExceptionMessage;

    public void start() {
        shouldStop = false;
        this.thread = new Thread(new Runnable() {

            @Override
            public void run() {
                manageLock();
            }
        }, "LockManager");
        thread.start();
    }

    public void stop() {
        shouldStop = true;
    }

    /**
     * Keep Try to get or refresh the lock on the table
     * If an exception happens or we shut down we exit from the loop
     * 
     * Basically we would not need to exit the loop if only the transaction
     * times out while getting the lock. As this case is difficult to
     * detect with all different drivers we also exit in that case.
     * 
     * @param stmt
     * @throws SQLException When either the transaction times out getting the log
     *         or another problem happens 
     * @throws InterruptedException when the sleep is interrupted
     */
    private void manageLockUsingStatement(Statement stmt) throws SQLException, InterruptedException {
        do {
            this.active = false;
            if (handler != null) {
                handler.stop();
            }
            stmt.execute(String.format("select * from %s for update", lockTableName));
            if (handler != null) {
                handler.start();
            }
            this.active = true;
            Thread.sleep(sleepTime);
        } while (!shouldStop);
    }
    
    /**
     * Check and log the exception and determine if a reconnect is needed
     * 
     * Override this if your database returns something else on transaction timeout
     * 
     * @param t Throwable that was thrown
     * @return true if reconnect is needed
     */
    protected boolean handleException(Throwable t) {
        if (t.getMessage() != null && t.getMessage().startsWith("Lock wait timeout exceeded")) {
            lastExceptionMessage = null;
            LOG.debug("Failed to acquire lock. " + t.getMessage());
            return false;
        } else {
            if (lastExceptionMessage != null && lastExceptionMessage.equals(t.getMessage())) {
                LOG.debug(t.getMessage(), t);
            } else {
                LOG.error(t.getMessage(), t);
                lastExceptionMessage = t.getMessage();
            }
            return true;
        }
    }

    /**
     * Create a connection and statement and try to get the lock
     * 
     * In case of an error we reconnect so we can also handle if the server is down
     */
    private void manageLock() {
        do {
            Connection con = null;
            Statement stmt = null;
            try {
                if (con == null) {
                    con = dataSource.getConnection();
                    con.setAutoCommit(false);
                }
                stmt = con.createStatement();
                manageLockUsingStatement(stmt);
            } catch (Throwable t) {
                reconnect = handleException(t);
            } finally {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    return;
                }
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException e1) {
                    }
                }
                if (con != null && this.reconnect) {
                    try {
                        con.close();
                    } catch (SQLException e1) {
                    } finally {
                        con = null;
                        this.reconnect = false;
                    }
                }
            }
        } while (!shouldStop);

    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setSleepTime(int sleepTime) {
        this.sleepTime = sleepTime;
    }

    public void setLockTableName(String lockTableName) {
        this.lockTableName = lockTableName;
    }

    public void setHandler(FailoverHandler handler) {
        this.handler = handler;
    }

    public boolean isActive() {
        return active;
    }
}
