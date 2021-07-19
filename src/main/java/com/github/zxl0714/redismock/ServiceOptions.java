package com.github.zxl0714.redismock;

/**
 * Created by Xiaolu on 2015/4/22.
 */
public class ServiceOptions {

    private int closeSocketAfterSeveralCommands = 0;

    private int databaseCount = 16;

    public ServiceOptions() {}

    public void setCloseSocketAfterSeveralCommands(int count) {
        this.closeSocketAfterSeveralCommands = count;
    }

    public int getCloseSocketAfterSeveralCommands() {
        return closeSocketAfterSeveralCommands;
    }

    public int getDatabaseCount() {
        return databaseCount;
    }

    public void setDatabaseCount(int databaseCount) {
        this.databaseCount = databaseCount;
    }
}
