package org.trahim.util;

import org.trahim.row.Person;

public final class DebugRowInfo {
    private final Object object;
    private final boolean isDeleted;
    private final boolean isTemporary;

    public DebugRowInfo(Object object, boolean isDeleted, boolean isTemporary) {
        this.object = object;
        this.isDeleted = isDeleted;
        this.isTemporary = isTemporary;
    }

    public Object getObject() {
        return object;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public boolean isTemporary() {
        return isTemporary;
    }
}
