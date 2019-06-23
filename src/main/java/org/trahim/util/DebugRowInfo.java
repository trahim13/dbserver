package org.trahim.util;

import org.trahim.row.Person;

public final class DebugRowInfo {
    private final Person person;
    private final boolean isDeleted;
    private final boolean isTemporary;

    public DebugRowInfo(Person person, boolean isDeleted, boolean isTemporary) {
        this.person = person;
        this.isDeleted = isDeleted;
        this.isTemporary = isTemporary;
    }

    public Person getPerson() {
        return person;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public boolean isTemporary() {
        return isTemporary;
    }
}
