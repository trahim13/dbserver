package org.trahim.util;

import org.trahim.row.Person;

public final class DebugRowInfo {
    private Person person;
    private boolean isDeleted;

    public DebugRowInfo(Person person, boolean isDeleted) {
        this.person = person;
        this.isDeleted = isDeleted;
    }

    public Person getPerson() {
        return person;
    }

    public boolean isDeleted() {
        return isDeleted;
    }
}
