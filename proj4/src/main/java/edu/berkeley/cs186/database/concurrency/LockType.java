package edu.berkeley.cs186.database.concurrency;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement

        //  |     |  S    X    IS    IX    SIX    NL
        //  -----------------------------------------
        //  |   S |  T    F     T     F     F     T
        //  |   X |  F    F     F     F     F     T
        //  |  IS |  T    F     T     T     T     T
        //  |  IX |  F    F     T     T     F     T
        //  | SIX |  F    F     T     F     F     T
        //  |  NL |  T    T     T     T     T     T

        boolean[][] isCompatible =
                new boolean[][] {{true, false, true, false, false, true},
                        {false, false, false, false, false, true},
                        {true, false, true, true, true, true},
                        {false, false, true, true, false, true},
                        {false, false, true, false, false, true},
                        {true, true, true, true, true, true}};
        Map<LockType, Integer> map = new HashMap<>();
        map.put(LockType.S, 0);
        map.put(LockType.X, 1);
        map.put(LockType.IS, 2);
        map.put(LockType.IX, 3);
        map.put(LockType.SIX, 4);
        map.put(LockType.NL, 5);

        int numOfA = map.get(a);
        int numOfB = map.get(b);
        return isCompatible[numOfA][numOfB];

    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        switch (parentLockType) {
            case NL:
            case S:
            case X:
                return childLockType == NL;
            case IS: return (childLockType == S || childLockType == IS || childLockType == NL);
            case IX: return true;
            case SIX: return (childLockType == X || childLockType == IX ||
                    childLockType == SIX || childLockType == NL);
            default: throw new UnsupportedOperationException("bad parent LockType");
        }

    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        switch (required) {
            case S: return (substitute == S || substitute == X || substitute == SIX);
            case X: return (substitute == X);
            case IS: return (substitute != NL);
            case IX: return (substitute == SIX || substitute == IX || substitute == X);
            case SIX: return (substitute == SIX || substitute == X);
            case NL: return true;
            default: throw new UnsupportedOperationException("bad required LockType");
        }
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

