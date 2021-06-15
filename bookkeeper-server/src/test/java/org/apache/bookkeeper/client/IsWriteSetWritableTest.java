package org.apache.bookkeeper.client;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


@RunWith(Parameterized.class)
public class IsWriteSetWritableTest {
    private DistributionSchedule.WriteSet writeSet;
    private long key;
    private int allowedNonWritableCount;

    public IsWriteSetWritableTest( TestParameters input ){
        this.writeSet = input.getWriteSet();
        this.key = input.getKey();
        this.allowedNonWritableCount = input.getAllowedNonWritableCount();
    }

    Parameterized.Parameters
    public static Collection<TestParameters> getTestParameters() {
        List<TestParameters> inputs = new ArrayList<>();

        /*
            data: {0}, {not empty}
            offset: {-1}, {0}, {not valid, entry filled}, {valid}
            length: {true}, {false}
            cb valid: {true}, {false}

         */


        return inputs;
    }

    private static class TestParameters{
        private DistributionSchedule.WriteSet writeSet;
        private long key;
        private int allowedNonWritableCount;


        public TestParameters(DistributionSchedule.WriteSet writeSet,
                              long key, int allowedNonWritableCount) {
            this.writeSet = writeSet;
            this.key = key;
            this.allowedNonWritableCount = allowedNonWritableCount;
        }

        public DistributionSchedule.WriteSet getWriteSet() {
            return writeSet;
        }

        public long getKey() {
            return key;
        }

        public int getAllowedNonWritableCount() {
            return allowedNonWritableCount;
        }
    }

    @Before
    public void setup(){

    }

    @Test
    public void isWriteSetWritableTest(){
        
    }
}
