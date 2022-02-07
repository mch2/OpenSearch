/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.segmentcopy.copy;

import org.opensearch.indices.recovery.Timer;

public class ReplicationState {

    private final Timer timer;

    public ReplicationState() {
        this.timer = new Timer();
        timer.start();
    }

    public Timer getTimer() {
        return timer;
    }

    public enum Stage {
        INIT((byte) 0),

        /**
         * Copy Segment files
         */
        COPY((byte) 1),

        /**
         * potentially running check index
         */
        VERIFY_INDEX((byte) 2),

        /**
         * starting up the engine, sync the translog with new files.
         */
        TRANSLOG((byte) 3),

        /**
         * performing final task after all translog ops have been done
         */
        FINALIZE((byte) 4),

        DONE((byte) 5);

        private static final ReplicationState.Stage[] STAGES = new ReplicationState.Stage[ReplicationState.Stage.values().length];

        static {
            for (ReplicationState.Stage stage : ReplicationState.Stage.values()) {
                assert stage.id() < STAGES.length && stage.id() >= 0;
                STAGES[stage.id] = stage;
            }
        }

        private final byte id;

        Stage(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static ReplicationState.Stage fromId(byte id) {
            if (id < 0 || id >= STAGES.length) {
                throw new IllegalArgumentException("No mapping for id [" + id + "]");
            }
            return STAGES[id];
        }
    }
}
