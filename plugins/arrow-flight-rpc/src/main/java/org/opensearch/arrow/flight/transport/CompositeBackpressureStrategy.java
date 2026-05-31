/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.BackpressureStrategy;

/**
 * {@link BackpressureStrategy} that composes Arrow's standard ready/cancel handling with
 * channel-specific cleanup.
 *
 * <p>{@link BackpressureStrategy#register(org.apache.arrow.flight.FlightProducer.ServerStreamListener)}
 * installs both {@code setOnReadyHandler} and {@code setOnCancelHandler} on the listener,
 * overwriting any handlers the channel installed earlier. Subclassing lets the channel run
 * its own cancel cleanup before the parent strategy notifies parked waiters, so a thread
 * waking from {@code waitForListener} always observes the channel in cancelled state.
 */
final class CompositeBackpressureStrategy extends BackpressureStrategy.CallbackBackpressureStrategy {
    private final Runnable channelCancelCleanup;

    CompositeBackpressureStrategy(Runnable channelCancelCleanup) {
        this.channelCancelCleanup = channelCancelCleanup;
    }

    @Override
    protected void cancelCallback() {
        try {
            channelCancelCleanup.run();
        } finally {
            super.cancelCallback();
        }
    }
}
