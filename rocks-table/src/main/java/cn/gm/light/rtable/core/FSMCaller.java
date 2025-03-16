package cn.gm.light.rtable.core;

import cn.gm.light.rtable.exception.RtException;

/**
 * Finite state machine caller.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 11:07:52 AM
 */
public interface FSMCaller {

    /**
     * Listen on lastAppliedLogIndex update events.
     *
     * @author dennis
     */
    interface LastAppliedLogIndexListener {

        /**
         * Called when lastAppliedLogIndex updated.
         *
         * @param lastAppliedLogIndex the log index of last applied
         */
        void onApplied(final long lastAppliedLogIndex);
    }

    /**
     * Returns true when current thread is the thread that calls state machine callback methods.
     * @return
     */
    boolean isRunningOnFSMThread();

    /**
     * Adds a LastAppliedLogIndexListener.
     */
    void addLastAppliedLogIndexListener(final LastAppliedLogIndexListener listener);

    /**
     * Called when log entry committed
     *
     * @param committedIndex committed log index
     */
    boolean onCommitted(final long committedIndex);


    /**
     * Called when error happens.
     *
     * @param error error info
     */
    boolean onError(final RtException error);

    /**
     * Returns the last log entry index to apply state machine.
     */
    long getLastAppliedIndex();

    /**
     * Called after shutdown to wait it terminates.
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    void join() throws InterruptedException;
}