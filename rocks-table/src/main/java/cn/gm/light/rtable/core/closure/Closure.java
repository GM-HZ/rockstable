package cn.gm.light.rtable.core.closure;

import cn.gm.light.rtable.Status;

public interface Closure {

    /**
     * Called when task is done.
     *
     * @param status the task status.
     */
    void run(final Status status);
}
