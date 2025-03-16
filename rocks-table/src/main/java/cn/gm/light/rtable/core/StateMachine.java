package cn.gm.light.rtable.core;

import cn.gm.light.rtable.exception.RtException;

/**
 * |StateMachine| is the sink of all the events of a very raft node.
 * Implement a specific StateMachine for your own business logic.
 * NOTE: All the interfaces are not guaranteed to be thread safe and they are
 * called sequentially, saying that every single operation will block all the
 * following ones.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 5:43:21 PM
 */
public interface StateMachine {

    /**
     * Update the StateMachine with a batch a tasks that can be accessed
     * through |iterator|.
     *
     * Invoked when one or more tasks that were passed to Node#apply(Task) have been
     * committed to the raft group (quorum of the group peers have received
     * those tasks and stored them on the backing storage).
     *
     * Once this function returns to the caller, we will regard all the iterated
     * tasks through |iter| have been successfully applied. And if you didn't
     * apply all the the given tasks, we would regard this as a critical error
     * and report a error whose type is ERROR_TYPE_STATE_MACHINE.
     *
     * @param iter iterator of states
     */
    void onApply(final Iterator iter);

    /**
     * Invoked once when the raft node was shut down.
     * Default do nothing
     */
    void onShutdown();

    /**
     * This method is called when a critical error was encountered, after this
     * point, no any further modification is allowed to applied to this node
     * until the error is fixed and this node restarts.
     *
     * @param e raft error message
     */
    void onError(final RtException e);

}
