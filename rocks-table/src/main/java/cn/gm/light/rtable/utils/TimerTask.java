package cn.gm.light.rtable.utils;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicBoolean;

public class TimerTask {
    private final Timer timer;
    private final Runnable electionTask;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private java.util.TimerTask currentTask;

    public TimerTask(Runnable runnable) {
        this.timer = new Timer("TimerTask",true);
        this.electionTask = runnable;
        this.isRunning.set(true);
    }

    public void start(long initialDelay, long delay) {
        if (currentTask != null) {
            currentTask.cancel(); // 取消已有任务
        }
        currentTask = new java.util.TimerTask() {
            @Override
            public void run() {
                electionTask.run();
            }
        };
        timer.schedule(currentTask, initialDelay, delay);
    }

    public void start(long newDelay) {
        this.start(newDelay, newDelay);
    }

    public void stop() {
        if (currentTask != null) {
            currentTask.cancel(); // 取消当前任务
        }
        timer.cancel(); // 终止定时器线程
        this.isRunning.set(false);
    }

    public boolean isRunning() {
        return this.isRunning.get();
    }
}
