package cn.gm.light.rtable;

import cn.gm.light.rtable.utils.Copiable;

public class Status implements Copiable<Status> {

    /**
     * Status internal state.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 11:17:51 AM
     */
    private static class State {
        /** error code */
        int    code;
        /** error msg*/
        String msg;

        State(int code, String msg) {
            super();
            this.code = code;
            this.msg = msg;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + this.code;
            result = prime * result + (this.msg == null ? 0 : this.msg.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            State other = (State) obj;
            if (this.code != other.code) {
                return false;
            }
            if (this.msg == null) {
                return other.msg == null;
            } else {
                return this.msg.equals(other.msg);
            }
        }
    }

    private State state;

    public Status() {
        this.state = null;
    }

    /**
     * Creates a OK status instance.
     */
    public static Status OK() {
        return new Status();
    }

    public Status(Status s) {
        if (s.state != null) {
            this.state = new State(s.state.code, s.state.msg);
        } else {
            this.state = null;
        }
    }

    public Status(int code, String fmt, Object... args) {
        this.state = new State(code, String.format(fmt, args));
    }

    public Status(int code, String errorMsg) {
        this.state = new State(code, errorMsg);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.state == null ? 0 : this.state.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Status other = (Status) obj;
        if (this.state == null) {
            return other.state == null;
        } else {
            return this.state.equals(other.state);
        }
    }

    /**
     * Reset status to be OK state.
     */
    public void reset() {
        this.state = null;
    }

    /**
     * Returns true when status is in OK state.
     */
    public boolean isOk() {
        return this.state == null || this.state.code == 0;
    }

    /**
     * Set error code.
     */
    public void setCode(int code) {
        if (this.state == null) {
            this.state = new State(code, null);
        } else {
            this.state.code = code;
        }
    }

    /**
     * Get error code.
     */
    public int getCode() {
        return this.state == null ? 0 : this.state.code;
    }


    /**
     * Set error msg
     */
    public void setErrorMsg(String errMsg) {
        if (this.state == null) {
            this.state = new State(0, errMsg);
        } else {
            this.state.msg = errMsg;
        }
    }

    /**
     * Set error code and error msg.
     */
    public void setError(int code, String fmt, Object... args) {
        this.state = new State(code, String.format(String.valueOf(fmt), args));
    }


    @Override
    public String toString() {
        if (isOk()) {
            return "Status[OK]";
        } else {
            return "Status[" +  "<" + this.state.code + ">: " + this.state.msg
                   + "]";
        }
    }

    @Override
    public Status copy() {
        return new Status(this.getCode(), this.getErrorMsg());
    }

    /**
     * Get the error msg.
     */
    public String getErrorMsg() {
        return this.state == null ? null : this.state.msg;
    }
}
