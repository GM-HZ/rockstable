package cn.gm.light.rtable.utils;

public interface Copiable<T> {

    /**
     * Copy current object(deep-clone).
     */
    T copy();
}
