package me.kiril.mesos.example;

final class Tuple2<T1, T2> {
    public final T1 _1;
    public final T2 _2;

    public Tuple2(final T1 v1, final T2 v2) {
        _1 = v1;
        _2 = v2;
    }

    public static <T1, T2> Tuple2<T1, T2> create( final T1 v1,  final T2 v2) {
        return new Tuple2<>(v1, v2);
    }

    public static <T1, T2> Tuple2<T1, T2> t( final T1 v1,  final T2 v2) {
        return create(v1, v2);
    }
}