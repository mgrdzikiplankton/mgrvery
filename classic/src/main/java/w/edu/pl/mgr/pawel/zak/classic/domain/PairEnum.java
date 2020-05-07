package w.edu.pl.mgr.pawel.zak.classic.domain;

public enum PairEnum {
    AUDUSD(1),
    EURCHF(2),
    EURJPY(3),
    EURUSD(4),
    USDCAD(5),
    USDCHF(6),
    USDJPY(7);

    private int indx;

    PairEnum(int indx) {
        this.indx = indx;
    }

    public int getIndx() {
        return indx;
    }
}