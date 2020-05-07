package w.edu.pl.mgr.pawel.zak.classic.domain;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
public class RateId implements Serializable {

    LocalDateTime dateTime;
    int pairId;

}
