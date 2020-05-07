package w.edu.pl.mgr.pawel.zak.classic.domain;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import java.io.Serializable;
import java.time.LocalDateTime;

@Entity
@IdClass(RateId.class)
@Data
public class Rate implements Serializable {

    @Id
    LocalDateTime dateTime;

    @Id
    int pairId;

    double high;
    double low;
    double tOpen;
    double tClose;




}
