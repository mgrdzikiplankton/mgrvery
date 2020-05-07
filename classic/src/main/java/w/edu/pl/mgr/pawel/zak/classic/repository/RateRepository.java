package w.edu.pl.mgr.pawel.zak.classic.repository;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import w.edu.pl.mgr.pawel.zak.classic.domain.Rate;
import w.edu.pl.mgr.pawel.zak.classic.domain.RateId;

import java.time.LocalDateTime;
import java.util.Optional;

public interface RateRepository extends CrudRepository<Rate, RateId> {

    Rate findByPairIdAndAndDateTime(int pairId, LocalDateTime dateTime);

    @Query("select avg(high) from Rate where pairId = :pairId  and dateTime between :fromd and :until")
    Optional<Double> getAverageHigh(@Param("pairId") int pairId, @Param("fromd") LocalDateTime from, @Param("until") LocalDateTime until);

    @Query("select avg(low) from Rate where pairId = :pairId  and dateTime between :fromd and :until")
    Optional<Double> getAverageLow(@Param("pairId") int pairId, @Param("fromd") LocalDateTime from, @Param("until") LocalDateTime until);

    @Query("select avg(tOpen) from Rate where pairId = :pairId  and dateTime between :fromd and :until")
    Optional<Double> getAverageOpen(@Param("pairId") int pairId, @Param("fromd") LocalDateTime from, @Param("until") LocalDateTime until);

    @Query("select avg(tClose) from Rate where pairId = :pairId  and dateTime between :fromd and :until")
    Optional<Double> getAverageClose(@Param("pairId") int pairId, @Param("fromd") LocalDateTime from, @Param("until") LocalDateTime until);

}
