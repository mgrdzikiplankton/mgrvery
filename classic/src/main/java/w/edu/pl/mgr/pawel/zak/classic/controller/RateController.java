package w.edu.pl.mgr.pawel.zak.classic.controller;

import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import w.edu.pl.mgr.pawel.zak.classic.repository.RateRepository;
import w.edu.pl.mgr.pawel.zak.classic.domain.PairEnum;
import w.edu.pl.mgr.pawel.zak.classic.domain.Rate;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@RestController
public class RateController {

    private final RateRepository rateRepository;

    public RateController(RateRepository rateRepository) {
        this.rateRepository = rateRepository;
    }

    @GetMapping(value = "/random", produces = "application/json")
    public String getRandom() {
        JSONObject response = new JSONObject();
        response
                .put("value", ThreadLocalRandom.current().nextDouble());
        return response.toString();
    }

    @GetMapping(value = "/rate/{pair}/{dateTime}", produces = "application/json")
    public ResponseEntity getRate(@PathVariable String pair, @PathVariable String dateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, formatter);

        Rate rate = rateRepository.findByPairIdAndAndDateTime(PairEnum.valueOf(pair).getIndx(), localDateTime);

        JSONObject response = new JSONObject();

        if (rate != null) {
            response
                    .put("high", rate.getHigh())
                    .put("low", rate.getLow())
                    .put("t_open", rate.getTOpen())
                    .put("t_close", rate.getTClose())
                    .put("pair", pair)
                    .put("date_time", dateTime);
            return new ResponseEntity<>(response.toString(), HttpStatus.OK);
        } else {
            response
                    .put("success", false)
                    .put("error", "There is no data for day " + dateTime + "and pair " + pair);
            return new ResponseEntity<>(response.toString(), HttpStatus.NOT_FOUND);
        }
    }

    @GetMapping(value = "/mediumRate/{pair}/{dimension}/{from}/{until}", produces = "application/json")
    public ResponseEntity getAverage(@PathVariable String pair, @PathVariable String dimension,
                                     @PathVariable String from, @PathVariable String until) {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
        LocalDateTime localFrom = LocalDateTime.parse(from, formatter);
        LocalDateTime localUntil = LocalDateTime.parse(until, formatter);

        Optional<Double> value;

        switch (dimension) {
            case "high":
                value = rateRepository.getAverageHigh(PairEnum.valueOf(pair).getIndx(), localFrom, localUntil);
                break;
            case "low":
                value = rateRepository.getAverageLow(PairEnum.valueOf(pair).getIndx(), localFrom, localUntil);
                break;
            case "open":
                value = rateRepository.getAverageOpen(PairEnum.valueOf(pair).getIndx(), localFrom, localUntil);
                break;
            case "close":
                value = rateRepository.getAverageClose(PairEnum.valueOf(pair).getIndx(), localFrom, localUntil);
                break;
            default:
                value = Optional.empty();
        }

        JSONObject response = new JSONObject();

        if (value.isPresent()) {
            response
                    .put("value", value.get())
                    .put("pair", pair)
                    .put("from", from)
                    .put("until", until)
                    .put("dimension", dimension);
            return new ResponseEntity<>(response.toString(), HttpStatus.OK);
        } else{
            response
                    .put("success", false)
                    .put("error", "There is no data for days " + from + " " + until + " for pair " + pair);
            return new ResponseEntity<>(response.toString(), HttpStatus.NOT_FOUND);
        }
    }

}
