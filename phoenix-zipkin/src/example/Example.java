import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.*;
import org.springframework.web.bind.annotation.*;

@RestController
@EnableAutoConfiguration
public class Example {
	

    @RequestMapping(value="/trace", method=RequestMethod.GET)
    public ResponseEntity<Trace> get() {

        Trace t = new Trace("1","upsert into mytable");
        return new ResponseEntity<Trace>(t, HttpStatus.OK);
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(Example.class, args);
    }

}