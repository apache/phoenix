import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.*;
import org.springframework.web.bind.annotation.*;

@RestController
@EnableAutoConfiguration
public class Example {
	
	
	@RequestMapping("/greet")
    String sayHello(@RequestParam("name") String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("The 'name' parameter must not be null or empty");
        }
        return String.format("Hello %s!", name);
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(Example.class, args);
    }
    
    

    
}