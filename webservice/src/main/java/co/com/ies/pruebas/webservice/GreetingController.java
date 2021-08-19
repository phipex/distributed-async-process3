package co.com.ies.pruebas.webservice;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.Random;

@RestController
public class GreetingController {

    private static final String TEMPLATE = "Hello Docker, %s!";

    private final String hostAddress;

    private final GreetingRepository greetingRepository;

    private final AsyncProcessService asyncProcessService;

    public GreetingController(GreetingRepository greetingRepository, AsyncProcessService asyncProcessService) {
        this.asyncProcessService = asyncProcessService;
        String hostAddress1;
        this.greetingRepository = greetingRepository;
        try {
            hostAddress1 = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            hostAddress1 = "no found";
        }
        this.hostAddress = hostAddress1;
    }

    @GetMapping("/greeting")
    public Greeting greeting(@RequestParam(value = "name",
            defaultValue = "World") String name) {

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        return getGreeting(name);
    }

    private Greeting getGreeting(String name) {
        String format = String.format(TEMPLATE, name);
        return new Greeting(format, hostAddress);
    }

    @GetMapping("/greeting/new")
    public ResponseEntity<Greeting> createGreeting() {

        Random random = new SecureRandom();
        int ran = random.nextInt();

        final Greeting nuevo = getGreeting("Nuevo" + ran);
        greetingRepository.save(nuevo);

        return ResponseEntity.ok(nuevo);
    }

    @GetMapping("/add")
    public ResponseEntity<String> addTask() {
        asyncProcessService.addTasks();
        return ResponseEntity.ok("Tareas nuevas");
    }

    @GetMapping("/process")
    public ResponseEntity<String> adquireTask() {
        asyncProcessService.processTaskList();
        return ResponseEntity.ok("Tareas adquiridas");
    }

}