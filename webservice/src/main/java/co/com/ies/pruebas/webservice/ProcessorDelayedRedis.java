package co.com.ies.pruebas.webservice;

import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Optional;

@Component
public class ProcessorDelayedRedis {

    private final GreetingRepository greetingRepository;

    private final String hostAddress;

    private final FinishedTasckRedis finishedTasck;

    public ProcessorDelayedRedis(FinishedTasckRedis finishedTasck, GreetingRepository greetingRepository) {
        this.finishedTasck = finishedTasck;
        String hostAddress1;
        this.greetingRepository = greetingRepository;
        try {
            hostAddress1 = InetAddress.getLocalHost().getHostAddress() ;
        } catch (UnknownHostException e) {
            e.printStackTrace();
            hostAddress1 = "no found";
        }
        this.hostAddress = hostAddress1;
    }


    public void processElement(Greeting greeting) {
        //final int timeSleep = rn.nextInt(2000);
        final int timeSleep = 2000;

        try {
            Thread.sleep(timeSleep);
            System.out.println("ProcessorDelayedRedis.processElement resolviendo element = " + greeting + " ,timeSleep = "+ timeSleep);


//TODO verificar que ya ha sido tramitado para ver la tasa de falsos positivos

            verifyExist(greeting);

            greeting.setIpTramited(hostAddress);
            final long timeInMillis = Calendar.getInstance().getTimeInMillis();
            greeting.setTime(timeInMillis);

            greetingRepository.save(greeting);
            greetingRepository.flush();
            finishedTasck.offer(greeting);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void verifyExist(Greeting task) {
        try {
            final Optional<Greeting> optional = greetingRepository.findById(task.getId());
            if (optional.isPresent()) {

                final Greeting greeting = optional.get();
                if (greeting.getIpTramited() != null) {
                    System.out.println("ProcessorDelayedRedis.processElement byId = " + greeting + "<<<<<<<<<<<<<<<<<<<<<<<<<<");
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


