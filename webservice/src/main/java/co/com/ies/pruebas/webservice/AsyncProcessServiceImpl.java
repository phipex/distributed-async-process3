package co.com.ies.pruebas.webservice;

import org.redisson.api.RSemaphore;
import org.redisson.api.RedissonClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
public class AsyncProcessServiceImpl implements AsyncProcessService{

    public static final String LOCK_ADD_TASK = "Lock.AddTask";
    public static final String LOCK_ADQUIRE_TASK = "Lock.AdquireTask";
    public static final String LOCK_PROCESS_TASKS = "Lock.ProcessTask";

    private final GreetingRepository greetingRepository;
    private final RedissonClient redissonClient;
    private final QeueuAsyncRedis qeueuAsyncRedis;

    private final ProcessorDelayedRedis processorDelayed;

    public AsyncProcessServiceImpl(GreetingRepository greetingRepository, RedissonClient redissonClient, QeueuAsyncRedis qeueuAsyncRedis, ProcessorDelayedRedis processorDelayed) {

        this.greetingRepository = greetingRepository;
        this.redissonClient = redissonClient;
        this.qeueuAsyncRedis = qeueuAsyncRedis;
        this.processorDelayed = processorDelayed;
    }

    @Scheduled(fixedDelay = 1000)
    public void scheduleFixedDelayAddTask() {
        //System.out.println("scheduleFixedDelayAddTask Fixed delay task - " + System.currentTimeMillis() / 1000);

        RSemaphore semaphore = redissonClient.getSemaphore(LOCK_ADD_TASK);
        final int availablePermits = semaphore.availablePermits();

        if(availablePermits == 0){
            final boolean trySetPermits = semaphore.trySetPermits(1);
            System.out.println("AsyncProcessServiceImpl.scheduleFixedDelayAddTask trySetPermits" + trySetPermits);
        }
        final boolean tryAcquire = semaphore.tryAcquire();
        System.out.println("AsyncProcessServiceImpl.scheduleFixedDelayAddTask " + tryAcquire + " availablePermits = " + availablePermits);
        if(tryAcquire){
            System.out.println("AsyncProcessServiceImpl.scheduleFixedDelayAddTask adquire");
            addTasks();
            semaphore.release();
            System.out.println("AsyncProcessServiceImpl.scheduleFixedDelayAddTask release");
        }

    }

    @Override
    public void addTasks(){
        final List<Greeting> greetingsByIpTramitedIsNull = greetingRepository.findByIpTramitedIsNull();

        if(greetingsByIpTramitedIsNull.isEmpty()){
            System.out.print("-");
            return;
        }

        final List<Greeting> collect = greetingsByIpTramitedIsNull.stream()
                .filter(greeting -> greeting.getIpTramited() == null)
                .collect(Collectors.toList());

        qeueuAsyncRedis.offerTascks(collect);
        final int size = greetingsByIpTramitedIsNull.size();
        System.out.println("AsyncProcessServiceImpl.addTasks size = " + size);
    }

    @Scheduled(fixedDelay = 1000)
    public void scheduleFixedDelayProcessTask() {

        //System.out.println("scheduleFixedDelayProcessTask Fixed delay task - " + System.currentTimeMillis() / 1000);

        RSemaphore semaphore = redissonClient.getSemaphore(LOCK_PROCESS_TASKS);
        final int availablePermits = semaphore.availablePermits();

        if(availablePermits == 0){
            final boolean trySetPermits = semaphore.trySetPermits(1);
            System.out.println("AsyncProcessServiceImpl.scheduleFixedDelayProcessTask trySetPermits" + trySetPermits);
        }
        final boolean tryAcquire = semaphore.tryAcquire();
        System.out.println("AsyncProcessServiceImpl.scheduleFixedDelayProcessTask " + tryAcquire + " availablePermits = " + availablePermits);
        if(tryAcquire){
            System.out.println("AsyncProcessServiceImpl.scheduleFixedDelayProcessTask adquire");
            processTaskList();
            semaphore.release();
            System.out.println("AsyncProcessServiceImpl.scheduleFixedDelayProcessTask release");
        }


    }

    @Override
    public void processTaskList() {
        final Queue<Greeting> queue = qeueuAsyncRedis.getQueue();

        if(queue.isEmpty()){

            return;
        }

        final List<Greeting> taskList = queue.stream()
                .filter(value -> value.getIpTramited() == null)
                .collect(Collectors.toList());

        processTaskList(taskList);
    }

    private void processTaskList(List<Greeting> lista) {
        System.out.println("AsyncProcessServiceImpl.processTaskList iniciando lista = " + lista.size());
        for(Greeting element: lista){

            processorDelayed.processElement(element);
            //qeueuAsyncRedis.updateElement(element);
            qeueuAsyncRedis.remove(element);

        }
        System.out.println("AsyncProcessServiceImpl.processTaskList finalizando lista = " + lista.size());
    }
}
