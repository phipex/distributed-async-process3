package co.com.ies.pruebas.webservice;

import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.Queue;

@Component
public class FinishedTasckRedis extends QueueAsyncAbstract<Greeting> {

    private static final String KEY_QEUEU = "Finish.TaskTest_Qeueu";

    private final RedissonClient redissonClient;

    public FinishedTasckRedis(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    @Override
    protected void offer(Greeting element) {

        final Queue<Greeting> queue = getQueue();
        queue.add(element);
/*        final boolean noContains = !isContains(element, queue);
        if(noContains){
            queue.add(element);
        }else{
            System.out.println("QeueuAsyncRedis.offer ya habia sido agregada >>>>>>>>>>>>>>>>"+element);
        }*/

    }

    @Override
    protected void updateElement(Greeting element) {
        final Queue<Greeting> queue = getQueue();
        final Optional<Greeting> byTaskId = findByTaskId(element, queue);
        byTaskId.ifPresent(queue::remove);
        queue.add(element);

    }

    @Override
    protected boolean isContains(Greeting element, Queue<Greeting> queue) {
        final Optional<Greeting> first = findByTaskId(element, queue);
        return first.isPresent();

    }

    @Override
    protected boolean isContains(Greeting element) {
        final Queue<Greeting> queue = getQueue();
        return isContains(element, queue);
    }

    private Optional<Greeting> findByTaskId(Greeting element, Queue<Greeting> queue) {
        return queue.stream().filter(item -> {
            final Long id = item.getId();
            return id.equals(element.getId());
        }).findFirst();

    }

    @Override
    protected boolean remove(Greeting element) {
        Queue<Greeting> queue = getQueue();
        final Optional<Greeting> first = findByTaskId(element, queue);
        if(first.isPresent()){
            queue.remove(first.get());
            return true;
        }
        System.out.println("FinishedTasckRedis.remove no habia sido agregada >>>>>>>>>>>>>>>>");

        return false;
    }

    @Override
    protected Queue<Greeting> getQueue() {
        return redissonClient.getQueue(KEY_QEUEU);
        //TODO mirar el tema de los listener

    }

    @Override
    protected void processElement(Greeting element) {
        System.out.println("FinishedTasckRedis.processElement "+ element);

    }

    @Override
    protected int size() {
        return getQueue().size();
    }
}


