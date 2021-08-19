package co.com.ies.pruebas.webservice;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;

public abstract class QueueAsyncAbstract<TaskType> {

    protected abstract void offer(TaskType element);

    protected abstract void updateElement(TaskType element);
    protected abstract boolean isContains(TaskType element, Queue<TaskType> queue);
    protected abstract boolean isContains(TaskType element);
    protected abstract boolean remove(TaskType element);
    protected abstract Queue<TaskType> getQueue();
    protected abstract void processElement(TaskType element);
    protected abstract int size();

    /**
     *
     * @param elements
     * @deprecated no es efectivo para el metodo que verifica si ya esta adentro
     */
    @Deprecated
    public void offerTascks(List<TaskType> elements){
        //agregar a la cola
        elements.forEach(this::offer);

    }

    public void offerTasck(TaskType element){
        //agregar a la cola
        offer(element);

    }

    public void processQueue(){
        Queue<TaskType> elements = getQueue();
        System.out.println("QueueAsyncAbstract.processQueue elements = " + elements.size());
        final Iterator<TaskType> iterator = elements.iterator();

        while (iterator.hasNext()){
            final TaskType tasckType = iterator.next();
            try {
                processElement(tasckType);
                elements.remove(tasckType);
                iterator.remove();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Error al procesar la tarea, se agrega de nuevo en la cola");
                offer(tasckType);
            }
        }


    }

    private void packProcess(int limit) {
        Queue<TaskType> elementsPack = getQueue();
        System.out.println("QueueAsyncAbstract.processQueue elementsPack = " + elementsPack.size());
        // use un iterator en eves del for y un poll por que me quedaban completas las tareas
        final Iterator<TaskType> iterator = elementsPack.iterator();
        int count = 0;
        while (iterator.hasNext() && count < limit){
            final TaskType tasckType = iterator.next();
            try {
                processElement(tasckType);
                elementsPack.remove(tasckType);
                iterator.remove();
                count++;
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Error al procesar la tarea, se agrega de nuevo en la cola");
                offer(tasckType);
            }
        }
    }


}

