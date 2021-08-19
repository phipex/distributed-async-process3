package co.com.ies.pruebas.webservice;

import org.springframework.scheduling.annotation.Scheduled;

public interface AsyncProcessService {

    void addTasks();

    void processTaskList();
}
