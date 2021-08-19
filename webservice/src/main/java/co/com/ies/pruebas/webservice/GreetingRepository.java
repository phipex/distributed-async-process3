package co.com.ies.pruebas.webservice;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface GreetingRepository extends JpaRepository<Greeting, Long> {

    List<Greeting> findByIpTramitedIsNull();

}
