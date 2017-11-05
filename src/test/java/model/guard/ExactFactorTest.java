package java.model.guard;

import fileop.Reader;
import model.guard.ExactFactor;
import model.policy.BEExpression;
import model.policy.BooleanCondition;
import model.policy.BooleanPredicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Author primpap
 */
class ExactFactorTest {

    //TODO: Just change testing to string comparisons from print

    BEExpression beExpression;
    BooleanCondition mulP1, mulP2, mulP22;
    ExactFactor qP1, rP1, qP2, rP2;

    @BeforeEach
    void setUp() {
        beExpression = new BEExpression();
        mulP1 = new BooleanCondition();
        mulP1.setAttribute("SEMANTIC_OBSERVATION.user_id");
        mulP1.getBooleanPredicates().add(new BooleanPredicate("10", "="));

    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void findBestFactor(){
        assert(returnBestFactor("/policies/policy0.json").getMultiplier().isEmpty());
        ExactFactor ef = new ExactFactor();
        ef = returnBestFactor("/policies/policy1.json");
        assert(ef.getMultiplier().get(0).compareTo(mulP1) == 0);
        assert(ef.getQuotient());
        assert(ef.getReminder());

    }

    ExactFactor returnBestFactor(String filename) {
        beExpression.parseJSONList(Reader.readFile(filename));
        System.out.printf("Finding exact factor for %s: %s", filename, beExpression.createQueryFromPolices());
        ExactFactor ef = new ExactFactor(beExpression);
        ef.findBestFactor();
        return ef;
    }

}