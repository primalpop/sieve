package model.guard;

import fileop.Reader;
import model.guard.ExactFactor;
import model.policy.BEExpression;
import model.policy.BooleanCondition;
import model.policy.BooleanPredicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Author primpap
 */
class ExactFactorTest {

    BEExpression beExpression;
    ExactFactor ef;
    List <String> policies;

    @BeforeEach
    void setUp() {
        beExpression = new BEExpression();
        ef = new ExactFactor();
        policies = new ArrayList<String>();
        policies.add(Reader.readFile("policy0.txt"));
        policies.add(Reader.readFile("policy1.txt"));
        policies.add(Reader.readFile("policy2.txt"));
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void greedyFactorization1(){
        ef = returnBestFactor("/policies/policy0.json");
        assert(ef.createQueryFromExactFactor().equals(policies.get(0)));
        assert(ef.getMultiplier().isEmpty());
    }

    @Test
    void greedyFactorization2(){
        ef = returnBestFactor("/policies/policy1.json");
        assert(ef.createQueryFromExactFactor().equals(policies.get(1)));
    }

    @Test
    void greedyFactorization3(){
        ef = returnBestFactor("/policies/policy3.json");
        assert(ef.createQueryFromExactFactor().equals(policies.get(3)));
    }


    ExactFactor returnBestFactor(String filename) {
        beExpression.parseJSONList(Reader.readFile(filename));
        System.out.printf("Finding exact factor for %s: %s", filename, beExpression.createQueryFromPolices());
        ExactFactor ef = new ExactFactor(beExpression);
        ef.findBestFactor();
        return ef;
    }

}