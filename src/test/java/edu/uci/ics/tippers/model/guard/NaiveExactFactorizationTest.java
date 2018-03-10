package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.model.policy.BEExpression;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Author primpap
 */
public class NaiveExactFactorizationTest {

    BEExpression beExpression;
    NaiveExactFactorization ef;
    List <String> policies;

    @Before
    public void setUp() {
        beExpression = new BEExpression();
        ef = new NaiveExactFactorization();
        policies = new ArrayList<String>();
        policies.add(Reader.readTxt("src/test/resources/NaiveExactFactorization/policy0.txt"));
        policies.add(Reader.readTxt("src/test/resources/NaiveExactFactorization/policy1.txt"));
        policies.add(Reader.readTxt("src/test/resources/NaiveExactFactorization/policy2_1.txt"));
        policies.add(Reader.readTxt("src/test/resources/NaiveExactFactorization/policy2_2.txt"));

    }

    @After
    public void tearDown() {
    }

    @Test
    public void greedyFactorization1(){
        ef = returnBestFactor("/policies/policy0.json");
        assert(ef.createQueryFromExactFactor().equals(policies.get(0)));
        assertThat(
                ef.getMultiplier(),
                Matchers.hasSize(0));
    }

    @Test
    public void greedyFactorization2(){
        ef = returnBestFactor("/policies/policy1.json");
        assertThat(
                ef.createQueryFromExactFactor(),
                equalTo(policies.get(1)));
    }

    @Ignore
    @Test
    public void greedyFactorization3(){
        ef = returnBestFactor("/policies/policy2.json");
        assertThat(
                ef.createQueryFromExactFactor(),
                anyOf(equalTo(policies.get(3))));
    }


    NaiveExactFactorization returnBestFactor(String filename) {
        beExpression.parseJSONList(Reader.readFile(filename));
        NaiveExactFactorization ef = new NaiveExactFactorization(beExpression);
        ef.greedyFactorization();
        return ef;
    }
}