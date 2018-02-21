package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.model.policy.BEExpression;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

/**
 * Created by cygnus on 2/20/18.
 */
public class FactorizationTest {


    BEExpression beExpression;
    Factorization f;
    List<String> output;

    @Before
    public void setUp() throws Exception {
        beExpression = new BEExpression();
        f = new Factorization();
        output = new ArrayList<String>();
        output.add(Reader.readTxt("src/test/resources/ApproximateFactor/policy4.txt"));
        output.add(Reader.readTxt("src/test/resources/ApproximateFactor/policy5_2.txt"));
        output.add(Reader.readTxt("src/test/resources/ApproximateFactor/policy6.txt"));
    }

    @Test
    @DisplayName("Test using policy4.json with integer(temperature)")
    public void approximateFactorization() throws Exception {
        beExpression.parseJSONList(Reader.readFile("/policies/policy4.json"));
        f = new Factorization(beExpression);
        f.approximateFactorization();
        assertThat(
                f.getExpression().createQueryFromPolices(),
                equalTo(output.get(0)));
    }


    @Test
    @DisplayName("Test using policy5.json with integers(temperature and energy)")
    public void approximateFactorization1() throws Exception {
        beExpression.parseJSONList(Reader.readFile("/policies/policy5_2.json"));
        f = new Factorization(beExpression);
        f.approximateFactorization();
        assertThat(
                f.getExpression().createQueryFromPolices(),
                equalTo(output.get(1)));
    }

    @Test
    @DisplayName("Test using policy5.json with timestamps")
    public void approximateFactorization2() throws Exception {
        beExpression.parseJSONList(Reader.readFile("/policies/policy6.json"));
        f = new Factorization(beExpression);
        f.approximateFactorization();
        assertThat(
                f.getExpression().createQueryFromPolices(),
                equalTo(output.get(2)));
    }
}