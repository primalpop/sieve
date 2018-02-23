package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.model.policy.BEExpression;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class CombinedTest {

    BEExpression beExpression;
    ExactFactor ef;
    Factorization f;
    List<String> output;

    @Before
    public void setUp() throws Exception {
        beExpression = new BEExpression();
        f = new Factorization();
        ef = new ExactFactor();
        output = new ArrayList<String>();
        output.add(Reader.readTxt("src/test/resources/Combined/policy11.txt"));
    }

    @Ignore()
    @Test
    @DisplayName("Test using policy11.json with 6 policies and 2 approximate factors (time and energy)")
    public void approximateFactorization() throws Exception {
        beExpression.parseJSONList(Reader.readFile("/policies/policy11.json"));
        f = new Factorization(beExpression);
        f.approximateFactorization();
        ef = new ExactFactor(f.getExpression());
        ef.greedyFactorization();
        assertThat(
                ef.createQueryFromExactFactor(),
                equalTo(output.get(0)));
    }
}
