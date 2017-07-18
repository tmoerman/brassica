package ml.dmlc.xgboost4j.java;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Thomas Moerman
 */
public class CVPack {

    public static String[] evalRounds(CVPack cvPack, int[] rounds) throws XGBoostError {
        final String[] evalHist = new String[rounds.length];

        for (int i = 0; i < rounds.length; i++) {
            evalHist[i] = cvPack.updateAndEval(rounds[i]);
        }

        return evalHist;
    }

    public interface Predicate<E> {

        /**
         * @param evalHist
         * @return an Array of int index, hijacked to emulate a Scala Option.
         */
        E apply(List<String> evalHist);

        boolean isDefined(E e);

    }

    public static <E> E updateWhile(CVPack cvPack, int nrRounds, int batch, Predicate<E> predicate) throws XGBoostError {
        final List<String> evalHist = new ArrayList<>();

        int nrRoundsCompleted = 0;

        E result;

        do {

            for (int i = 0; i < batch; i++) {
                final int currentRound = nrRoundsCompleted + i;
                final String eval = cvPack.updateAndEval(currentRound);

                evalHist.add(eval);
            }

            nrRoundsCompleted += batch;

            result = predicate.apply(evalHist);

        } while (
                (! predicate.isDefined(result)) &&
                nrRoundsCompleted < nrRounds);

        return result;
    }

    final DMatrix dtrain;
    final DMatrix dtest;
    final DMatrix[] dmats;
    final String[] names;
    final Booster booster;

    public CVPack(DMatrix dtrain, DMatrix dtest, Map<String, Object> params) throws XGBoostError {
        this.dmats      = new DMatrix[]{dtrain, dtest};
        this.booster    = new Booster(params, dmats);
        this.names      = new String[]{"train", "test"};
        this.dtrain     = dtrain;
        this.dtest      = dtest;
    }

    public String updateAndEval(int iteration) throws XGBoostError {
        booster.update(dtrain, iteration);

        return booster.evalSet(dmats, names, iteration);
    }

    public void dispose() throws XGBoostError {
        booster.dispose();
        dtrain.dispose();
        dtest.dispose();
    }

    public Booster getBooster() {
        return booster;
    }

}