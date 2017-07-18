package ml.dmlc.xgboost4j.java;

/**
 * @author Thomas Moerman
 */
public class CVPack {

    /**
     * Static helper method. Necessary to call the CV routines from here, otherwise XGBoost crashes in
     * unpredictable and non-deterministic ways. Reason unclear.
     *
     * @param cvPack
     * @param nrRounds
     * @return Returns an Array of Strings with the evaluation results
     * @throws XGBoostError
     */
    public static String[] crossValidation(CVPack cvPack, int nrRounds) throws XGBoostError {
        final String[] evalHist = new String[nrRounds];

        for (int i = 0; i < nrRounds; i++) {
            evalHist[i] = cvPack.updateAndEval(i);
        }

        return evalHist;
    }

    final DMatrix dtrain;
    final DMatrix dtest;
    final DMatrix[] dmats;
    final String[] names;
    final Booster booster;

    public CVPack(DMatrix dtrain, DMatrix dtest, Booster booster) throws XGBoostError {
        this.dmats      = new DMatrix[]{dtrain, dtest};
        this.booster    = booster;
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

}