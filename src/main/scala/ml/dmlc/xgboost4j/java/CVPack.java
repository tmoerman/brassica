package ml.dmlc.xgboost4j.java;

/**
 * @author Thomas Moerman
 */
public class CVPack {

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