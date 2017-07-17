/*
 Copyright (c) 2014 by Contributors

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package ml.dmlc.xgboost4j.java;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * trainer for xgboost
 *
 * @author hzx
 */
public class XGBoostTMO {

    private static final Log logger = LogFactory.getLog(XGBoost.class);

    public static String[] crossValidation(
            DMatrix data,
            CVPack[] cvPacks,
            int round,
            int nfold) throws XGBoostError {

        String[] evalHist = new String[round];
        String[] results = new String[cvPacks.length];

        for (int i = 0; i < round; i++) {
            for (CVPack cvPack : cvPacks) {
                cvPack.update(i);
            }

            for (int j = 0; j < cvPacks.length; j++) {
                results[j] = cvPacks[j].eval(i);
            }

            evalHist[i] = results[0];
        }

        return evalHist;
    }

    /**
     * cross validation package for xgb
     *
     * @author hzx
     */
    public static class CVPack {
        DMatrix dtrain;
        DMatrix dtest;
        DMatrix[] dmats;
        String[] names;
        Booster booster;


        public CVPack(DMatrix dtrain, DMatrix dtest, Booster booster) throws XGBoostError {
            dmats = new DMatrix[]{dtrain, dtest};
            this.booster = booster;
            names = new String[]{"train", "test"};
            this.dtrain = dtrain;
            this.dtest = dtest;
        }

        /**
         * update one iteration
         *
         * @param iter iteration num
         * @throws XGBoostError native error
         */
        public void update(int iter) throws XGBoostError {
            booster.update(dtrain, iter);
        }


        /**
         * evaluation
         *
         * @param iter iteration num
         * @return evaluation
         * @throws XGBoostError native error
         */
        public String eval(int iter) throws XGBoostError {
            return booster.evalSet(dmats, names, iter);
        }

    }
}
