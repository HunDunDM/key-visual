/**
 * Created by 混沌DM on 2019/10/24.
 */

import { createStore, applyMiddleware } from 'redux';
import logger from 'redux-logger';
import thunk from 'redux-thunk';
import { persistStore, persistReducer } from 'redux-persist';
import localForage from 'localforage';

import appReducer from './reducer';

const config = {
  key: 'root',
  storage: localForage,
  whitelist: ['persist'],
};

function createAppStore() {
  const store = createStore(
    persistReducer(config, appReducer),
    process.env.NODE_ENV !== 'production' ? applyMiddleware(thunk, logger) : applyMiddleware(thunk),
  );
  const persistor = persistStore(store);
  return { store, persistor };
}

export default createAppStore;
