/**
 * Created by 混沌DM on 2019/10/24.
 */

import { combineReducers } from 'redux';
import { createReducer } from "redux-act";
import actions from "./actions";

import {defaultSettingsState, defaultDisplayState} from './config';

export default combineReducers({
  persist: combineReducers({
    settings: createReducer({
      [actions.Settings]: (state, payload) => ({...state, ...payload}),
    }, defaultSettingsState),
  }),
  display: createReducer({
    [actions.Display]: (state, payload) => ({...state, ...payload}),
  }, defaultDisplayState),
});
