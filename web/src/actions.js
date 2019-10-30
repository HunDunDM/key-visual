/**
 * Created by 混沌DM on 2019/10/24.
 */

import { createAction } from 'redux-act';

import { defaultSettings } from "./config";

const types = {
  Settings: createAction('SETTING'),
  Display: createAction('DISPLAY'),
};

export default {
  ...types,
  Update: (force = false) => async (dispatch, getState) => {
    const { server, startKey, endKey, startTime, endTime, autoFresh, statPreference, dataPreference} = getState().persist.settings;
    if ((!force && !autoFresh) || (!server)) return;
    const params = new URLSearchParams();
    params.set('startkey', startKey || defaultSettings.startKey.value);
    params.set('endkey', endKey || defaultSettings.endKey.value);
    params.set('starttime', startTime || defaultSettings.startTime.value);
    params.set('endtime', autoFresh || !endTime ? defaultSettings.endTime.value : endTime);
    params.set('tag', dataPreference);
    params.set('mode', statPreference);
    const uri = ['http://', server, '/heatmaps?', params.toString()].join('');
    try {
      console.log("Try Fetch: ", uri);
      // await (new Promise((resolve) => setTimeout(resolve, 2000)));
      // dispatch(types.Display(mockResponse))
      const response = await fetch(uri);
      const matrix = await response.json();
      dispatch(types.Display(matrix));
    } catch (e) {
      console.error("Fetch ERROR: ", uri);
    }
  },
  Attention: (obj) => (dispatch, getState) => {
    let {attentionList} = getState().display;
    const msg = JSON.stringify(obj, undefined, 2);
    if (attentionList.length > 0 && attentionList[attentionList.length - 1] === msg) {
      return;
    }
    attentionList = attentionList.slice(attentionList.length >= 5 ? 1 : 0);
    attentionList.push(msg);
    dispatch(types.Display({ attentionList }));
  },
};
