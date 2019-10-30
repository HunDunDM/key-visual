/**
 * Created by 混沌DM on 2019/10/25.
 */

export const statPreferenceEnum = {
  max: {
    display: 'Max',
    value: 'max',
    func: value => value.max
  },
  average: {
    display: 'Total',
    value: 'average',
    func: value => value.average
  },
};

export const dataPreferenceEnum = {
  load: {
    display: 'Load',
    value: 'load',
    func: value => value.value,
  },
  read: {
    display: 'Read',
    value: 'read',
    func: value => value.value,
  },
  write: {
    display: 'Write',
    value: 'write',
    func: value => value.value,
  },
};

export const defaultSettingsState = {
  server: '',
  startKey: '',
  endKey: '',
  startTime: '',
  endTime: '',
  autoFresh: true,
  statPreference: statPreferenceEnum.max.value,
  dataPreference: dataPreferenceEnum.load.value,
};

// 默认state下的实际默认取值
export const defaultSettings = {
  server: {
    display: 'XXX.XXX.XX.XX:PORT',
    value: '',
  },
  startKey: {
    display: 'Default:  - ∞',
    value: '',
  },
  endKey: {
    display: 'Default:  + ∞',
    value: '~'
  },
  startTime: {
    display: 'Default: -60m',
    value: '-60m',
  },
  endTime: {
    display: 'Default: now',
    value: '0m',
  },
};

export const defaultDisplayState = {
  data: [],
  keys: [],
  times: [],
  labels: [],
  attentionList: [],
};

export const heat_map_colors = [
  {
    value: 0,
    color: [0, 0, 0],
    background: 'rgb(0,0,0)',
    textColor: 'white',
  },
  {
    value: 0.4,
    color: [63, 4, 115],
    background: 'rgb(63,4,115)',
    textColor: 'white',
  },
  {
    value: 0.6,
    color: [114, 8, 123],
    background: 'rgb(114,8,123)',
    textColor: 'white',
  },
  {
    value: 0.75,
    color: [177, 13, 86],
    background: 'rgb(177,13,86)',
    textColor: 'white',
  },
  {
    value: 0.85,
    color: [253, 200, 53],
    background: 'rgb(253,200,53)',
    textColor: 'rgba(0, 0, 0, 0.9)',
  },
  {
    value: 0.9,
    color: [254, 255, 63],
    background: 'rgb(254,255,63)',
    textColor: 'rgba(0, 0, 0, 0.9)',
  },
  {
    value: 1,
    color: [254, 255, 176],
    background: 'rgb(254,255,176)',
    textColor: 'rgba(0, 0, 0, 0.9)',
  },
];

export const label_colors = [
  '#98df8a',
  '#2ca02c',
  '#1f77b4',
  '#17becf',
];

export const heat_map_gamma = 1;
