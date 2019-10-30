import {dataPreferenceEnum, statPreferenceEnum, heat_map_colors, heat_map_gamma, label_colors} from "./config";
y
function normalize(channel) {
  return Math.pow(channel / 255, heat_map_gamma);
}

const colorStrArray = ['rgb(', '', ',', '', ',', '', ')'];

function gradientColor(start, end, step, range) {
  start = start.map(normalize);
  end = end.map(normalize);
  const ms = step / range;
  const me = 1 - ms;
  let output = colorStrArray.slice(0);
  for (let i = 0; i < 3; i++) {
    output[i * 2 + 1] = Math.round(Math.pow(start[i] * me + end[i] * ms, 1 / heat_map_gamma) * 255)
  }
  return output.join('');
}

function toColor(value, ceiling) {
  value = Math.log((value || 0) + 1) / ceiling;
  let i = 1;
  for(; heat_map_colors[i].value < value && i < heat_map_colors.length; i++) {}
  return gradientColor(
    heat_map_colors[i-1].color,
    heat_map_colors[i].color,
    value - heat_map_colors[i-1].value,
    heat_map_colors[i].value - heat_map_colors[i-1].value,
  );
}

function generateColorLabels(ceiling) {
  return heat_map_colors.map(({ value, background, textColor }) => ({
    background,
    textColor,
    text: Math.round(Math.pow(Math.E, ceiling * value) - 1).toString(),
  }));
}

export function convert(data, statPreference, dataPreference) {
  const convertFunc = value => value;
  let maxValue = 0;
  const values = data.map(axis => axis.map(statUnit => {
    const value = convertFunc(statUnit);
    if (value > maxValue) maxValue = value;
    return value;
  }));
  const ceiling = Math.log(Math.max(maxValue + 1, 100));
  return [values.map(axis => axis.map(value => toColor(value, ceiling))), generateColorLabels(ceiling)];
}

export function markColor(labels) {
  let p = 0;
  return labels.map(({ start_key, end_key, labels: names}, j) => {
    let equal = false;
    if (j > 0 && names.length === labels[j - 1].labels.length) {
      equal = true;
      names.forEach((name, i) => {
        if (name !== labels[j-1].labels[i]) {
          equal = false;
        }
      });
    }
    if (j === 0 || ((start_key !== labels[j-1].start_key || end_key !== labels[j-1].end_key) && !equal)) {
      p = p + 1;
    }
    return label_colors[p % label_colors.length];
  });
}
