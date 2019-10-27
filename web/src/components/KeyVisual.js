/**
 * Created by 混沌DM on 2019/10/24.
 */

import React, {PureComponent} from 'react';
import {connect} from 'react-redux';
import {Label} from 'semantic-ui-react';

import actions from "../actions";
import {convert, markColor} from '../heat_map';

const Colors = ({ labels }) => (
  <div className="flex-none flex-col flex-justify-start colors">
    {labels.map(({ text, textColor, background }, i) => (
      <div key={i} className="padding">
        <Label style={{color: textColor, background}} content={text} />
      </div>
    ))}
  </div>
);

export default connect(state => ({
  data: state.display.data,
  keys: state.display.keys,
  times: state.display.times,
  labels: state.display.labels,
  statPreference: state.persist.settings.statPreference,
  dataPreference: state.persist.settings.dataPreference,
}))(
  class KeyVisual extends PureComponent {
    onUnitClick = (i, j) => {
      const { dispatch, data, keys, times, statPreference } = this.props;
      const endTime = new Date(times[times.length - 1]);
      const value = { [statPreference]: data[i][j][statPreference] };
      value.start_key = keys[j];
      value.end_key = keys[j + 1];
      value.start_time = timeDelta(endTime - new Date(times[i]));
      value.end_time = timeDelta(endTime - new Date(times[i + 1]));
      dispatch(actions.Attention(value));
    };
    onInfoClick = j => {
      const { dispatch, labels } = this.props;
      dispatch(actions.Attention(labels[j]));
    };
    render() {
      const {data, labels, statPreference, dataPreference } = this.props;
      const [values, colorLabels] = convert(data, statPreference, dataPreference);
      const divMatrix = values.map((axis, i) => (
        <div key={i} className="flex-auto flex-col">
          {axis.map((color, j) => (
            <div key={j} className="flex-auto stat-unit" style={{background: color}} onClick={() => this.onUnitClick(i, j)}/>
          ))}
        </div>
      ));
      return (
        <div className="flex-auto visual-background padding flex-row">
          <Colors labels={colorLabels} />
          <div className="flex-none flex-col labels">
            {markColor(labels).map((background, j) => (
              <div key={j} className="flex-auto stat-info" style={{background}} onClick={() => this.onInfoClick(j)} />
            ))}
          </div>
          {divMatrix}
        </div>
      );
    }
  }
)

function timeDelta(delta) {
  console.log("timedelta input: ", delta);
  delta = Math.round(delta / 1000);

  const s = delta % 60;
  delta = Math.round((delta - s) / 60);
  const m = delta % 60;
  delta = Math.round((delta - m) / 60);
  const h = delta % 24;
  const d = Math.round((delta - h) / 24);

  const output = ['-'];
  if (d) output.push(d.toString(), 'd');
  if (h) output.push(h.toString(), 'h');
  if (m) output.push(m.toString(), 'm');
  if (s) output.push(s.toString(), 's');
  console.log("timedelta output: ", output);
  return output.length === 1 ? 'now' : output.join('');
}
