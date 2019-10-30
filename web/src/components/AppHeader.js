/**
 * Created by 混沌DM on 2019/10/24.
 */
import React, {PureComponent} from 'react';
import {connect} from 'react-redux';
import {Input, Radio, Button, Label} from 'semantic-ui-react';

import actions from "../actions";
import {defaultSettings, statPreferenceEnum, dataPreferenceEnum} from "../config";

export default connect(state => ({ settings: state.persist.settings }))(
  class AppHeader extends PureComponent {
    state = { loading: false };

    refresh = async (isForce = false) => {
      this.setState({ loading: true });
      await this.props.dispatch(actions.Update(isForce));
      this.setState({ loading: false });
    };

    forceRefresh = () => this.refresh(true);

    onChange = (key, value) => {
        this.props.dispatch(actions.Settings({
            [key]: value,
        }));
        this.refresh(false)
    }

    onServerChange = e => this.onChange('server', e.target.value);
    onStartKeyChange = e => this.onChange('startKey', e.target.value);
    onEndKeyChange = e => this.onChange('endKey', e.target.value);
    onStartTimeChange = e => this.onChange('startTime', e.target.value);
    onEndTimeChange = e => this.onChange('endTime', e.target.value);

    onAutoFreshChange = () => this.onChange('autoFresh', !this.props.settings.autoFresh);
    onChooseMax = () => this.onChange('statPreference', statPreferenceEnum.max.value);
    onChooseAverage = () => this.onChange('statPreference', statPreferenceEnum.average.value);
    onChooseLoad = () => this.onChange('dataPreference', dataPreferenceEnum.load.value);
    onChooseRead = () => this.onChange('dataPreference', dataPreferenceEnum.read.value);
    onChooseWrite = () => this.onChange('dataPreference', dataPreferenceEnum.write.value);

    componentDidMount() {
      this.refresh(true);
      this.refreshInterval = setInterval(this.refresh, 30000);
    }

    componentWillUnmount() {
      clearInterval(this.refreshInterval)
    }

    render() {
      let {
        server = '',
        startKey = '',
        endKey = '',
        startTime = '',
        endTime = '',
        autoFresh = true,
        statPreference = statPreferenceEnum.max.value,
        dataPreference = dataPreferenceEnum.load.value,
      } = this.props.settings;
      let { loading } = this.state;

      let endTimeInputProps;
      if (autoFresh) {
        endTimeInputProps = {
          key: 'endTimeInputDisabled',
          value: '',
          disabled: true,
        }
      } else {
        endTimeInputProps = {
          key: 'endTimeInput',
          defaultValue: endTime,
          onChange: this.onEndTimeChange,
        }
      }

      return (
        <div className="flex-col flex-none">
          <div className="flex-row flex-nowrap padding">
            <Input
              label="Server: "
              placeholder={defaultSettings.server.display}
              style={{flex: 4}}
              defaultValue={server}
              onChange={this.onServerChange}
            />
            <Input
              className="flex-single"
              label="Start Key: "
              placeholder={defaultSettings.startKey.display}
              defaultValue={startKey}
              onChange={this.onStartKeyChange}
            />
            <Input
              className="flex-single"
              label="End Key: "
              placeholder={defaultSettings.endKey.display}
              defaultValue={endKey}
              onChange={this.onEndKeyChange}
            />
            <Input
              className="flex-single"
              label="Start Time: "
              placeholder={defaultSettings.startTime.display}
              defaultValue={startTime}
              onChange={this.onStartTimeChange}
            />
            <Input
              {...endTimeInputProps}
              className="flex-single"
              label="End Time: "
              placeholder={defaultSettings.endTime.display}
            />
          </div>
          <div className="flex-row flex-nowrap padding">
            <div className="flex-row flex-justify-start" style={{flex: 1}}>
              <Label>Statistical Preference</Label>
              <Radio
                className="non-row-head"
                label={statPreferenceEnum.max.display}
                checked={statPreference === statPreferenceEnum.max.value}
                onClick={this.onChooseMax}
              />
              <Radio
                className="non-row-head"
                label={statPreferenceEnum.average.display}
                checked={statPreference === statPreferenceEnum.average.value}
                onClick={this.onChooseAverage}
              />
            </div>
            <div className="flex-row flex-justify-start" style={{flex: 1}}>
              <Label>Data Preference</Label>
              <Radio
                className="non-row-head"
                label={dataPreferenceEnum.load.display}
                checked={dataPreference === dataPreferenceEnum.load.value}
                onClick={this.onChooseLoad}
              />
              <Radio
                className="non-row-head"
                label={dataPreferenceEnum.read.display}
                checked={dataPreference === dataPreferenceEnum.read.value}
                onClick={this.onChooseRead}
              />
              <Radio
                className="non-row-head"
                label={dataPreferenceEnum.write.display}
                checked={dataPreference === dataPreferenceEnum.write.value}
                onClick={this.onChooseWrite}
              />
            </div>
            <div className="flex-row flex-justify-end" style={{flex: 1}}>
              <Radio toggle label="Auto Refresh" checked={autoFresh} onClick={this.onAutoFreshChange} />
              <div className="non-row-head">
                <Button primary disabled={loading} loading={loading} onClick={this.forceRefresh}>Query</Button>
              </div>
            </div>
          </div>
        </div>
      )
    }
  }
)
