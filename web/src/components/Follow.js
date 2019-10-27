/**
 * Created by 混沌DM on 2019/10/25.
 */

import React, {PureComponent} from 'react';
import {connect} from 'react-redux';
import {Message} from 'semantic-ui-react';

export default connect(state => ({
  attentionList: state.display.attentionList,
}))(
  class Follow extends PureComponent {
    render() {
      const { attentionList } = this.props;
      const labels = attentionList.map((msg, id) => {
        return (
          <div key={id} className="padding">
            <Message content={formatString(msg)} />
          </div>
        )
      });
      return (
        <div className="flex-col flex-none follow-outer no-scrollbar">
          <div className="flex-col flex-none flex-justify-start follow-inner">
            {labels}
          </div>
        </div>
      )
    }
  }
)

const maxLineLength = 20;

function formatString(s) {
  let substrList = [];
  for (let i = 0; s.length > maxLineLength; i++) {
    substrList.push(s.substr(0, maxLineLength));
    s = s.substr(maxLineLength);
  }
  substrList.push(s);
  return substrList.join('\n');
}
