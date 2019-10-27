/**
 * Created by 混沌DM on 2019/10/24.
 */
import React from 'react';

import AppHeader from './AppHeader';
import KeyVisual from "./KeyVisual";
import Follow from './Follow';

export default () => (
  <div className="flex-col main-screen">
    <AppHeader />
    <div className="flex-row flex-auto" style={{overflow: 'hidden'}}>
      <Follow />
      <KeyVisual />
    </div>
  </div>
);
