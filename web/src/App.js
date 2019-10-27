import React, {PureComponent} from 'react';
import {Provider} from 'react-redux';
import {PersistGate} from 'redux-persist/es/integration/react';

import createAppStore from './store';
import AppFrame from './components';

class App extends PureComponent {
  state = createAppStore();

  render() {
    const {store, persistor} = this.state;
    return (
      <Provider store={store}>
        <PersistGate loading={null} persistor={persistor}>
          <AppFrame />
        </PersistGate>
      </Provider>
    );
  }
}

export default App;
