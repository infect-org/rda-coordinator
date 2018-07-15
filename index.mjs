'use strict';

import Service from './src/Service';
import logd from 'logd';
import ConsoleTransport from 'logd-console-transport';

// enable console logging
logd.transport(new ConsoleTransport());



export {
    Service as default
};