import Service from './src/Service.js';
import logd from 'logd';
import ConsoleTransport from 'logd-console-transport';

// enable console logging
logd.transport(new ConsoleTransport());



export {
    Service as default
};