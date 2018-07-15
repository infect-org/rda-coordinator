'use strict';


import StatusHandler from 'status-handler';



export default class ClusterStatus extends StatusHandler {

    constructor() {
        super([
            ['blank', 1000],
            ['initializing', 2000],
            ['informing', 3000],
            ['requesting', 4000],
            ['sharding', 5000],
            ['preparing', 6000],
            ['ready', 7000],
            ['ended', 10000],
            ['failed', 10000],
        ]);


        this.setStatus('blank');
    }
}