'use strict';


import RDAService from 'rda-service';
import path from 'path';
import logd from 'logd';

const log = logd.module('rda-service-registry');



// controllers
import ClusterController from './controller/Cluster';







export default class RDACoordinatorService extends RDAService {


    constructor() {
        super('rda-coordinator');
    }




    /**
    * prepare the service
    */
    async load() {
        // get a map of dtaa sources
        this.dataSources = new Set(this.config.dataSources);


        // register controllers
        this.registerController(new ClusterController({
            dataSources: this.dataSources,
            registryClient: this.registryClient,
        }));


        await super.load();


        // tell the service registry that we're up and running
        await this.registerService();
    }
}