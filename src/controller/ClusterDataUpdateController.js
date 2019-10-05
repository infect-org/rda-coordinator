import { Controller } from '@infect/rda-service';
import type from 'ee-types';
import logd from 'logd';
import RDALockClient from '@infect/rda-lock-client';



const log = logd.module('rda-coordinator');


/**
 * Update running clusters; add or remove data versions
 */
export default class ClusterDataUpdateController extends Controller {

    /**
     * @param      {Object}          arg1                 options
     * @param      {array}           arg1.dataSources     array containing the name of available
     *                                                    data sources (at the time of writing this
     *                                                    is just the infect-rda-sample-strorage)
     * @param      {RegistryClient}  arg1.registryClient  The registry client
     */
    constructor({
        dataSources,
        registryClient,
    }) {
        super('clusterDataUpdate');

        // urls to remote services
        this.registryClient = registryClient;

        // the data sources that provide data and functions to execute
        // on the data
        this.dataSources = dataSources;

        // get locks for when working with clusters, we don't want multiple
        // processes modify clusters at the same time
        this.lockClient = new RDALockClient({
            registryClient: this.registryClient,
        });

        this.enableAction('create');
    }






    /**
     * add or remove data for a running cluster
     *
     * @param      {Express.Request}   request   express request
     * @param      {Exoress.Response}  response  express response
     * @return     {Promise}           undefined
     */
    async create(request, response) {
        
    }
}
