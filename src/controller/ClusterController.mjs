import { Controller } from 'rda-service';
import type from 'ee-types';
import logd from 'logd';
import RDALockClient from '@infect/rda-lock-client';
import Delay from '@distributed-systems/delay';
import HTTP2Client from '@distributed-systems/http2-client';


const log = logd.module('rda-coordinator');


/**
 * Create clusters for data sources. Data sources are services that provide data sets for RDA.
 */
export default class ClusterController extends Controller {

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
        super('cluster');

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


        this.httpClient = new HTTP2Client();


        this.enableAction('create');
        this.enableAction('listOne');
    }




    /**
     * unload the http client
     */
    async end() {
        await this.httpClient.end();
    }



    /**
    * get the status of a cluster
    *
    * @param      {Express.request}   request   express request
    * @param      {Express.response}  response  express response
    * @return     {Promise}  cluster info
    */
    async listOne(request) {
        return this.getClusterInfo(request.getParameter('id'));
    }







    /**
    * track the cluster status until it has failed or is built. Free the lock as soon the cluster
    * is created.
    *
    * @param      {number}   clusterId  cluster id
    * @param      {Lock}     lock       lock instance
    * @return     {Promise}  udnefined
    */
    async pollClusterCompletion(clusterId, lock) {
        const clusterInfo = await this.getClusterInfo(clusterId);

        if (['active', 'ended', 'failed'].includes(clusterInfo.status)) {
            // remove the lock so that th cluster can be modified by others

            await lock.free();
        } else if (['created', 'initialized'].includes(clusterInfo.status)) {
            // wait and try again

            await new Delay().wait(5000);
            await this.pollClusterCompletion(clusterId, lock);
        } else {
            throw new Error(`The cluster info returned an unknown status: ${clusterInfo.status}`);
        }
    }







    /**
    * load the cluster info
    *
    * @param      {number}   clusterId  cluster id
    * @return     {Promise}  clsuter info
    */
    async getClusterInfo(clusterId) {
        const clusterHost = await this.registryClient.resolve('rda-cluster');
        const clusterUrl = `${clusterHost}/rda-cluster.cluster/${clusterId}`;
        const response = await this.httpClient.get(clusterUrl)
            .expect(200)
            .send();

        return response.getData();
    }







    /**
     * check if a cluster exists
     *
     * @param      {string|number}   clusterId  the clsuters id or identifier
     * @return     {Promise}  boolean true if it exists
     */
    async clusterExists(clusterId) {
        const clusterHost = await this.registryClient.resolve('rda-cluster');
        const clusterStatusURL = `${clusterHost}/rda-cluster.cluster/${clusterId}`;
        const clusterStatusResponse = await this.httpClient.get(clusterStatusURL)
            .expect(404, 200)
            .send();

        return clusterStatusResponse.status(200);
    }






    /**
    * sets up a new cluster:
    * 1. check the cluster service about existing clusters
    * 2. get the data requirements from the data source
    * 3. request reasonably sized cluster at the cluster service
    * 4. tell the data source to create shards for the cluster
    * 5. tell the cluster to initialize
    * 6. enjoy!
    *
    * @param      {Express.request}   request   express request
    * @param      {Express.response}  response  express response
    * @return     {Promise}           object containing the clusters description
    */
    async create(request) {
        const data = await request.getData();

        if (!data) {
            request.response().status(400).send('Missing request body!');
        } else if (!type.object(data)) {
            request.response().status(400).send('Request body must be a json object!');
        } else if (!type.string(data.dataSource)) {
            request.response().status(400).send('Missing parameter \'dataSource\' in request body!');
        } else if (!type.string(data.dataSet)) {
            request.response().status(400).send('Missing parameter \'dataSet\' in request body!');
        } else if (!this.dataSources.has(data.dataSource)) {
            request.response().status(404).send(`The data source ${data.dataSource} was not found!`);
        } else {
            const clusterIdentifier = `${data.dataSource}/${data.dataSet}`;


            // get a lock for the cluster to be created, but don't wait too long
            // for it since the cluster should not be created when it already exists.
            const lock = this.lockClient.createLock(`cluster::${clusterIdentifier}`, {
                timeout: 10,
                ttl: 120,
            });

            await lock.lock().catch((err) => {
                throw new Error(`Failed to acquire lock for creating the cluster ${clusterIdentifier}: ${err.message}`);
            });


            const clusterExists = await this.clusterExists(clusterIdentifier);

            // make sure neither we, nor the cluster service knows
            // a thing about the cluster to be created
            if (!clusterExists) {

                // get the service address of the data source
                const storageHost = await this.registryClient.resolve(data.dataSource);


                // request information about the data that will be processed by the cluster. this is
                // needed to size the cluster correctly
                log.info('Getting information about the data that has to be loaded into the cluster');
                const dataInfoURL = `${storageHost}/${data.dataSource}.dataset-info/${data.dataSet}`;
                const dataInfoResponse = await this.httpClient.get(dataInfoURL)
                    .expect(200)
                    .send();

                const info = await dataInfoResponse.getData();



                // create the cluster
                log.info('Setting up the cluster');
                const clusterHost = await this.registryClient.resolve('rda-cluster');
                const clusterResponse = await this.httpClient.post(`${clusterHost}/rda-cluster.cluster`)
                    .expect(201)
                    .send({
                        requiredMemory: info.totalMemory,
                        recordCount: info.recordCount,
                        dataSet: data.dataSet,
                        dataSource: data.dataSource,
                    });


                const clusterData = await clusterResponse.getData();

                // instruct the data source to create the shards
                log.info('Instruct the datasource to create data shards');
                await this.httpClient.post(`${storageHost}/${data.dataSource}.shard`)
                    .expect(201)
                    .send({
                        shards: clusterData.shards,
                        dataSet: data.dataSet,
                    });


                // tell the cluster to initialize
                log.info('Initialize cluster');
                const { clusterId } = clusterData;
                const clusterInitilizeURL = `${clusterHost}/rda-cluster.cluster/${clusterId}`;
                await this.httpClient.patch(clusterInitilizeURL)
                    .expect(200)
                    .send();


                // start polling the clusters status
                this.pollClusterCompletion(clusterId, lock).catch(log.error);


                // return some info. the user has to get the status info using the lsitOne method
                return {
                    clusterId,
                    clusterIdentifier: clusterData.clusterIdentifier,
                    dataSet: data.dataSet,
                    dataSource: data.dataSource,
                    recordCount: info.recordCount,
                    requiredMemory: info.totalMemory,
                    shards: clusterData.shards,
                };
            } else {
                request.response()
                    .status(409)
                    .send(`Canont create cluster: the cluster '${clusterIdentifier}' exists already!`);
            }
        }
    }
}
