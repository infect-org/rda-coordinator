import { Controller } from 'rda-service';
import superagent from 'superagent';
import type from 'ee-types';
import logd from 'logd';
import ClusterStatus from '../ClusterStatus.mjs';



const log = logd.module('rda-coordinator');


/**
 * this is the brain of RDA, it controls the clusters and its modifications
 */
export default class ClusterController extends Controller {

    /**
     * @param      {Object}          arg1                 options
     * @param      {array}           arg1.dataSources     array containing the name of available
     *                                                    data sources (at the time of writing this
     *                                                    is just the rda-sample-strorage)
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

        // we're keeping track of the status
        // of all clusters we're building
        this.clusterStatus = new Map();


        this.enableAction('create');
    }






    /**
    * sets up a new cluster
    * 1. check the cluster service about existing clusters
    * 2. get the data requirements from the data source
    * 3. request reasonably sized cluster at the cluster service
    * 4. tell the data source to create shards for the cluster
    * 5. tell the cluster to initialize
    * 6. enjoy!
    */
    async create(request, response) {
        const data = request.body;

        if (!data) response.status(400).send('Missing request body!');
        else if (!type.object(data)) {
            response.status(400).send('Request body must be a json object!');
        } else if (!type.string(data.dataSource)) {
            response.status(400).send('Missing parameter \'dataSource\' in request body!');
        } else if (!type.string(data.dataSet)) {
            response.status(400).send('Missing parameter \'dataSet\' in request body!');
        } else if (!this.dataSources.has(data.dataSource)) {
            response.status(404).send(`The data source ${data.dataSource} was not found!`);
        } else {
            const clusterIdentifier = `${data.dataSource}/${data.dataSet}`;

            // resolve the cluster service
            const clusterHost = await this.registryClient.resolve('rda-cluster');

            // check the status on the cluster service
            log.info('Checking if the cluster to be created exists already');
            const clusterStatusURL = `${clusterHost}/rda-cluster.cluster/${clusterIdentifier}`;
            const clusterStatusResponse = await superagent.get(clusterStatusURL)
                .ok(res => [404, 200].includes(res.status))
                .send();

            // make sure neither we, nor the cluster service knows
            // a thing about the cluster to be created
            if (clusterStatusResponse.status === 404 &&
                !this.clusterStatus.has(clusterIdentifier)) {

                // get the service address of the data source
                const storageHost = await this.registryClient.resolve(data.dataSource);


                // request information about the data that will be processed by the cluster. this is
                // needed to size the cluster correctly
                log.info('Getting information about the data that has to be loaded into the cluster');
                const dataInfoURL = `${storageHost}/${data.dataSource}.dataset-info/${data.dataSet}`;
                const dataInfoResponse = await superagent.get(dataInfoURL)
                    .ok(res => res.status === 200)
                    .send();

                const info = dataInfoResponse.body;



                // create the cluster
                log.info('Setting up the cluster');
                const clusterResponse = await superagent.post(`${clusterHost}/rda-cluster.cluster`)
                    .ok(res => res.status === 201)
                    .send({
                        requiredMemory: info.totalMemory,
                        recordCount: info.recordCount,
                        dataSet: data.dataSet,
                        dataSource: data.dataSource,
                    });



                // instruct the data source to create the shards
                log.info('Instruct the datasource to create data shards');
                await superagent.post(`${storageHost}/${data.dataSource}.shard`)
                    .ok(res => res.status === 201)
                    .send({
                        shards: clusterResponse.body.shards,
                        dataSet: data.dataSet,
                    });


                // tell the cluster to initialize
                log.info('Initialize cluster');
                const { clusterId } = clusterResponse.body;
                const clusterInitilizeURL = `${clusterHost}/rda-cluster.cluster/${clusterId}`;
                await superagent.patch(clusterInitilizeURL)
                    .ok(res => res.status === 200)
                    .send();


                // return some info. the user has to get the status info using the lsitOne method
                return {
                    clusterId: clusterResponse.body.clusterId,
                    clusterIdentifier: clusterResponse.body.clusterIdentifier,
                    shards: clusterResponse.body.shards,
                    requiredMemory: info.totalMemory,
                    recordCount: info.recordCount,
                    dataSet: data.dataSet,
                    dataSource: data.dataSource,
                };
            } else response.status(409).send(`Canont create cluster: the cluster '${clusterIdentifier}' exists already!`);
        }
    }
}
