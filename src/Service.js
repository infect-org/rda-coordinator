import RDAService from '@infect/rda-service';
import path from 'path';

// controllers
import ClusterController from './controller/ClusterController.js';
import ClusterDataUpdateController from './controller/ClusterDataUpdateController.js';



const appRoot = path.join(path.dirname(new URL(import.meta.url).pathname), '../');




export default class RDACoordinatorService extends RDAService {


    constructor() {
        super({
            name: 'rda-coordinator',
            appRoot,
        });
    }



    /**
    * prepare the service
    */
    async load() {
        await this.initialize();

        // get a map of data sources
        this.dataSources = new Set(this.config.get('data-sources'));

        const options = {
            dataSources: this.dataSources,
            registryClient: this.registryClient,
        };


        this.registerController(new ClusterController(options));
        this.registerController(new ClusterDataUpdateController(options));

        // load the web server
        await super.load();

        // tell the service registry that we're up and running
        await this.registerService();
    }
}
