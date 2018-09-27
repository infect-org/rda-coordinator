import RDAService from 'rda-service';


// controllers
import ClusterController from './controller/ClusterController.mjs';
import ClusterDataUpdateController from './controller/ClusterDataUpdateController.mjs';





export default class RDACoordinatorService extends RDAService {


    constructor() {
        super('rda-coordinator');
    }




    /**
    * prepare the service
    */
    async load() {

        // get a map of data sources
        this.dataSources = new Set(this.config.dataSources);

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
