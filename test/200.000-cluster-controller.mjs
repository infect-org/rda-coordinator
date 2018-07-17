'use strict';

import Service from '../index.mjs';
import section from 'section-tests';
import superagent from 'superagent';
import assert from 'assert';
import log from 'ee-log';
import {ServiceManager} from 'rda-service';
import {DataSet} from 'rda-fixtures';



const host = 'http://l.dns.porn';



section('Cluster Controller', (section) => { 
    let sm;
    let dataSetId;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('rda-service-registry');
        await sm.startServices('infect-rda-sample-storage', 'rda-cluster');
        await sm.startServices('rda-compute', 'rda-compute', 'rda-compute', 'rda-compute');


        // add fixtures
        const dataSet = new DataSet();
        dataSetId = await dataSet.create();
    });



    section.test('create cluster', async() => {
        const service = new Service();
        await service.load();

        const clusterResponse = await superagent.post(`${host}:${service.getPort()}/rda-coordinator.cluster`).ok(res => true/*res.status === 201*/).send({
            dataSource: 'infect-rda-sample-storage',
            dataSet: dataSetId,
        });

        assert(clusterResponse.body);
        assert(clusterResponse.body.clusterId);
        assert.equal(clusterResponse.body.recordCount, 1000)

        await section.wait(200);
        await service.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});