import Service from '../index.js';
import section from 'section-tests';
import assert from 'assert';
import log from 'ee-log';
import ServiceManager from '@infect/rda-service-manager';
import { DataSet } from '@infect/rda-fixtures';
import HTTP2Client from '@distributed-systems/http2-client';


const host = 'http://l.dns.porn';



section('Cluster Controller', (section) => {
    let sm;
    let dataSetId;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev.testing --log-level=error+ --log-module=*'.split(' '),
        });

        await sm.startServices('rda-service-registry');
        await sm.startServices('infect-rda-sample-storage', 'rda-cluster', 'rda-lock');
        await sm.startServices('rda-compute', 'rda-compute', 'rda-compute', 'rda-compute');


        // add fixtures
        const dataSet = new DataSet();
        dataSetId = await dataSet.create();
    });



    section.test('create cluster', async() => {
        section.setTimeout(5000);
        
        const service = new Service();
        const client = new HTTP2Client();
        await service.load();

        const clusterResponse = await client.post(`${host}:${service.getPort()}/rda-coordinator.cluster`).expect(201).send({
            dataSource: 'infect-rda-sample-storage',
            dataSet: dataSetId,
        });

        const data = await clusterResponse.getData();

        assert(data);
        assert(data.clusterId);
        assert.equal(data.recordCount, 1000);

        await section.wait(1000);
        await service.end();
        await client.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});
